package walframe

// Regression tests for the round-2 reviews: each failed before its fix.

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/v10/db"
)

// slowFSM delays every command entry before handing it to the Node, so that
// entries sit queued in Raft's FSM channel while Raft's AppliedIndex has moved on.
type slowFSM struct {
	*Node
	delay time.Duration
}

func (s *slowFSM) Apply(l *raft.Log) any {
	if l.Type == raft.LogCommand {
		time.Sleep(s.delay)
	}
	return s.Node.Apply(l)
}

// newSlowCluster is newCluster where node i's FSM sleeps delays[i] per entry.
func newSlowCluster(t *testing.T, delays []time.Duration) *cluster {
	t.Helper()
	c := &cluster{t: t}
	for i, d := range delays {
		id := fmt.Sprintf("n%d", i)
		e, err := NewEngine(t.TempDir(), 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { e.Close() })
		n := NewNode(e, nil)
		addr, tr := raft.NewInmemTransport(raft.ServerAddress(id))
		var fsm raft.FSM = n
		if d > 0 {
			fsm = &slowFSM{Node: n, delay: d}
		}
		r, err := raft.NewRaft(raftConf(id), fsm, raft.NewInmemStore(), raft.NewInmemStore(), raft.NewInmemSnapshotStore(), tr)
		if err != nil {
			t.Fatal(err)
		}
		n.SetRaft(r)
		t.Cleanup(func() { r.Shutdown().Error() })
		tn := &tnode{id: raft.ServerID(id), n: n, r: r, tr: tr, addr: addr}
		for _, o := range c.nodes {
			o.tr.Connect(addr, tr)
			tr.Connect(o.addr, o.tr)
		}
		c.nodes = append(c.nodes, tn)
	}
	var cfg raft.Configuration
	for _, tn := range c.nodes {
		cfg.Servers = append(cfg.Servers, raft.Server{ID: tn.id, Address: tn.addr})
	}
	if err := c.nodes[0].r.BootstrapCluster(cfg).Error(); err != nil {
		t.Fatal(err)
	}
	c.leader()
	return c
}

func engSeq(e *Engine) uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.seq
}

// converge must wait on what the FSM applied, not on Raft's AppliedIndex, which
// hashicorp/raft sets when it QUEUES a batch to the FSM goroutine.
func TestConvergeWaitsForTheFSM(t *testing.T) {
	c := newSlowCluster(t, []time.Duration{0, 300 * time.Millisecond, 0})
	l, f := c.nodes[0], c.nodes[1]
	if _, err := l.n.Execute(bg, req("CREATE TABLE t(x)")); err != nil {
		t.Fatal(err)
	}
	c.converge()
	ls, fs := engSeq(l.n.eng), engSeq(f.n.eng)
	li, _ := l.n.eng.PageImage()
	fi, _ := f.n.eng.PageImage()
	if fs != ls || !bytes.Equal(li, fi) {
		t.Errorf("converge returned while the follower FSM had not applied: seq %d vs %d, images equal=%v", ls, fs, bytes.Equal(li, fi))
	}
}

// A new Leader whose FSM still holds queued entries must not run a request on
// that stale state: it runs a barrier first, once per term.
func TestNewLeaderBarriersBeforeServing(t *testing.T) {
	for _, waitNoop := range []bool{false, true} {
		t.Run(fmt.Sprint("waitNoop=", waitNoop), func(t *testing.T) {
			c := newSlowCluster(t, []time.Duration{0, 400 * time.Millisecond, 0})
			a, b := c.nodes[0], c.nodes[1]
			for _, q := range []string{"CREATE TABLE t(x)", "INSERT INTO t VALUES(1)"} {
				if _, err := a.n.Execute(bg, req(q)); err != nil {
					t.Fatal(err)
				}
			}
			for b.r.AppliedIndex() < a.r.LastIndex() {
				time.Sleep(time.Millisecond)
			}
			if err := a.r.LeadershipTransferToServer(b.id, b.addr).Error(); err != nil {
				t.Fatal(err)
			}
			for b.r.State() != raft.Leader {
				time.Sleep(time.Millisecond)
			}
			if waitNoop {
				for b.r.AppliedIndex() < b.r.LastIndex() {
					time.Sleep(time.Millisecond)
				}
			}
			res, err := b.n.Execute(bg, req("UPDATE t SET x=2 WHERE x=1"))
			if err != nil {
				t.Fatalf("a new Leader had its write rejected: %v", err)
			}
			if len(res) != 1 || res[0].GetE() == nil || res[0].GetE().Error != "" || res[0].GetE().RowsAffected != 1 {
				t.Fatalf("a result computed on a state behind the committed log: %v", res)
			}
		})
	}
}

// The barrier runs once per term, not on every request.
func TestBarrierRunsOncePerTerm(t *testing.T) {
	c := newCluster(t, 3, 1<<30)
	a := c.leader()
	calls := 0
	real := func() error { calls++; return a.r.Barrier(5 * time.Second).Error() }
	a.n.barrier = real
	for i := 0; i < 3; i++ {
		c.mustExec(req(fmt.Sprintf("CREATE TABLE t%d(x)", i)))
	}
	if calls != 1 {
		t.Fatalf("barrier calls in one term: %d, want 1", calls)
	}
	var b *tnode
	for _, tn := range c.nodes {
		if tn != a {
			b = tn
		}
	}
	if err := a.r.LeadershipTransferToServer(b.id, b.addr).Error(); err != nil {
		t.Fatal(err)
	}
	for b.r.State() != raft.Leader {
		time.Sleep(5 * time.Millisecond)
	}
	if err := b.r.LeadershipTransferToServer(a.id, a.addr).Error(); err != nil {
		t.Fatal(err)
	}
	for a.r.State() != raft.Leader {
		time.Sleep(5 * time.Millisecond)
	}
	c.mustExec(req("CREATE TABLE after(x)"))
	if calls != 2 {
		t.Fatalf("barrier calls after losing and regaining leadership: %d, want 2", calls)
	}
}

// A snapshot whose database part is empty but whose WAL holds data is read by
// SQLite as an empty database (it removes a WAL beside an empty file).
func TestEmptyDatabaseSnapshotIsRefused(t *testing.T) {
	a := testEngine(t, 0)
	pl, id := runOK(t, a, "CREATE TABLE t(x)", "INSERT INTO t VALUES(1),(2),(3)")
	if err := a.Commit(id, pl); err != nil {
		t.Fatal(err)
	}
	sb, err := a.snapshotBytes()
	if err != nil {
		t.Fatal(err)
	}
	s, err := parseSnapshot(sb)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("genuine snapshot: db %d bytes, wal %d bytes", len(s.d), len(s.w))
	forged := mkSnap(s.seq, s.pageSize, s.dbPages, 0, nil, s.w)
	b := testEngine(t, 0)
	if err := b.restoreBytes(forged); err == nil {
		t.Error("a snapshot with an empty database part was installed")
	}
	// and the genuine one still restores
	c := testEngine(t, 0)
	if err := c.restoreBytes(sb); err != nil {
		t.Errorf("genuine snapshot refused: %v", err)
	}
}

// Real commits repeat page numbers; the growth rule must count DISTINCT pages.
func TestDuplicatePageNumbersDoNotInflateGrowth(t *testing.T) {
	a := testEngine(t, 0)
	pl, id := runOK(t, a, "CREATE TABLE t(x)")
	if err := a.Commit(id, pl); err != nil {
		t.Fatal(err)
	}
	if a.dbPages != 2 {
		t.Skipf("fixture has %d pages, want 2", a.dbPages)
	}
	page := bytes.Repeat([]byte{2}, 4096)
	gid, gpl, err := unmarshalCommand(marshalCommand(id, &PageList{
		BaseSeq: a.seq, BaseHash: a.head, PageSize: 4096,
		Txs: []Tx{{Pgnos: []uint32{2, 2, 2}, Pages: [][]byte{page, page, page}, DBSize: 4}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Commit(gid, gpl); !errors.Is(err, ErrRejected) {
		t.Errorf("DBSize 4 backed by one distinct new page: %v", err)
	}
	// Duplicates are fine when every page is backed.
	gid2, gpl2, err := unmarshalCommand(marshalCommand(id, &PageList{
		BaseSeq: a.seq, BaseHash: a.head, PageSize: 4096,
		Txs: []Tx{{Pgnos: []uint32{2, 3, 4, 3}, Pages: [][]byte{page, page, page, page}, DBSize: 4}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := a.checkLocked(gpl2); err != nil {
		t.Errorf("legitimate duplicates refused: %v (%x)", err, gid2[:2])
	}
}

// An engine that stops must not stay Raft Leader: the cluster could not write.
func TestStoppedLeaderStepsDown(t *testing.T) {
	c := newCluster(t, 3, 1) // checkpoint after every entry
	c.mustExec(req("CREATE TABLE t(x)"))
	l := c.leader()
	l.n.eng.mu.Lock()
	l.n.eng.ckpt = func(*db.Conn) (*db.CheckpointMeta, error) { return nil, errInjected }
	l.n.eng.mu.Unlock()
	if _, err := l.n.Execute(bg, req("INSERT INTO t VALUES(1)")); err == nil {
		t.Log("the write whose checkpoint failed was acknowledged")
	}
	deadline := time.Now().Add(10 * time.Second)
	for l.r.State() == raft.Leader && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if l.r.State() == raft.Leader {
		t.Fatal("a stopped engine is still Raft Leader")
	}
	// the cluster elects a healthy leader and writes again
	var nl *tnode
	for i := 0; i < 400 && nl == nil; i++ {
		for _, tn := range c.nodes {
			if tn != l && tn.r.State() == raft.Leader {
				nl = tn
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if nl == nil {
		t.Fatal("no healthy leader elected")
	}
	if _, err := nl.n.Execute(bg, req("INSERT INTO t VALUES(2)")); err != nil {
		t.Errorf("the cluster cannot write after the stop: %v", err)
	}
}

// CREATE TEMP TABLE and writes to the temp schema change no replicated page, yet
// they are acknowledged: they exist on the Leader only.
func TestTempWritesAreRefused(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.mustExec(req("CREATE TABLE t(x)"))
	l := c.leader()
	for _, q := range []string{
		"CREATE TEMP TABLE x(a)",
		"CREATE TEMPORARY TABLE x(a)",
		"CREATE TEMP VIEW v AS SELECT 1",
		"CREATE TEMP TRIGGER g AFTER INSERT ON t BEGIN SELECT 1; END",
		"CREATE TEMP TABLE y AS SELECT 1",
	} {
		res, err := l.n.Execute(bg, req(q))
		refused := err != nil || (len(res) == 1 && res[0].GetE() != nil && strings.Contains(res[0].GetE().Error, "not authorized"))
		if !refused {
			t.Errorf("%q acknowledged: %v %v", q, res, err)
		}
	}
	// ordinary writes still work
	c.mustExec(req("INSERT INTO t VALUES(1)"))
}

// stepDown does nothing for a node that is not the Leader (or has no Raft), and
// says so in the log when a Leader cannot give up leadership.
func TestStepDownOnlyActsOnALeader(t *testing.T) {
	defer nopanic(t)
	(&Node{}).stepDown() // no Raft: must not panic

	c := newCluster(t, 3, 1<<30)
	l := c.leader()
	for _, tn := range c.nodes {
		if tn == l {
			continue
		}
		var buf bytes.Buffer
		tn.n.log = log.New(&buf, "", 0)
		tn.n.stepDown()
		if buf.Len() != 0 {
			t.Errorf("a follower tried to give up leadership: %s", buf.String())
		}
	}
	if c.leader() != l {
		t.Error("the Leader changed")
	}

	// A single node has nobody to hand over to: the failure is logged.
	one := newCluster(t, 1, 1<<30)
	var buf bytes.Buffer
	n := one.leader().n
	n.log = log.New(&buf, "", 0)
	n.stepDown()
	if !strings.Contains(buf.String(), "cannot give up leadership") {
		t.Errorf("a failed transfer was not logged: %q", buf.String())
	}
}

// The lock-byte page: 0x40000000 / page size, plus one.
func TestLockBytePage(t *testing.T) {
	for ps, want := range map[uint32]uint32{512: 2097153, 4096: 262145, 65536: 16385} {
		if got := lockBytePage(ps); got != want {
			t.Errorf("lockBytePage(%d) = %d, want %d", ps, got, want)
		}
	}
}
