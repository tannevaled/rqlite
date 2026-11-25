package walframe

// Regression tests for the round-3 reviews: each failed before its fix.

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	command "github.com/rqlite/rqlite/v10/command/proto"
)

// F-R3-1: an ATTACH whose file name is not a string literal reaches the
// authorizer with a NULL name, which must not pass for VACUUM's literal ”.
func TestAttachWithAComputedNameIsRefused(t *testing.T) {
	e := testEngine(t, 0)
	pl, id := runOK(t, e, "CREATE TABLE t(x)")
	if err := e.Commit(id, pl); err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	f1, f2, f3, f4 := filepath.Join(out, "e1.db"), filepath.Join(out, "e2.db"), filepath.Join(out, "e3.db"), filepath.Join(out, "e4.db")
	cases := []struct {
		name string
		rq   *command.Request
		file string
	}{
		{"concatenation", req("ATTACH ('"+f1+"' || '') AS vacuum_y", "CREATE TABLE vacuum_y.p(a)"), f1},
		{"URI with vfs", req("ATTACH ('file:"+f2+"?vfs=unix' || '') AS vacuum_u", "CREATE TABLE vacuum_u.p(a)"), f2},
		{"bound parameter", &command.Request{Statements: []*command.Statement{
			{Sql: "ATTACH ? AS vacuum_p", Parameters: []*command.Parameter{{Value: &command.Parameter_S{S: "file:" + f3 + "?vfs=unix"}}}},
			{Sql: "CREATE TABLE vacuum_p.p(a)"}}}, f3},
		{"cast", req("ATTACH CAST('"+f4+"' AS TEXT) AS vacuum_c", "CREATE TABLE vacuum_c.p(a)"), f4},
		{"in the engine directory", req("ATTACH ('side.db' || '') AS vacuum_s", "CREATE TABLE vacuum_s.p(a)"), filepath.Join(e.dir, "side.db")},
	}
	for _, c := range cases {
		res, pl, _, err := e.Run(bg, c.rq, false)
		first := ""
		if len(res) > 0 && res[0].GetE() != nil {
			first = res[0].GetE().Error
		}
		if err == nil && !strings.Contains(first, "not authorized") {
			t.Errorf("%s: ATTACH accepted: %v", c.name, res)
		}
		if pl != nil {
			t.Errorf("%s: produced pages", c.name)
		}
		if _, serr := os.Stat(c.file); serr == nil {
			t.Errorf("%s: a client created %s", c.name, c.file)
		}
	}
	// VACUUM, which ATTACHes a literal '' itself, still works.
	if _, _, _, err := e.Run(bg, req("VACUUM"), false); err != nil {
		t.Errorf("VACUUM: %v", err)
	}
}

func r4SlowHandover(t *testing.T) (*cluster, *tnode, *tnode) {
	t.Helper()
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
	return c, a, b
}

// R3-2: a read on a new Leader, before any write ran the barrier, must see the
// committed rows.
func TestQueryOnANewLeaderWaitsForTheBarrier(t *testing.T) {
	_, _, b := r4SlowHandover(t)
	rows, err := b.n.Query(bg, req("SELECT count(*) FROM t"))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 1 || rows[0].Error != "" || len(rows[0].Values) != 1 || rows[0].Values[0].Parameters[0].GetI() != 1 {
		t.Errorf("the Leader served a read from a state behind the committed log: %v", rows)
	}
}

// R3-3: the barrier is bounded by the request's context.
func TestBarrierHonoursTheContext(t *testing.T) {
	_, _, b := r4SlowHandover(t)
	ctx, cancel := context.WithTimeout(bg, 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := b.n.Execute(ctx, req("UPDATE t SET x=2 WHERE x=1"))
	if d := time.Since(start); d > 300*time.Millisecond {
		t.Errorf("Execute outlived its 50 ms context: %v (err=%v)", d, err)
	}
	if err == nil {
		t.Error("Execute succeeded after its context expired")
	}
	// A cancelled read is bounded too.
	_, _, b = r4SlowHandover(t)
	ctx2, cancel2 := context.WithTimeout(bg, 50*time.Millisecond)
	defer cancel2()
	start = time.Now()
	b.n.Query(ctx2, req("SELECT count(*) FROM t"))
	if d := time.Since(start); d > 300*time.Millisecond {
		t.Errorf("Query outlived its 50 ms context: %v", d)
	}
}

func r4WaitState(tn *tnode, want raft.RaftState, d time.Duration) bool {
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
		if tn.r.State() == want {
			return true
		}
	}
	return tn.r.State() == want
}

func r4OtherLeader(c *cluster, not *tnode) *tnode {
	for end := time.Now().Add(8 * time.Second); time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
		for _, tn := range c.nodes {
			if tn != not && tn.r.State() == raft.Leader {
				return tn
			}
		}
	}
	return nil
}

// R3-1 and F-R3-3: a node whose engine stopped steps down not once but every time
// it is elected again.
func TestStoppedNodeElectedAgainStepsDownAgain(t *testing.T) {
	c := newCluster(t, 3, 1<<30)
	c.mustExec(req("CREATE TABLE t(x)"))
	l := c.leader()
	l.n.eng.Fail(errInjected)
	nl := r4OtherLeader(c, l)
	if nl == nil {
		t.Fatal("no healthy leader after the first stop")
	}
	if _, err := nl.n.Execute(bg, req("INSERT INTO t VALUES(1)")); err != nil {
		t.Fatal(err)
	}
	if err := nl.r.LeadershipTransferToServer(l.id, l.addr).Error(); err != nil {
		t.Fatal(err)
	}
	// Elected, then it must give the leadership up again.
	if !r4WaitState(l, raft.Leader, 5*time.Second) {
		t.Skip("the stopped node was not elected")
	}
	if !r4WaitState(l, raft.Follower, 8*time.Second) {
		t.Fatal("the stopped node is Leader again and stays so")
	}
	if h := r4OtherLeader(c, l); h == nil {
		t.Fatal("no healthy Leader")
	} else if _, err := h.n.Execute(bg, req("INSERT INTO t VALUES(2)")); err != nil {
		t.Errorf("the cluster cannot write: %v", err)
	}
}

// A follower whose engine stopped while it was a follower, then elected.
func TestStoppedFollowerElectedStepsDown(t *testing.T) {
	c := newCluster(t, 3, 1<<30)
	c.mustExec(req("CREATE TABLE t(x)"))
	l := c.leader()
	var f *tnode
	for _, tn := range c.nodes {
		if tn != l {
			f = tn
		}
	}
	c.converge()
	f.n.eng.Fail(errInjected)
	time.Sleep(200 * time.Millisecond) // the stop hook ran: it was no Leader
	if err := l.r.LeadershipTransferToServer(f.id, f.addr).Error(); err != nil {
		t.Fatal(err)
	}
	if !r4WaitState(f, raft.Leader, 5*time.Second) {
		t.Skip("the stopped follower was not elected")
	}
	if !r4WaitState(f, raft.Follower, 8*time.Second) {
		t.Errorf("the stopped follower is Raft %v after its election", f.r.State())
	}
}

// F-R3-2: the snapshot's header fields are installed as they stand, so they must
// agree with the files.
func TestSnapshotHeaderFieldsAreValidated(t *testing.T) {
	e := testEngine(t, 0)
	pl, id := runOK(t, e, "CREATE TABLE t(x)", "INSERT INTO t VALUES(1)")
	if err := e.Commit(id, pl); err != nil {
		t.Fatal(err)
	}
	b, err := e.snapshotBytes()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseSnapshot(b); err != nil {
		t.Fatalf("the control snapshot is refused: %v", err)
	}
	reseal := func(mut func(body []byte)) []byte {
		body := append([]byte(nil), b[:len(b)-HashSize]...)
		mut(body)
		sum := sha256.Sum256(body)
		return append(body, sum[:]...)
	}
	pagesAt := 4 + 8 + HashSize + 4
	cases := map[string]func([]byte){
		"more pages than the files hold":  func(x []byte) { binary.BigEndian.PutUint32(x[pagesAt:], binary.BigEndian.Uint32(x[pagesAt:])+1000) },
		"fewer pages than the files hold": func(x []byte) { binary.BigEndian.PutUint32(x[pagesAt:], 1) },
		"no head hash":                    func(x []byte) { clear(x[12 : 12+HashSize]) },
	}
	for name, mut := range cases {
		if _, err := parseSnapshot(reseal(mut)); err == nil {
			t.Errorf("%s: the snapshot is accepted", name)
		}
		if err := testEngine(t, 0).restoreBytes(reseal(mut)); err == nil {
			t.Errorf("%s: the snapshot is installed", name)
		}
	}
}

// F-R3-4: Node.fatal keeps the first error, not every one joined.
func TestFatalKeepsOnlyTheFirstError(t *testing.T) {
	n := NewNode(testEngine(t, 0), nil)
	// A well-formed entry reaching an engine that has stopped is an apply this node
	// cannot vouch for: it is the case that sets fatal.
	n.eng.Fail(errInjected)
	cmd := marshalCommand([idSize]byte{1}, &PageList{PageSize: 4096,
		Txs: []Tx{{Pgnos: []uint32{1}, Pages: [][]byte{filled(4096, 1)}, DBSize: 1}}})
	n.Apply(&raft.Log{Type: raft.LogCommand, Index: 1, Data: cmd})
	first := n.Fatal()
	if first == nil {
		t.Fatal("a failed apply did not stop the node")
	}
	for i := 2; i < 50; i++ {
		n.Apply(&raft.Log{Type: raft.LogCommand, Index: uint64(i), Data: cmd})
	}
	if n.Fatal() != first {
		t.Errorf("fatal changed or grew: %v", n.Fatal())
	}
	if got := n.Fatal().Error(); got != first.Error() || strings.Count(got, "injected failure") != 1 {
		t.Errorf("fatal grew: %v", got)
	}
}

// T1: each half of the temp fix has its own end-to-end case. The first three
// are held by the refusal of CREATE TEMP; the last by the refusal to write to a
// database that is not main (ATTACH ” itself is let through for VACUUM).
func TestEachHalfOfTheTempFixHasItsOwnCase(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.mustExec(req("CREATE TABLE t(x)"))
	l := c.leader()
	refused := func(sqls ...string) bool {
		res, err := l.n.Execute(bg, req(sqls...))
		if err != nil {
			return true
		}
		for _, r := range res {
			if r.GetE() != nil && strings.Contains(r.GetE().Error, "not authorized") {
				return true
			}
		}
		return false
	}
	// half 1: CREATE TEMP
	for _, q := range []string{"CREATE TEMP TABLE x(a)", "CREATE TEMP VIEW v AS SELECT 1"} {
		if !refused(q) {
			t.Errorf("half 1: %q acknowledged", q)
		}
	}
	// half 2: a write to another schema, with no CREATE TEMP involved
	if !refused("ATTACH '' AS plain", "CREATE TABLE plain.s(a)") {
		t.Error("half 2: CREATE TABLE in an attached private database acknowledged")
	}
	c.mustExec(req("INSERT INTO t VALUES(1)"))
}

// stepDown calls the transfer once when it succeeds, retries a failure up to
// stepDownTries times, and calls it never on a node that is not the Leader.
func TestStepDownTransfersAndRetries(t *testing.T) {
	c := newCluster(t, 3, 1<<30)
	l := c.leader()
	calls := 0
	l.n.transfer = func() error { calls++; return nil } // the node stays Leader
	l.n.stepDown()
	if calls != 1 {
		t.Errorf("a successful transfer was called %d times", calls)
	}
	calls = 0
	l.n.transfer = func() error {
		calls++
		if calls < 3 {
			return errInjected
		}
		return nil
	}
	l.n.stepDown()
	if calls != 3 {
		t.Errorf("a transfer failing twice was called %d times, want 3", calls)
	}
	calls = 0
	l.n.transfer = func() error { calls++; return errInjected }
	l.n.stepDown()
	if calls != stepDownTries {
		t.Errorf("a failing transfer was called %d times, want %d", calls, stepDownTries)
	}
	for _, tn := range c.nodes {
		if tn == l {
			continue
		}
		calls = 0
		tn.n.transfer = func() error { calls++; return nil }
		tn.n.stepDown()
		if calls != 0 {
			t.Errorf("a follower called the transfer %d times", calls)
		}
	}
}

// The goroutine that watches Raft's state returns when Raft shuts down.
func TestLeadershipWatcherEndsWithRaft(t *testing.T) {
	c := newCluster(t, 1, 1<<30)
	n := c.leader().n
	if n.watchDone == nil {
		t.Fatal("no watcher")
	}
	select {
	case <-n.watchDone:
		t.Fatal("the watcher returned while Raft runs")
	default:
	}
	if err := c.leader().r.Shutdown().Error(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-n.watchDone:
	case <-time.After(3 * time.Second):
		t.Error("the watcher outlived Raft")
	}
}

// A Leader whose barrier failed does not serve the read.
func TestQueryRefusesWhenTheBarrierFails(t *testing.T) {
	c := newCluster(t, 3, 1<<30)
	c.mustExec(req("CREATE TABLE t(x)"))
	l := c.leader()
	l.n.barrierTerm = 0
	l.n.barrier = func() error { return errInjected }
	rows, err := l.n.Query(bg, req("SELECT count(*) FROM t"))
	if !errors.Is(err, errInjected) || rows != nil {
		t.Errorf("Query after a failed barrier: rows=%v err=%v", rows, err)
	}
}
