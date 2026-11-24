package walframe

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	command "github.com/rqlite/rqlite/v10/command/proto"
	"google.golang.org/protobuf/encoding/protojson"
)

type tnode struct {
	id   raft.ServerID
	n    *Node
	r    *raft.Raft
	tr   *raft.InmemTransport
	addr raft.ServerAddress
}

type cluster struct {
	t     testing.TB
	nodes []*tnode
	ckpt  int64
}

func raftConf(id string) *raft.Config {
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(id)
	c.HeartbeatTimeout = 100 * time.Millisecond
	c.ElectionTimeout = 100 * time.Millisecond
	c.LeaderLeaseTimeout = 50 * time.Millisecond
	c.CommitTimeout = 5 * time.Millisecond
	c.SnapshotInterval = 20 * time.Millisecond
	c.SnapshotThreshold = 1 << 30 // snapshots are taken explicitly
	c.TrailingLogs = 1
	c.LogOutput = io.Discard
	return c
}

func (c *cluster) addNode(id string) *tnode {
	c.t.Helper()
	e, err := NewEngine(c.t.TempDir(), c.ckpt, nil)
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(func() { e.Close() })
	n := NewNode(e, nil)
	addr, tr := raft.NewInmemTransport(raft.ServerAddress(id))
	r, err := raft.NewRaft(raftConf(id), n, raft.NewInmemStore(), raft.NewInmemStore(), raft.NewInmemSnapshotStore(), tr)
	if err != nil {
		c.t.Fatal(err)
	}
	n.SetRaft(r)
	c.t.Cleanup(func() { r.Shutdown().Error() })
	tn := &tnode{id: raft.ServerID(id), n: n, r: r, tr: tr, addr: addr}
	for _, o := range c.nodes {
		o.tr.Connect(addr, tr)
		tr.Connect(o.addr, o.tr)
	}
	c.nodes = append(c.nodes, tn)
	return tn
}

// newCluster starts n voters, the first bootstrapping with all of them.
func newCluster(t testing.TB, n int, ckptBytes int64) *cluster {
	t.Helper()
	c := &cluster{t: t, ckpt: ckptBytes}
	for i := 0; i < n; i++ {
		c.addNode(fmt.Sprintf("n%d", i))
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

func (c *cluster) leaderAmong(nodes []*tnode) *tnode {
	for i := 0; i < 400; i++ {
		for _, tn := range nodes {
			if tn.r.State() == raft.Leader {
				return tn
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	c.t.Fatal("no leader")
	return nil
}

func (c *cluster) leader() *tnode { return c.leaderAmong(c.nodes) }

func req(stmts ...string) *command.Request {
	r := &command.Request{}
	for _, s := range stmts {
		r.Statements = append(r.Statements, &command.Statement{Sql: s})
	}
	return r
}

func intParam(sql string, v int64) *command.Request {
	return &command.Request{Statements: []*command.Statement{{Sql: sql,
		Parameters: []*command.Parameter{{Value: &command.Parameter_I{I: v}}}}}}
}

// mustExec runs on the Leader and fails the test on a request error or a
// statement error.
func (c *cluster) mustExec(r *command.Request) {
	c.t.Helper()
	res, err := c.leader().n.Execute(context.Background(), r)
	if err != nil {
		c.t.Fatalf("execute: %v", err)
	}
	for _, x := range res {
		if e := x.GetE(); e != nil && e.Error != "" {
			c.t.Fatalf("statement error: %s", e.Error)
		}
	}
}

// converge waits until every node in nodes has applied everything the Leader
// has logged.
func (c *cluster) converge(nodes ...*tnode) {
	c.t.Helper()
	if len(nodes) == 0 {
		nodes = c.nodes
	}
	l := c.leaderAmong(nodes)
	// A barrier returns once the Leader's FSM has applied everything logged, so
	// the Leader's engine state is the target. Raft's AppliedIndex is no witness
	// for the followers: it moves when a batch is queued to the FSM, not when
	// the FSM has applied it. The engine's own count of applied entries is.
	if err := l.r.Barrier(5 * time.Second).Error(); err != nil {
		c.t.Fatal(err)
	}
	want := engSeq(l.n.eng)
	deadline := time.Now().Add(20 * time.Second)
	for _, tn := range nodes {
		// A stopped node applies nothing more; its callers read Fatal.
		for engSeq(tn.n.eng) < want && tn.n.Fatal() == nil {
			if time.Now().After(deadline) {
				c.t.Fatalf("node %s applied %d page lists, want %d", tn.id, engSeq(tn.n.eng), want)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (c *cluster) integrity(tn *tnode) string {
	c.t.Helper()
	rows, err := tn.n.Query(context.Background(), req("PRAGMA integrity_check"))
	if err != nil || len(rows) != 1 || rows[0].Error != "" {
		c.t.Fatalf("integrity_check on %s: %v %v", tn.id, rows, err)
	}
	return rows[0].Values[0].Parameters[0].GetS()
}

func (c *cluster) rows(tn *tnode, q string) string {
	c.t.Helper()
	rows, err := tn.n.Query(context.Background(), req(q))
	if err != nil || len(rows) != 1 {
		c.t.Fatalf("%s on %s: %v %v", q, tn.id, rows, err)
	}
	rows[0].Time = 0
	b, err := protojson.Marshal(rows[0])
	if err != nil {
		c.t.Fatal(err)
	}
	return string(b)
}

func firstDiff(a, b []byte) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return -1
}

// assertIdentical: after converging, every node holds byte-identical pages,
// passes integrity_check, and answers the same for q (if not empty).
func (c *cluster) assertIdentical(step, q string, nodes ...*tnode) {
	c.t.Helper()
	if len(nodes) == 0 {
		nodes = c.nodes
	}
	c.converge(nodes...)
	ref, err := nodes[0].n.eng.PageImage()
	if err != nil {
		c.t.Fatal(err)
	}
	for _, tn := range nodes {
		if f := tn.n.Fatal(); f != nil {
			c.t.Fatalf("%s: node %s diverged: %v", step, tn.id, f)
		}
		img, err := tn.n.eng.PageImage()
		if err != nil {
			c.t.Fatal(err)
		}
		if !bytes.Equal(ref, img) {
			c.t.Fatalf("%s: page images of %s and %s differ (%d vs %d bytes), first diff at %d (page %d, offset in page %d)", step, nodes[0].id, tn.id, len(ref), len(img), firstDiff(ref, img), firstDiff(ref, img)/4096+1, firstDiff(ref, img)%4096)
		}
		if ic := c.integrity(tn); ic != "ok" {
			c.t.Fatalf("%s: integrity_check on %s = %q", step, tn.id, ic)
		}
		if q != "" && c.rows(tn, q) != c.rows(nodes[0], q) {
			c.t.Fatalf("%s: query result differs on %s:\n%s\nvs\n%s", step, tn.id, c.rows(tn, q), c.rows(nodes[0], q))
		}
	}
	c.t.Logf("%s: %d nodes identical (%d pages, integrity ok)", step, len(nodes), len(ref)/4096)
}

const dumpQ = "SELECT id, hex(b) FROM t ORDER BY id"

// TestClusterNonDeterministicSQLIdentical: rqlite#3055 (randomblob(?)) and
// other non-deterministic SQL. The statement path replays the SQL on every
// node and can diverge; here only pages travel, so the nodes cannot.
func TestClusterNonDeterministicSQLIdentical(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.mustExec(req("CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB, ts TEXT, r REAL)"))
	c.assertIdentical("create", dumpQ)
	c.mustExec(intParam("INSERT INTO t(b) VALUES(randomblob(?))", 16)) // #3055
	c.assertIdentical("randomblob(?)", dumpQ)
	c.mustExec(intParam("INSERT INTO t(b) VALUES(randomblob(?))", 20000)) // overflow pages
	c.assertIdentical("randomblob(20000)", dumpQ)
	c.mustExec(req(
		"INSERT INTO t(b, ts, r) VALUES(randomblob(8), datetime('now'), random())",
		"INSERT INTO t(b, ts, r) SELECT randomblob(abs(random() % 3000)), strftime('%f','now'), random() FROM (SELECT 1 UNION SELECT 2 UNION SELECT 3)",
	))
	c.assertIdentical("random()/now", dumpQ)
	// A statement that fails halfway keeps the effects of the ones before it.
	res, err := c.leader().n.Execute(context.Background(), req(
		"INSERT INTO t(b) VALUES(randomblob(100))", "INSERT INTO t(id) VALUES(1)", "INSERT INTO t(b) VALUES(randomblob(100))"))
	if err != nil {
		t.Fatal(err)
	}
	if res[1].GetE() == nil || res[1].GetE().Error == "" {
		t.Fatalf("expected a constraint error, got %v", res)
	}
	c.assertIdentical("failing statement", dumpQ)
	c.mustExec(req("CREATE INDEX ib ON t(b)"))
	c.assertIdentical("create index", dumpQ)
	c.mustExec(req("DELETE FROM t WHERE id%2=0"))
	c.mustExec(req("VACUUM"))
	c.assertIdentical("vacuum", dumpQ)
	c.mustExec(req("ANALYZE"))
	c.mustExec(req("PRAGMA optimize"))
	c.assertIdentical("analyze+optimize", "SELECT tbl, idx, stat FROM sqlite_stat1 ORDER BY 1,2")
	c.mustExec(intParam("INSERT INTO t(b) VALUES(randomblob(?))", 5000))
	c.assertIdentical("after vacuum", dumpQ)
}

// TestFollowerCheckpointsAreNodeLocal: a tiny checkpoint threshold makes every
// node checkpoint on its own schedule while replication continues.
func TestFollowerCheckpointsAreNodeLocal(t *testing.T) {
	c := newCluster(t, 3, 64<<10)
	c.mustExec(req("CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB)"))
	for i := 0; i < 40; i++ {
		c.mustExec(intParam("INSERT INTO t(b) VALUES(randomblob(?))", 3000))
	}
	c.assertIdentical("after 40 inserts", dumpQ)
	total := 0
	for _, tn := range c.nodes {
		total += tn.n.eng.Stats().Checkpoints
	}
	if total == 0 {
		t.Fatal("expected node-local checkpoints to have run")
	}
	t.Logf("checkpoints per node: %v", func() (s []int) {
		for _, tn := range c.nodes {
			s = append(s, tn.n.eng.Stats().Checkpoints)
		}
		return
	}())
}

// TestLeaderLosesLeadershipBeforeCommit: the Leader executes locally, is cut
// off before its entry reaches Raft, and a new Leader is elected. The write
// must be gone from the old Leader (fenced undo), the cluster consistent, and
// the old Leader must converge once reconnected.
func TestLeaderLosesLeadershipBeforeCommit(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.mustExec(req("CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB)"))
	c.mustExec(intParam("INSERT INTO t(b) VALUES(randomblob(?))", 100))
	c.assertIdentical("baseline", dumpQ)

	old := c.leader()
	var others []*tnode
	for _, tn := range c.nodes {
		if tn != old {
			others = append(others, tn)
		}
	}
	before := c.rows(old, dumpQ)
	imgBefore, _ := old.n.eng.PageImage()
	var newLeader *tnode
	old.n.afterLocalExec = func() {
		// The local frames exist now. Cut the old Leader off and wait for the
		// others to elect a Leader.
		if rows := old.n.eng.pend; rows == nil {
			t.Error("no pending frames at the hook")
		}
		old.tr.DisconnectAll()
		for _, o := range others {
			o.tr.Disconnect(old.addr)
		}
		newLeader = c.leaderAmong(others)
	}
	_, err := old.n.Execute(context.Background(), intParam("INSERT INTO t(b) VALUES(randomblob(?))", 9000))
	old.n.afterLocalExec = nil
	if err == nil {
		t.Fatal("expected the write to fail: the entry could not be committed")
	}
	t.Logf("old leader's Execute: %v", err)
	if old.n.eng.Stats().Undos == 0 {
		t.Fatal("expected the local frames to have been undone")
	}
	// Gone from the old Leader, which is still cut off.
	if got := c.rows(old, dumpQ); got != before {
		t.Fatalf("write still visible on the old leader after undo\n got %s\nwant %s", got, before)
	}
	if ic := c.integrity(old); ic != "ok" {
		t.Fatalf("old leader integrity: %s", ic)
	}
	if img, _ := old.n.eng.PageImage(); !bytes.Equal(img, imgBefore) {
		t.Fatal("old leader's page image changed by the undone write")
	}

	// The new Leader commits something else; then the old one rejoins.
	res, err := newLeader.n.Execute(context.Background(), intParam("INSERT INTO t(b) VALUES(randomblob(?))", 700))
	if err != nil || res[0].GetE() != nil && res[0].GetE().Error != "" {
		t.Fatalf("new leader write: %v %v", res, err)
	}
	for _, o := range others {
		old.tr.Connect(o.addr, o.tr)
		o.tr.Connect(old.addr, old.tr)
	}
	c.assertIdentical("after reconnect", dumpQ)
	c.mustExec(intParam("INSERT INTO t(b) VALUES(randomblob(?))", 123))
	c.assertIdentical("after more writes", dumpQ)
	if got := c.rows(old, "SELECT count(*) FROM t"); !strings.Contains(got, `"3"`) {
		t.Fatalf("expected 3 rows (baseline, new leader, last), got %s", got)
	}
}

// TestSnapshotRestoreAndLateJoin: a partitioned node falls behind a compacted
// log and catches up through InstallSnapshot/Restore; a brand-new node joins
// late the same way.
func TestSnapshotRestoreAndLateJoin(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.mustExec(req("CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB)"))
	c.assertIdentical("create", dumpQ)

	l := c.leader()
	var lag *tnode
	for _, tn := range c.nodes {
		if tn != l {
			lag = tn
			break
		}
	}
	lag.tr.DisconnectAll()
	for _, o := range c.nodes {
		o.tr.Disconnect(lag.addr)
	}
	for i := 0; i < 25; i++ {
		c.mustExec(intParam("INSERT INTO t(b) VALUES(randomblob(?))", int64(500+i*400)))
	}
	if err := l.r.Snapshot().Error(); err != nil {
		t.Fatal(err)
	}
	for _, tn := range c.nodes {
		if tn != lag && tn != l { // the other follower snapshots too, compacting its log
			if err := tn.r.Snapshot().Error(); err != nil {
				t.Fatal(err)
			}
		}
	}
	c.mustExec(intParam("INSERT INTO t(b) VALUES(randomblob(?))", 77))

	restoresBefore := lag.n.eng.Stats().Applies
	for _, o := range c.nodes {
		if o != lag {
			lag.tr.Connect(o.addr, o.tr)
			o.tr.Connect(lag.addr, lag.tr)
		}
	}
	c.assertIdentical("partitioned node caught up", dumpQ)
	t.Logf("lagging node: %d page-list applies before, %d after", restoresBefore, lag.n.eng.Stats().Applies)

	// A new node joins late.
	nn := c.addNode("late")
	if err := c.leader().r.AddVoter(nn.id, nn.addr, 0, 5*time.Second).Error(); err != nil {
		t.Fatal(err)
	}
	c.mustExec(intParam("INSERT INTO t(b) VALUES(randomblob(?))", 4444))
	c.assertIdentical("late joiner", dumpQ)
	if nn.n.eng.Stats().Applies > 3 {
		t.Logf("late joiner applied %d page lists (log replay rather than snapshot)", nn.n.eng.Stats().Applies)
	}
}

// TestRefusedStatements: what this mode refuses.
func TestRefusedStatements(t *testing.T) {
	c := newCluster(t, 3, 0)
	for _, s := range []string{
		"ATTACH DATABASE 'x.db' AS x",
		"PRAGMA journal_mode=DELETE",
		"pragma main.wal_checkpoint(TRUNCATE)",
		"  PRAGMA locking_mode = NORMAL",
		"PRAGMA page_size=8192",
		"PRAGMA auto_vacuum=FULL",
		"PRAGMA synchronous=FULL",
		"PRAGMA/**/journal_mode=delete",
		"pragma  JOURNAL_MODE",
		"-- hi\nPRAGMA\n\tJournal_Mode=OFF",
		"SELECT 1; ATTACH DATABASE 'y.db' AS y",
		"/* c */ ATTACH 'z.db' AS z; SELECT 2",
		"DETACH DATABASE x",
		"PRAGMA [main].[wal_checkpoint]",
		"PRAGMA writable_schema=ON",
	} {
		res, err := c.leader().n.Execute(context.Background(), req(s))
		if err != nil {
			t.Errorf("%q: request error %v", s, err)
			continue
		}
		// A leading SELECT answers as rows, anything else as an execute result.
		if len(res) != 1 || !strings.Contains(res[0].String(), "not authorized") {
			t.Errorf("%q: expected a statement error \"not authorized\", got %v", s, res)
		}
		rows, qerr := c.leader().n.Query(context.Background(), req(s))
		if qerr != nil || len(rows) != 1 || !strings.Contains(rows[0].Error, "not authorized") {
			t.Errorf("%q: query path: expected \"not authorized\", got %v %v", s, rows, qerr)
		}
	}
	// Nothing was replicated, and the connection is still in WAL mode.
	if got := c.rows(c.leader(), "PRAGMA journal_mode"); !strings.Contains(got, "not authorized") {
		t.Errorf("journal_mode readable by a user: %s", got)
	}
	if st := c.leader().n.eng.Stats(); st.OwnCommits != 0 {
		t.Errorf("refused statements were replicated: %+v", st)
	}
	// A transaction left open across requests is rolled back and reported.
	_, err := c.leader().n.Execute(context.Background(), req("CREATE TABLE t(a)", "BEGIN", "INSERT INTO t VALUES(1)"))
	if err == nil || !strings.Contains(err.Error(), "span requests") {
		t.Fatalf("expected the open transaction to be refused, got %v", err)
	}
	c.assertIdentical("after refused tx", "")
}
