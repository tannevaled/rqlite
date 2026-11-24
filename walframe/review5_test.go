package walframe

// Round 5 (the answer to the round-4 security review): the write guard is an
// allowlist, reads have levels, a stopped node does not churn, a malformed entry
// is refused, one barrier per term, and Run honours the query timeout.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	command "github.com/rqlite/rqlite/v10/command/proto"
)

func r5Committed(t *testing.T) *Engine {
	t.Helper()
	e := testEngine(t, 1<<30)
	for _, q := range []string{"CREATE TABLE t(x)", "INSERT INTO t VALUES(1)"} {
		pl, id := runOK(t, e, q)
		if err := e.Commit(id, pl); err != nil {
			t.Fatal(err)
		}
	}
	return e
}

// r5Run runs one request in the engine's write mode, commits what it produced,
// and reports whether it went through without any refusal or error.
func r5Run(e *Engine, rq *command.Request) (string, bool) {
	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()
	res, pl, id, err := e.Run(ctx, rq, false)
	ok := err == nil
	var parts []string
	for _, r := range res {
		s := ""
		if x := r.GetE(); x != nil {
			s = x.Error
		}
		if x := r.GetQ(); x != nil {
			s = x.Error
		}
		if s != "" {
			ok = false
			parts = append(parts, s)
		}
	}
	if err != nil {
		parts = append(parts, err.Error())
	}
	if pl != nil {
		if cerr := e.Commit(id, pl); cerr != nil {
			ok = false
			parts = append(parts, "commit: "+cerr.Error())
		}
	}
	return strings.Join(parts, " | "), ok
}

func r5Dir(t *testing.T, dir string) string {
	t.Helper()
	es, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, x := range es {
		names = append(names, x.Name())
	}
	return strings.Join(names, ",")
}

func r5Param(sql, v string) *command.Request {
	return &command.Request{Statements: []*command.Statement{{Sql: sql,
		Parameters: []*command.Parameter{{Value: &command.Parameter_S{S: v}}}}}}
}

// F-R4-1: what a client with execute rights is refused, in every spelling, and
// what it is still given.
func TestWriteGuardMatrix(t *testing.T) {
	e := r5Committed(t)
	out := t.TempDir()
	f := func(n string) string { return filepath.Join(out, n) }
	refused := []*command.Request{
		req("ATTACH NULL AS vacuum_n"),
		req("ATTACH '' AS plain"),
		req(`ATTACH "" AS vacuum_dq`),
		req("ATTACH '' AS vacuum_x KEY 'k'"),
		req("ATTACH ':memory:' AS vacuum_m"),
		req("ATTACH x'' AS vacuum_blob"),
		req("ATTACH (SELECT '" + f("sub.db") + "') AS vacuum_sub"),
		req("ATTACH '" + f("lit.db") + "' AS vacuum_lit"),
		req("ATTACH 'file:" + f("uri.db") + "?vfs=unix' AS vacuum_uri"),
		req("ATTACH printf('%s','" + f("printf.db") + "') AS vacuum_pf"),
		r5Param("ATTACH ?1 AS vacuum_p", f("param.db")),
		r5Param("ATTACH ?1 AS vacuum_pe", ""),
		req("DETACH vacuum_dq"),
		req("DETACH main"),
		req("VACUUM INTO '" + f("vi.db") + "'"),
		r5Param("VACUUM INTO ?1", f("vi3.db")),
		req("VACUUM INTO ''"),
		req("PRAGMA journal_mode=DELETE"),
		req("PRAGMA writable_schema=ON"),
		req("PRAGMA locking_mode=NORMAL"),
		req("PRAGMA temp_store_directory='" + out + "'"),
		req("PRAGMA wal_checkpoint(TRUNCATE)"),
		req("PRAGMA page_size=8192"),
		req("PRAGMA schema_version=99"),
		req("PRAGMA temp.user_version=7"),
		req("PRAGMA plain.user_version=7"),
		req("PRAGMA vacuum_dq.user_version=7"),
		req("PRAGMA query_only=1"),
		req("UPDATE sqlite_dbpage SET data=zeroblob(4096) WHERE pgno=2"),
		req("INSERT INTO sqlite_dbpage(pgno,data) VALUES(2,zeroblob(4096))"),
		req("DELETE FROM sqlite_dbpage WHERE pgno=2"),
		req("CREATE VIRTUAL TABLE vr USING rtree(id,a,b)"),
		req("CREATE VIRTUAL TABLE vp USING sqlite_dbpage"),
		req("CREATE VIRTUAL TABLE temp.ft USING fts5(x)"),
		req("CREATE VIRTUAL TABLE vacuum_dq.ft USING fts5(x)"),
		req("CREATE VIRTUAL TABLE plain.ft USING fts5(x)"),
		req("CREATE TEMP TABLE tmp1(a)"),
		req("CREATE TABLE temp.tmp2(a)"),
		req("CREATE TEMP VIEW tv AS SELECT 1"),
		req("ANALYZE temp"),
		req("INSERT INTO temp.sqlite_master VALUES(1,2,3,4,5)"),
		req("CREATE TABLE plain.p(a)"),
	}
	for _, c := range refused {
		if o, ok := r5Run(e, c); ok {
			t.Errorf("%q went through (%s)", c.Statements[0].Sql, o)
		}
	}
	if got := r5Dir(t, out); got != "" {
		t.Errorf("a client created files outside the engine directory: %s", got)
	}
	// What stays: ordinary DML and DDL on main, user_version, FTS5, ANALYZE.
	for _, q := range []string{
		"INSERT INTO t VALUES(2)", "UPDATE t SET x=3", "CREATE INDEX ti ON t(x)", "ANALYZE",
		"PRAGMA user_version=7", "PRAGMA optimize", "CREATE VIRTUAL TABLE ft USING fts5(x)",
		"INSERT INTO ft VALUES('hello')", "DROP INDEX ti", "ALTER TABLE t ADD COLUMN y",
		"CREATE VIEW v AS SELECT x FROM t", "DROP VIEW v", "DELETE FROM t WHERE x=3", "VACUUM",
	} {
		if o, ok := r5Run(e, req(q)); !ok {
			t.Errorf("%q refused: %s", q, o)
		}
	}
	if e.Fatal() != nil {
		t.Errorf("fatal: %v", e.Fatal())
	}
}

// A plain VACUUM sent through Run is the engine's own: it works, once the guard
// runs in vacuum mode, and nothing of the private database leaks.
func TestPlainVacuumThroughRun(t *testing.T) {
	e := r5Committed(t)
	for _, q := range []string{"VACUUM", "VACUUM;", " vacuum ;"} {
		if o, ok := r5Run(e, req(q)); !ok {
			t.Errorf("%q: %s", q, o)
		}
	}
	// It is only a plain VACUUM: a batch with a second statement is not.
	if _, ok := r5Run(e, req("VACUUM", "ATTACH '' AS p")); ok {
		t.Error("VACUUM followed by ATTACH went through")
	}
	if _, ok := r5Run(e, req("ATTACH '' AS p", "VACUUM")); ok {
		t.Error("ATTACH followed by VACUUM went through")
	}
	if !isPlainVacuum(req("VACUUM")) || isPlainVacuum(req("VACUUM INTO 'x'")) || isPlainVacuum(req("VACUUM", "VACUUM")) ||
		isPlainVacuum(r5Param("VACUUM", "x")) {
		t.Error("isPlainVacuum")
	}
}

// F-R4-1: the private database no longer exists for a client, so it holds
// nothing, past any bound, on the Leader only; and ATTACH slots cannot be used up.
func TestPrivateDatabaseIsRefused(t *testing.T) {
	e := r5Committed(t)
	e.SetMaxEntryBytes(1 << 20)
	if o, ok := r5Run(e, req("ATTACH '' AS vacuum_x", "CREATE TABLE vacuum_x.s(a)")); ok {
		t.Fatalf("ATTACH '' accepted: %s", o)
	}
	if o, ok := r5Run(e, req("INSERT INTO vacuum_x.s(a) VALUES(zeroblob(8000000))")); ok {
		t.Fatalf("a private insert accepted: %s", o)
	}
	for i := 0; i < 130; i++ {
		if _, ok := r5Run(e, req(fmt.Sprintf("ATTACH '' AS vacuum_slot%d", i))); ok {
			t.Fatalf("attach %d accepted", i)
		}
	}
	if o, ok := r5Run(e, req("VACUUM")); !ok {
		t.Errorf("VACUUM after 130 attach attempts: %s", o)
	}
}

// F-R4-1 on a cluster: no node holds a database the others lack.
func TestPrivateDatabaseClusterReads(t *testing.T) {
	c := newCluster(t, 3, 1<<30)
	c.mustExec(req("CREATE TABLE t(x)"))
	l := c.leader()
	res, err := l.n.Execute(bg, req("ATTACH '' AS vacuum_x", "CREATE TABLE vacuum_x.s(a)", "INSERT INTO vacuum_x.s VALUES('leader-only')"))
	if err == nil {
		for _, r := range res {
			if r.GetE() != nil && r.GetE().Error != "" {
				err = errors.New(r.GetE().Error)
			}
		}
	}
	if err == nil {
		t.Fatalf("the ATTACH went through: %v", res)
	}
	c.converge()
	var answers []string
	for _, tn := range c.nodes {
		rows, qerr := tn.n.QueryLevel(bg, req("SELECT a FROM vacuum_x.s"), None)
		answers = append(answers, fmt.Sprintf("%v %v", rows, qerr))
		if qerr == nil && len(rows) == 1 && rows[0].Error == "" {
			t.Errorf("%s answered a read of a database nobody replicates: %v", tn.id, rows)
		}
	}
	if answers[0] != answers[1] || answers[1] != answers[2] {
		t.Errorf("nodes at the same applied index answer the same read differently: %v", answers)
	}
}

// F-R4-6: Run, like Query, stops at the query timeout.
func TestRunHonoursTheQueryTimeout(t *testing.T) {
	e := r5Committed(t)
	e.SetQueryTimeout(200 * time.Millisecond)
	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()
	start := time.Now()
	e.Run(ctx, req("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c) SELECT count(*) FROM c"), false)
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("a request held the engine for %v with a 200 ms query timeout", d)
	}
}

// F-R4-2: the consistency levels.
func TestConsistencyLevels(t *testing.T) {
	c := newCluster(t, 3, 1<<30)
	c.mustExec(req("CREATE TABLE t(x)"))
	c.mustExec(req("INSERT INTO t VALUES(1)"))
	c.converge()
	l := c.leader()
	for _, lv := range []Consistency{Linearizable, Weak, None} {
		rows, err := l.n.QueryLevel(bg, req("SELECT count(*) FROM t"), lv)
		if err != nil || len(rows) != 1 || rows[0].Error != "" {
			t.Errorf("leader, level %d: %v %v", lv, rows, err)
		}
	}
	if Consistency(0) != Linearizable {
		t.Error("the zero value of Consistency must be the strongest level")
	}
	for _, tn := range c.nodes {
		if tn == l {
			continue
		}
		if _, err := tn.n.QueryLevel(bg, req("SELECT 1"), None); err != nil {
			t.Errorf("follower, none: %v", err)
		}
		for _, lv := range []Consistency{Weak, Linearizable} {
			if _, err := tn.n.QueryLevel(bg, req("SELECT 1"), lv); !errors.Is(err, ErrNotLeader) {
				t.Errorf("follower, level %d: %v", lv, err)
			}
		}
		if _, err := tn.n.Query(bg, req("SELECT 1")); !errors.Is(err, ErrNotLeader) {
			t.Errorf("follower, default level: %v", err)
		}
	}
	if _, err := l.n.QueryLevel(bg, req("SELECT 1"), Consistency(99)); err == nil {
		t.Error("an unknown level was accepted")
	}
	if _, err := (&Node{eng: l.n.eng}).QueryLevel(bg, req("SELECT 1"), Weak); !errors.Is(err, ErrNotLeader) {
		t.Errorf("no raft, weak: %v", err)
	}
	if _, err := (&Node{eng: l.n.eng}).QueryLevel(bg, req("SELECT 1"), Linearizable); !errors.Is(err, ErrNotLeader) {
		t.Errorf("no raft, linearizable: %v", err)
	}
}

// F-R4-2: a Leader that lost its quorum (but not yet its state) does not serve a
// default read, nor a read-only Execute.
func TestDeposedLeaderDefaultReadRefused(t *testing.T) {
	c := newCluster(t, 3, 1<<30)
	c.mustExec(req("CREATE TABLE t(x)"))
	c.mustExec(req("INSERT INTO t VALUES(1)"))
	c.converge()
	l := c.leader()
	var others []*tnode
	for _, o := range c.nodes {
		if o != l {
			others = append(others, o)
			o.tr.Disconnect(l.addr)
			l.tr.Disconnect(o.addr)
		}
	}
	acked := false
	for end := time.Now().Add(15 * time.Second); time.Now().Before(end) && !acked; time.Sleep(5 * time.Millisecond) {
		for _, o := range others {
			if o.r.State() == raft.Leader {
				if _, err := o.n.Execute(bg, req("INSERT INTO t VALUES(2)")); err == nil {
					acked = true
				}
			}
		}
	}
	if !acked {
		t.Fatal("the majority never committed")
	}
	stale := func(rows []*command.QueryRows, err error) bool {
		return err == nil && len(rows) == 1 && len(rows[0].Values) == 1 && rows[0].Values[0].Parameters[0].GetI() == 1
	}
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(bg, 2*time.Second)
		rows, err := l.n.Query(ctx, req("SELECT count(*) FROM t"))
		cancel()
		if stale(rows, err) {
			t.Fatalf("the default level served count=1 after count=2 was acknowledged")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// F-R4-2: a read-only request through Execute proves leadership as Linearizable does.
func TestExecuteOfAReadVerifiesLeadership(t *testing.T) {
	c := newCluster(t, 1, 1<<30)
	l := c.leader().n
	c.mustExec(req("CREATE TABLE t(x)"))
	called := 0
	l.verify = func() error { called++; return errInjected }
	if res, err := l.Execute(bg, req("SELECT count(*) FROM t")); !errors.Is(err, ErrNotLeader) || res != nil || called != 1 {
		t.Fatalf("a read whose leadership could not be proved: %v %v (verify called %d times)", res, err, called)
	}
	l.verify = func() error { called++; return nil }
	if res, err := l.Execute(bg, req("SELECT count(*) FROM t")); err != nil || len(res) != 1 {
		t.Fatalf("a proved read: %v %v", res, err)
	}
}

// F-R4-3: when every node is stopped, the leadership does not move for ever.
func TestAllNodesStoppedDoNotChurn(t *testing.T) {
	c := newCluster(t, 3, 1<<30)
	c.mustExec(req("CREATE TABLE t(x)"))
	c.converge()
	ch := make(chan raft.Observation, 4096)
	obs := raft.NewObserver(ch, false, func(o *raft.Observation) bool {
		st, ok := o.Data.(raft.RaftState)
		return ok && st == raft.Leader
	})
	for _, tn := range c.nodes {
		tn.r.RegisterObserver(obs)
	}
	for _, tn := range c.nodes {
		tn.n.eng.Fail(errInjected)
	}
	time.Sleep(6 * time.Second)
	for _, tn := range c.nodes {
		tn.r.DeregisterObserver(obs)
	}
	// Each node gives up after stepDownBudget transfers: the churn has an end.
	if max := len(c.nodes)*stepDownBudget + 1; len(ch) > max {
		t.Errorf("%d elections in 6 s with every node stopped; the bound is %d", len(ch), max)
	}
	before := len(ch)
	time.Sleep(3 * time.Second)
	if len(ch) != before {
		t.Errorf("elections kept going after the budget: %d -> %d", before, len(ch))
	}
}

// The guards of the new read and step-down paths, each held by an assertion.
func TestLevelAndStepDownGuards(t *testing.T) {
	c := newCluster(t, 3, 1<<30)
	c.mustExec(req("CREATE TABLE t(x)"))
	c.converge()
	l := c.leader()
	var f *tnode
	for _, tn := range c.nodes {
		if tn != l {
			f = tn
		}
	}

	// No Raft: nothing is proved, nothing is read, and the hooks are not asked.
	calls := 0
	bare := &Node{eng: l.n.eng, behind: func() bool { return false }, verify: func() error { calls++; return nil }}
	for _, lv := range []Consistency{Weak, Linearizable} {
		if rows, err := bare.QueryLevel(bg, req("SELECT 1"), lv); !errors.Is(err, ErrNotLeader) || rows != nil {
			t.Errorf("no raft, level %d: %v %v", lv, rows, err)
		}
	}
	if err := bare.verifyLeader(bg); !errors.Is(err, ErrNotLeader) || calls != 0 {
		t.Errorf("verifyLeader without raft: %v (hook called %d times)", err, calls)
	}
	tcalls := 0
	bare.transfer = func() error { tcalls++; return nil }
	bare.stepDown()
	if tcalls != 0 {
		t.Errorf("a node with no raft asked for a transfer")
	}

	// A Leader whose quorum proof fails refuses a Linearizable read, and a Weak
	// one does not ask for the proof.
	asked := 0
	l.n.verify = func() error { asked++; return errInjected }
	if rows, err := l.n.QueryLevel(bg, req("SELECT 1"), Linearizable); !errors.Is(err, ErrNotLeader) || rows != nil {
		t.Errorf("linearizable with a failed proof: %v %v", rows, err)
	}
	if _, err := l.n.QueryLevel(bg, req("SELECT 1"), Weak); err != nil || asked != 1 {
		t.Errorf("weak: %v (proof asked %d times)", err, asked)
	}
	l.n.verify = nil

	// A follower never asks for a transfer, and its back-off wait ends at once.
	f.n.transfer = func() error { tcalls++; return nil }
	f.n.stepDown()
	if tcalls != 0 {
		t.Errorf("a follower asked for a transfer")
	}
	start := time.Now()
	if pauseWhileLeader(f.r, 10*time.Second) || time.Since(start) > 2*time.Second {
		t.Errorf("a follower waited out a leader's pause")
	}
	if !pauseWhileLeader(l.r, 30*time.Millisecond) {
		t.Error("a Leader's pause was cut short")
	}
}

func TestBackoffBefore(t *testing.T) {
	n := &Node{backoff: 10 * time.Millisecond, backoffMax: 35 * time.Millisecond}
	for _, c := range []struct {
		done int32
		want time.Duration
	}{{0, 0}, {-1, 0}, {1, 10 * time.Millisecond}, {2, 20 * time.Millisecond}, {3, 35 * time.Millisecond}, {9, 35 * time.Millisecond}} {
		if got := n.backoffBefore(c.done); got != c.want {
			t.Errorf("backoffBefore(%d) = %v, want %v", c.done, got, c.want)
		}
	}
	d := &Node{}
	if d.backoffBefore(1) != stepDownBackoff || d.backoffBefore(20) != stepDownBackoffMax {
		t.Errorf("defaults: %v %v", d.backoffBefore(1), d.backoffBefore(20))
	}
}

// F-R4-5: requests that give up on the barrier share one.
func TestImpatientRequestsShareOneBarrier(t *testing.T) {
	_, _, b := r4SlowHandover(t)
	li, g := b.r.LastIndex(), runtime.NumGoroutine()
	const n = 40
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(bg, time.Millisecond)
		b.n.Query(ctx, req("SELECT 1"))
		cancel()
	}
	if d := b.r.LastIndex() - li; d > 2 {
		t.Errorf("%d reads appended %d Barrier entries to the Raft log", n, d)
	}
	if now := runtime.NumGoroutine(); now > g+5 {
		t.Errorf("goroutines %d -> %d", g, now)
	}
}

// F-R4-4: a malformed entry is refused, as doc.go says, identically on every
// node, and stops nobody.
func TestMalformedEntryIsRefusedNotFatal(t *testing.T) {
	for _, tc := range []struct {
		name string
		data func(valid []byte) []byte
	}{
		{"garbage", func([]byte) []byte { return []byte("garbage") }},
		{"empty", func([]byte) []byte { return nil }},
		{"bit flip", func(v []byte) []byte { w := append([]byte(nil), v...); w[len(w)/2] ^= 1; return w }},
		{"truncated", func(v []byte) []byte { return v[:len(v)-7] }},
		{"wrong command byte", func(v []byte) []byte { w := append([]byte(nil), v...); w[0] ^= 0xff; return w }},
	} {
		src := testEngine(t, 1<<30)
		pl, id := runOK(t, src, "CREATE TABLE t(x)")
		valid := marshalCommand(id, pl)
		e := testEngine(t, 1<<30)
		n := NewNode(e, nil)
		resp := n.Apply(&raft.Log{Type: raft.LogCommand, Index: 1, Data: tc.data(valid)})
		if !errors.Is(asErr(resp), ErrRejected) {
			t.Errorf("%s: response %v, want ErrRejected", tc.name, resp)
		}
		if e.Fatal() != nil || n.Fatal() != nil {
			t.Errorf("%s: one malformed entry stopped the node: %v %v", tc.name, e.Fatal(), n.Fatal())
		}
		if r2 := n.Apply(&raft.Log{Type: raft.LogCommand, Index: 2, Data: valid}); r2 != nil {
			t.Errorf("%s: a valid entry afterwards: %v", tc.name, r2)
		}
	}
}
