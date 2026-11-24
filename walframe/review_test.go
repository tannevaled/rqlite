package walframe

// Regression tests for the two review rounds (correctness r1, security r1) of
// the prototype. Each asserts the CORRECT behaviour; each failed before the fix.

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	command "github.com/rqlite/rqlite/v10/command/proto"
)

// truncFail is osOps whose next Truncate fails once armed.
type truncFail struct {
	osOps
	armed bool
}

func (f *truncFail) Truncate(p string, n int64) error {
	if f.armed {
		f.armed = false
		return errInjected
	}
	return osOps{}.Truncate(p, n)
}

// pair returns a leading and a following engine holding CREATE TABLE t(x).
func pair(t *testing.T, ckpt int64) (a, b *Engine) {
	t.Helper()
	a, b = testEngine(t, ckpt), testEngine(t, ckpt)
	pl, id := runOK(t, a, "CREATE TABLE t(x)")
	both(t, a, b, id, pl)
	return a, b
}

// both commits one entry on both engines, through the codec as Raft carries it.
func both(t *testing.T, a, b *Engine, id [idSize]byte, pl *PageList) {
	t.Helper()
	gid, gpl, err := unmarshalCommand(marshalCommand(id, pl))
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Commit(gid, gpl); err != nil {
		t.Fatalf("leader commit: %v", err)
	}
	if err := b.Commit(gid, gpl); err != nil {
		t.Fatalf("follower commit: %v", err)
	}
}

// refused reports whether a read-path request was refused, as a request error
// or as a statement error.
func refused(rows []*command.QueryRows, err error) bool {
	if err != nil {
		return true
	}
	for _, r := range rows {
		if r.Error != "" {
			return true
		}
	}
	return false
}

func countOf(t *testing.T, e *Engine, tbl string) string {
	t.Helper()
	rows, err := e.Query(bg, req("SELECT count(*) FROM "+tbl), false)
	if err != nil || len(rows) != 1 || rows[0].Error != "" {
		t.Fatalf("count: %v %v", rows, err)
	}
	return rows[0].Values[0].Parameters[0].String()
}

func integrityOK(t *testing.T, e *Engine) {
	t.Helper()
	rows, err := e.Query(bg, req("PRAGMA integrity_check"), false)
	if err != nil || len(rows) != 1 || rows[0].Error != "" || rows[0].Values[0].Parameters[0].GetS() != "ok" {
		t.Fatalf("integrity_check: %v %v", rows, err)
	}
}

// F2: a failed undo truncate must not re-derive the committed end from the WAL
// (which would promote Raft-refused frames): the engine stops.
func TestUndoTruncateFailureStopsTheEngine(t *testing.T) {
	defer nopanic(t)
	ops := &truncFail{}
	a, err := newEngine(t.TempDir(), 0, nil, ops)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	pl, id := runOK(t, a, "CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB)")
	if err := a.Commit(id, pl); err != nil {
		t.Fatal(err)
	}
	_, id2 := runOK(t, a, "INSERT INTO t(b) VALUES('refused')")
	ops.armed = true
	if err := a.Undo(id2); err == nil {
		t.Fatal("expected the injected truncate failure")
	}
	if a.Fatal() == nil {
		t.Fatal("the engine did not stop after a failed undo")
	}
	if _, _, _, err := a.Run(bg, req("INSERT INTO t(b) VALUES('acked')"), false); !errors.Is(err, ErrStopped) {
		t.Errorf("Run on a stopped engine: %v", err)
	}
	if _, err := a.Query(bg, req("SELECT count(*) FROM t"), false); !errors.Is(err, ErrStopped) {
		t.Errorf("Query on a stopped engine: %v", err)
	}
	if err := a.Checkpoint(); !errors.Is(err, ErrStopped) {
		t.Errorf("Checkpoint on a stopped engine: %v", err)
	}
}

// F1 (engine): an entry built on a state that is no longer current is refused
// by every node alike, and nothing diverges.
func TestStaleBasePageListIsRejected(t *testing.T) {
	defer nopanic(t)
	a, b := testEngine(t, 0), testEngine(t, 0)
	pl, id := runOK(t, a, "CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB)", "CREATE TABLE u(id INTEGER PRIMARY KEY, b BLOB)")
	if err := a.Commit(id, pl); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(id, pl); err != nil {
		t.Fatal(err)
	}
	const big = "INSERT INTO %s(b) SELECT randomblob(500) FROM (WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n<40) SELECT n FROM c)"
	plA, idA := runOK(t, a, strings.Replace(big, "%s", "t", 1))
	plX, idX := runOK(t, b, strings.Replace(big, "%s", "u", 1))
	if err := b.Commit(idX, plX); err != nil {
		t.Fatal(err)
	}
	if err := a.Commit(idX, plX); err != nil { // foreign: undoes a's pending
		t.Fatal(err)
	}
	for name, e := range map[string]*Engine{"a": a, "b": b} {
		if err := e.Commit(idA, plA); !errors.Is(err, ErrStaleBase) || !errors.Is(err, ErrRejected) {
			t.Errorf("%s: stale entry: %v", name, err)
		}
		if e.Fatal() != nil {
			t.Errorf("%s: a rejected entry stopped the engine", name)
		}
		integrityOK(t, e)
		if got := countOf(t, e, "u"); got != "i:40" {
			t.Errorf("%s: count(u) = %s", name, got)
		}
		if got := countOf(t, e, "t"); got != "i:0" {
			t.Errorf("%s: count(t) = %s", name, got)
		}
	}
	if !bytes.Equal(image(t, a), image(t, b)) {
		t.Error("the engines diverged")
	}
	// The chain goes on from the rejection.
	pl2, id2 := runOK(t, a, "INSERT INTO t(b) VALUES('x')")
	both2 := func(e *Engine) {
		if err := e.Commit(id2, pl2); err != nil {
			t.Fatal(err)
		}
	}
	both2(a)
	both2(b)
}

// F1 (cluster): the window between the local run and raft.Apply spans a lost
// and regained leadership. The client gets an error and no node is corrupt.
func TestStaleBaseCluster(t *testing.T) {
	defer nopanic(t)
	c := newCluster(t, 3, 0)
	c.mustExec(req("CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB)", "CREATE TABLE u(id INTEGER PRIMARY KEY, b BLOB)"))
	c.converge()
	a := c.leader()
	var b *tnode
	for _, tn := range c.nodes {
		if tn != a {
			b = tn
			break
		}
	}
	const big = "INSERT INTO %s(b) SELECT randomblob(500) FROM (WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n<40) SELECT n FROM c)"
	a.n.afterLocalExec = func() {
		if err := a.r.LeadershipTransferToServer(b.id, b.addr).Error(); err != nil {
			t.Error(err)
			return
		}
		for b.r.State() != raft.Leader {
			time.Sleep(5 * time.Millisecond)
		}
		if _, err := b.n.Execute(context.Background(), req(strings.Replace(big, "%s", "u", 1))); err != nil {
			t.Error(err)
			return
		}
		c.converge()
		if err := b.r.LeadershipTransferToServer(a.id, a.addr).Error(); err != nil {
			t.Error(err)
			return
		}
		for a.r.State() != raft.Leader {
			time.Sleep(5 * time.Millisecond)
		}
		for a.r.AppliedIndex() < a.r.LastIndex() {
			time.Sleep(5 * time.Millisecond)
		}
	}
	res, err := a.n.Execute(context.Background(), req(strings.Replace(big, "%s", "t", 1)))
	a.n.afterLocalExec = nil
	if res != nil {
		t.Error("the client was shown the result of a write that was applied nowhere")
	}
	if err == nil {
		t.Error("the stale write was acknowledged to its client")
	}
	if !errors.Is(err, ErrRejected) {
		t.Errorf("Execute: %v", err)
	}
	c.converge()
	for _, tn := range c.nodes {
		if ic := c.integrity(tn); ic != "ok" {
			t.Errorf("%s corrupt: %s", tn.id, ic)
		}
		if tn.n.Fatal() != nil {
			t.Errorf("%s stopped: %v", tn.id, tn.n.Fatal())
		}
	}
	c.assertIdentical("after the stale entry", "SELECT count(*) FROM u")
	c.assertIdentical("after the stale entry", "SELECT count(*) FROM t")
}

// F3: the read path is strictly read-only on the Leader and on followers.
func TestReadPathIsReadOnly(t *testing.T) {
	defer nopanic(t)
	c := newCluster(t, 3, 0)
	c.mustExec(req("CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB)", "INSERT INTO t(b) VALUES(x'01')"))
	c.converge()
	for _, tn := range c.nodes {
		sizeBefore, _ := osOps{}.Size(tn.n.eng.walPath)
		for _, s := range []string{
			"INSERT INTO t(b) VALUES(x'02')", "DELETE FROM t", "UPDATE t SET b=x'03'",
			"DELETE FROM t RETURNING id", "CREATE TABLE z(a)", "DROP TABLE t",
			"BEGIN", "SAVEPOINT s", "PRAGMA query_only=1", "PRAGMA user_version=5",
			"UPDATE sqlite_dbpage SET data=zeroblob(4096) WHERE pgno=1", "ATTACH '' AS x",
		} {
			rows, err := tn.n.QueryLevel(bg, req(s), None)
			if !refused(rows, err) {
				t.Errorf("%s: %q was not refused", tn.id, s)
			}
		}
		if got := c.rows(tn, "SELECT count(*) FROM t"); !strings.Contains(got, "1") {
			t.Errorf("%s: rows changed: %s", tn.id, got)
		}
		if sizeAfter, _ := (osOps{}).Size(tn.n.eng.walPath); sizeAfter != sizeBefore {
			t.Errorf("%s: a read changed the WAL: %d -> %d", tn.id, sizeBefore, sizeAfter)
		}
	}
	// What reads must still do.
	for _, s := range []string{"SELECT count(*) FROM t", "PRAGMA table_info(t)", "PRAGMA integrity_check", "WITH x(a) AS (SELECT 1) SELECT a FROM x"} {
		if rows, err := c.leader().n.Query(bg, req(s)); refused(rows, err) {
			t.Errorf("read %q refused: %v %v", s, rows, err)
		}
	}
	// A checkpoint of a follower changes nothing observable.
	c.mustExec(intParam("INSERT INTO t(b) VALUES(randomblob(?))", 10))
	c.converge()
	for _, tn := range c.nodes {
		if err := tn.n.eng.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	c.mustExec(intParam("INSERT INTO t(b) VALUES(randomblob(?))", 10))
	c.converge()
	c.assertIdentical("after checkpoints", "SELECT count(*) FROM t")
	for _, tn := range c.nodes {
		if ic := c.integrity(tn); ic != "ok" {
			t.Errorf("%s: %s", tn.id, ic)
		}
	}
}

// F3: BEGIN and PRAGMA query_only through the read path leave no trace on the
// connection the write path uses.
func TestReadPathLeavesTheWriteConnectionAlone(t *testing.T) {
	defer nopanic(t)
	a, _ := pair(t, 0)
	for _, s := range []string{"BEGIN", "PRAGMA query_only=1", "SAVEPOINT s"} {
		a.Query(bg, req(s), false)
	}
	for i := 0; i < 2; i++ {
		_, pl, id, err := a.Run(bg, req("INSERT INTO t VALUES(1)"), false)
		if err != nil || pl == nil {
			t.Fatalf("write #%d after reads: pl=%v err=%v", i, pl != nil, err)
		}
		if err := a.Commit(id, pl); err != nil {
			t.Fatal(err)
		}
		if len(pl.Txs) != 1 {
			t.Errorf("%d transactions in the page list, want 1", len(pl.Txs))
		}
	}
}

// Guard: a write to sqlite_dbpage is refused before it can reach any node.
func TestDbpageWriteIsRefused(t *testing.T) {
	defer nopanic(t)
	a, b := pair(t, 0)
	before := image(t, a)
	for _, s := range []string{
		"UPDATE sqlite_dbpage SET data=zeroblob(4096) WHERE pgno=1",
		"INSERT INTO sqlite_dbpage(pgno,data) VALUES(9,zeroblob(4096))",
		"DELETE FROM sqlite_dbpage",
	} {
		res, pl, _, err := a.Run(bg, req(s), false)
		if pl != nil {
			t.Errorf("%q produced a page list", s)
		}
		if err == nil && (len(res) != 1 || res[0].GetE() == nil || res[0].GetE().Error == "") {
			t.Errorf("%q was not refused: %v", s, res)
		}
	}
	if !bytes.Equal(image(t, a), before) || !bytes.Equal(image(t, a), image(t, b)) {
		t.Error("the database changed")
	}
	integrityOK(t, a)
	integrityOK(t, b)
	// Pragmas are an allowlist.
	for _, s := range []string{"PRAGMA query_only=1", "PRAGMA foreign_keys=OFF", "PRAGMA hard_heap_limit=1", "PRAGMA journal_mode=delete"} {
		res, pl, pid, err := a.Run(bg, req(s), false)
		if pl != nil || (err == nil && (len(res) != 1 || res[0].GetE() == nil || res[0].GetE().Error == "")) {
			t.Errorf("%q was not refused: %v %v", s, res, err)
		}
		a.Undo(pid)
	}
	if _, pl, id, err := a.Run(bg, req("INSERT INTO t VALUES(1)"), false); err != nil || pl == nil {
		t.Fatalf("write after the refusals: %v", err)
	} else if err := a.Commit(id, pl); err != nil {
		t.Fatal(err)
	}
}

// An entry with no transaction cannot be decoded, so it cannot pin a page size.
func TestEmptyPageListIsRefused(t *testing.T) {
	defer nopanic(t)
	evil := marshalCommand([idSize]byte{9}, &PageList{PageSize: 512})
	if _, _, err := unmarshalCommand(evil); !errors.Is(err, ErrBadPageList) {
		t.Fatalf("empty list: %v", err)
	}
	a, b := testEngine(t, 0), testEngine(t, 0)
	pl, id := runOK(t, a, "CREATE TABLE t(x)")
	both(t, a, b, id, pl)
}

// The page size is fixed at the first entry and checked on every one, also
// after a node-local checkpoint emptied the WAL.
func TestPageSizeIsAlwaysChecked(t *testing.T) {
	defer nopanic(t)
	a, b := pair(t, 0)
	pl, id := runOK(t, a, "INSERT INTO t VALUES(1)")
	both(t, a, b, id, pl)
	if err := b.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	evil := based(b, &PageList{PageSize: 512, Txs: []Tx{{Pgnos: []uint32{2}, Pages: [][]byte{bytes.Repeat([]byte{0xAA}, 512)}, DBSize: 2}}})
	gid, gpl, err := unmarshalCommand(marshalCommand([idSize]byte{7}, evil))
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(gid, gpl); !errors.Is(err, ErrRejected) {
		t.Fatalf("512-byte page list over a 4096-byte database: %v", err)
	}
	integrityOK(t, b)
	pl, id = runOK(t, a, "INSERT INTO t VALUES(2)")
	both(t, a, b, id, pl)
	if got := countOf(t, b, "t"); got != "i:2" {
		t.Errorf("count = %s", got)
	}
}

// DBSize is checked against what the entry can justify.
func TestAbsurdDBSizeIsRefused(t *testing.T) {
	defer nopanic(t)
	_, b := pair(t, 0)
	img := image(t, b)
	for name, evil := range map[string]*PageList{
		"huge":         {PageSize: 4096, Txs: []Tx{{Pgnos: []uint32{1}, Pages: [][]byte{img[:4096]}, DBSize: 100000}}},
		"beyond":       {PageSize: 4096, Txs: []Tx{{Pgnos: []uint32{9}, Pages: [][]byte{img[:4096]}, DBSize: 2}}},
		"zero":         {PageSize: 4096, Txs: []Tx{{Pgnos: []uint32{1}, Pages: [][]byte{img[:4096]}, DBSize: 0}}},
		"short page":   {PageSize: 4096, Txs: []Tx{{Pgnos: []uint32{1}, Pages: [][]byte{img[:100]}, DBSize: 1}}},
		"no page list": {PageSize: 4096},
	} {
		evil.BaseSeq, evil.BaseHash = b.seq, b.head
		if err := b.Commit([idSize]byte{7}, evil); !errors.Is(err, ErrRejected) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if err := b.Checkpoint(); err != nil {
		t.Errorf("checkpoint: %v", err)
	}
	integrityOK(t, b)
}

// A bit flipped in transit is refused, not given fresh WAL checksums.
func TestBitFlipIsRefused(t *testing.T) {
	defer nopanic(t)
	a, b := pair(t, 0)
	pl, id := runOK(t, a, "INSERT INTO t VALUES('hello')")
	raw := marshalCommand(id, pl)
	if err := a.Commit(id, pl); err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-5-HashSize] ^= 0x40 // inside the last page image
	if _, _, err := unmarshalCommand(raw); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("bit flip: %v", err)
	}
	_ = b
}

// The first entry must carry page 1, or the database file stays empty beside a
// WAL that SQLite deletes on open.
func TestFirstEntryNeedsPageOne(t *testing.T) {
	defer nopanic(t)
	b := testEngine(t, 0)
	evil := &PageList{PageSize: 4096, Txs: []Tx{{Pgnos: []uint32{2}, Pages: [][]byte{make([]byte, 4096)}, DBSize: 2}}}
	if err := b.Commit([idSize]byte{1}, evil); !errors.Is(err, ErrRejected) {
		t.Fatalf("first entry without page 1: %v", err)
	}
	if b.seq != 0 {
		t.Error("the chain moved")
	}
}

// A snapshot of garbage, or one that fails its checksum, is refused before
// anything is installed.
func TestRestoreRefusesGarbage(t *testing.T) {
	defer nopanic(t)
	a, b := pair(t, 0)
	pl, id := runOK(t, a, "INSERT INTO t VALUES(1)")
	both(t, a, b, id, pl)
	good, err := b.snapshotBytes()
	if err != nil {
		t.Fatal(err)
	}
	flipped := append([]byte(nil), good...)
	flipped[len(flipped)/2] ^= 1
	for name, snap := range map[string][]byte{
		"0xFF database": mkSnap(1, 4096, 1, 100, bytes.Repeat([]byte{0xFF}, 100), nil),
		"bad checksum":  flipped,
		"no checksum":   good[:len(good)-HashSize],
	} {
		if err := b.restoreBytes(snap); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	// Nothing was installed: the node still serves its own state.
	if got := countOf(t, b, "t"); got != "i:1" {
		t.Errorf("count = %s", got)
	}
	integrityOK(t, b)
	if err := b.restoreBytes(good); err != nil {
		t.Errorf("the good snapshot: %v", err)
	}
}

// Entry size is bounded: a small statement cannot become a huge entry. The
// refusal comes before any proposal, and the engine goes on.
func TestEntryBoundIsRefusedBeforeProposing(t *testing.T) {
	defer nopanic(t)
	a, _ := pair(t, 1<<40)
	a.SetMaxEntryBytes(1 << 20)
	sql := "WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i<8192) INSERT INTO t SELECT randomblob(1000) FROM c"
	_, pl, _, err := a.Run(bg, req(sql), false)
	if !errors.Is(err, ErrEntryTooLarge) || pl != nil {
		t.Fatalf("Run: pl=%v err=%v", pl != nil, err)
	}
	if a.pend != nil {
		t.Error("a pending request was recorded")
	}
	if n, _ := (osOps{}).Size(a.walPath); n > a.tl.end && a.tl.end != 0 {
		t.Errorf("WAL is %d bytes, committed end %d", n, a.tl.end)
	}
	if got := countOf(t, a, "t"); got != "i:0" {
		t.Errorf("count = %s", got)
	}
	pl, id := runOK(t, a, "INSERT INTO t VALUES(1)")
	if err := a.Commit(id, pl); err != nil {
		t.Fatal(err)
	}
}

// The frames of a rolled-back transaction that spilled are cut from the WAL.
func TestRolledBackSpillIsTruncated(t *testing.T) {
	defer nopanic(t)
	a, _ := pair(t, 0)
	sql := "WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i<16384) INSERT INTO t SELECT randomblob(1000) FROM c"
	_, pl, _, _ := a.Run(bg, req("BEGIN", sql), false)
	if pl != nil {
		t.Fatal("unexpected page list")
	}
	n, _ := osOps{}.Size(a.walPath)
	if n > 1<<20 {
		t.Errorf("WAL still %d bytes after a rolled-back spill (committed end %d)", n, a.tl.end)
	}
	pl, id := runOK(t, a, "INSERT INTO t VALUES(1)")
	if err := a.Commit(id, pl); err != nil {
		t.Fatal(err)
	}
	if got := countOf(t, a, "t"); got != "i:1" {
		t.Errorf("count = %s", got)
	}
}

// A long read cannot hold up the FSM for longer than the query timeout.
func TestReadCannotBlockApplyForLong(t *testing.T) {
	defer nopanic(t)
	a, b := pair(t, 0)
	b.SetQueryTimeout(300 * time.Millisecond)
	pl, id := runOK(t, a, "INSERT INTO t VALUES(1)")
	if err := a.Commit(id, pl); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		b.Query(bg, req("WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c) SELECT count(*) FROM c"), false)
		close(done)
	}()
	<-started
	time.Sleep(100 * time.Millisecond)
	t0 := time.Now()
	if err := b.Commit(id, pl); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(t0); d > time.Second {
		t.Errorf("Commit waited %v behind a read", d)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("the read was not interrupted")
	}
}

// ReadRange: a range, the rest, past the end, a missing file.
func TestReadRange(t *testing.T) {
	defer nopanic(t)
	p := t.TempDir() + "/f"
	if err := (osOps{}).WriteFile(p, []byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		off, n int64
		want   string
	}{{0, 3, "012"}, {4, -1, "456789"}, {4, 100, "456789"}, {10, -1, ""}, {20, 5, ""}} {
		b, err := osOps{}.ReadRange(p, c.off, c.n)
		if err != nil || string(b) != c.want || (c.want == "") != (b == nil) {
			t.Errorf("ReadRange(%d,%d) = %q, %v; want %q", c.off, c.n, b, err, c.want)
		}
	}
	if b, err := (osOps{}).ReadRange(p+"x", 0, -1); b != nil || err != nil {
		t.Errorf("missing file: %v %v", b, err)
	}
}
