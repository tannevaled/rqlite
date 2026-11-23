package walframe

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/db"
	"modernc.org/sqlite/vfs"
)

var errInjected = errors.New("injected failure")

// faultOps is osOps that fails its failAt-th call (1-based; 0 never) and counts
// the calls.
type faultOps struct {
	n, failAt int
}

func (f *faultOps) hit() error {
	f.n++
	if f.n == f.failAt {
		return errInjected
	}
	return nil
}

func (f *faultOps) ReadFile(p string) ([]byte, error) {
	if err := f.hit(); err != nil {
		return nil, err
	}
	return osOps{}.ReadFile(p)
}

func (f *faultOps) WriteFile(p string, b []byte) error {
	if err := f.hit(); err != nil {
		return err
	}
	return osOps{}.WriteFile(p, b)
}

func (f *faultOps) Remove(p string) error {
	if err := f.hit(); err != nil {
		return err
	}
	return osOps{}.Remove(p)
}

func (f *faultOps) Truncate(p string, n int64) error {
	if err := f.hit(); err != nil {
		return err
	}
	return osOps{}.Truncate(p, n)
}

func (f *faultOps) Size(p string) (int64, error) {
	if err := f.hit(); err != nil {
		return 0, err
	}
	return osOps{}.Size(p)
}

func (f *faultOps) WriteAtTruncate(p string, b []byte, off, size int64) error {
	if err := f.hit(); err != nil {
		return err
	}
	return osOps{}.WriteAtTruncate(p, b, off, size)
}

var bg = context.Background()

func testEngine(t *testing.T, ckpt int64) *Engine {
	t.Helper()
	e, err := NewEngine(t.TempDir(), ckpt, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

// runOK runs sqls on e, which must change the database.
func runOK(t *testing.T, e *Engine, sqls ...string) (*PageList, [idSize]byte) {
	t.Helper()
	res, pl, id, err := e.Run(bg, req(sqls...), false)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res {
		if x := r.GetE(); x != nil && x.Error != "" {
			t.Fatalf("statement error: %s", x.Error)
		}
	}
	if pl == nil {
		t.Fatalf("%v changed nothing", sqls)
	}
	return pl, id
}

func image(t *testing.T, e *Engine) []byte {
	t.Helper()
	b, err := e.PageImage()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// scenario drives two engines (a leading, b following) and a restore target
// through every operation that touches files. It returns the first error.
func scenario(t *testing.T, ops fileOps) error {
	mk := func() (*Engine, error) {
		e, err := newEngine(t.TempDir(), 1<<30, nil, ops)
		if err == nil {
			t.Cleanup(func() { e.Close() })
		}
		return e, err
	}
	exec := func(e *Engine, sql string) (*PageList, [idSize]byte, error) {
		_, pl, id, err := e.Run(bg, req(sql), false)
		return pl, id, err
	}
	a, err := mk()
	if err != nil {
		return err
	}
	b, err := mk()
	if err != nil {
		return err
	}

	pl, id, err := exec(a, "CREATE TABLE t(x)")
	if err != nil {
		return err
	}
	if err = a.Commit(id, pl); err != nil {
		return err
	}
	if err = b.Commit(id, pl); err != nil {
		return err
	}
	// A request Raft refuses is undone.
	if _, id, err = exec(a, "INSERT INTO t VALUES(2)"); err != nil {
		return err
	}
	if err = a.Undo(id); err != nil {
		return err
	}
	// b holds frames nobody committed when a's entry arrives: they are orphans.
	plA, idA, err := exec(a, "INSERT INTO t VALUES(3)")
	if err != nil {
		return err
	}
	if _, _, err = exec(b, "INSERT INTO t VALUES(4)"); err != nil {
		return err
	}
	if err = b.Commit(idA, plA); err != nil {
		return err
	}
	if err = a.Commit(idA, plA); err != nil {
		return err
	}
	ia, err := a.PageImage()
	if err != nil {
		return err
	}
	ib, err := b.PageImage()
	if err != nil {
		return err
	}
	if !bytes.Equal(ia, ib) {
		return errors.New("page images differ")
	}
	// Snapshot and restore, with and without a WAL.
	snap, err := a.snapshotBytes()
	if err != nil {
		return err
	}
	c, err := mk()
	if err != nil {
		return err
	}
	if err = c.restoreBytes(snap); err != nil {
		return err
	}
	d, err := mk()
	if err != nil {
		return err
	}
	empty, err := d.snapshotBytes()
	if err != nil {
		return err
	}
	if err = c.restoreBytes(empty); err != nil {
		return err
	}
	// A node-local checkpoint keeps the pages.
	if err = a.Checkpoint(); err != nil {
		return err
	}
	ia2, err := a.PageImage()
	if err != nil {
		return err
	}
	if !bytes.Equal(ia, ia2) {
		return errors.New("a checkpoint changed the page image")
	}
	return nil
}

func TestEngineEveryFileOperationFailureSurfaces(t *testing.T) {
	defer nopanic(t)
	base := &faultOps{}
	if err := scenario(t, base); err != nil {
		t.Fatalf("scenario without faults: %v", err)
	}
	if base.n < 20 {
		t.Fatalf("the scenario made %d file operations: it no longer covers the engine", base.n)
	}
	for i := 1; i <= base.n; i++ {
		f := &faultOps{failAt: i}
		if err := scenario(t, f); err == nil {
			t.Errorf("a failure of file operation %d of %d was swallowed", i, base.n)
		} else if !errors.Is(err, errInjected) {
			t.Errorf("failure of file operation %d surfaced as %v", i, err)
		}
	}
}

func TestEngineRunUndoCommit(t *testing.T) {
	defer nopanic(t)
	e := testEngine(t, 1<<30)
	e.closeLocked() // nothing open: no-op
	e.closeLocked()

	_, pl, _, err := e.Run(bg, req("SELECT 1"), false)
	if err != nil || pl != nil {
		t.Fatalf("read-only request: %v, %v", pl, err)
	}
	res, pl, _, err := e.Run(bg, req("INSERT INTO nosuch VALUES(1)"), false)
	if pl != nil || (err == nil && res[0].GetE().Error == "") {
		t.Fatalf("failing statement: %v %v %v", res, pl, err)
	}
	_, pl, _, err = e.Run(bg, req("BEGIN", "CREATE TABLE u(x)"), false)
	if !errors.Is(err, ErrTxAcrossRequests) || pl != nil {
		t.Fatalf("open transaction: %v, %v", pl, err)
	}

	pl, id := runOK(t, e, "CREATE TABLE t(x)")
	if pl.PageSize != 4096 || len(pl.Txs) != 1 || pl.Txs[0].DBSize == 0 {
		t.Fatalf("page list %+v", pl)
	}
	if _, _, _, err := e.Run(bg, req("INSERT INTO t VALUES(1)"), false); err == nil || !strings.Contains(err.Error(), "not yet committed") {
		t.Fatalf("second Run before Commit: %v", err)
	}
	if err := e.Undo([idSize]byte{1}); err != nil || e.Stats().Undos != 0 {
		t.Fatalf("Undo of another id: %v, %+v", err, e.Stats())
	}
	if err := e.Undo(id); err != nil || e.Stats().Undos != 1 {
		t.Fatalf("Undo: %v, %+v", err, e.Stats())
	}
	if err := e.Undo(id); err != nil || e.Stats().Undos != 1 {
		t.Fatalf("second Undo: %v, %+v", err, e.Stats())
	}
	if n, _ := (osOps{}).Size(e.walPath); n != 0 {
		t.Fatalf("WAL holds %d bytes after Undo", n)
	}

	pl, id = runOK(t, e, "CREATE TABLE t(x)")
	if err := e.Commit(id, pl); err != nil {
		t.Fatal(err)
	}
	if st := e.Stats(); st.OwnCommits != 1 || st.Applies != 0 || st.Opens < 2 {
		t.Fatalf("stats %+v", st)
	}
	// The Leader's own entry arriving again is not applied twice: with nothing
	// pending it is a foreign entry, so applying is the caller's business; here
	// only the committed end is checked.
	end := e.tl.end
	if end <= walHeaderSize {
		t.Fatalf("committed end %d", end)
	}
	rows, err := e.Query(bg, req("SELECT count(*) FROM t"), false)
	if err != nil || len(rows) != 1 {
		t.Fatalf("Query: %v, %v", rows, err)
	}
}

func TestEngineCommitForeign(t *testing.T) {
	defer nopanic(t)
	const ps = 512
	e := testEngine(t, 1<<30)
	p1, p5, p2 := filled(ps, 1), filled(ps, 5), filled(ps, 2)
	pl := &PageList{PageSize: ps, Txs: []Tx{{Pgnos: []uint32{1, 5}, Pages: [][]byte{p1, p5}, DBSize: 1}}}
	if err := e.Commit([idSize]byte{9}, pl); err != nil {
		t.Fatal(err)
	}
	if st := e.Stats(); st.Applies != 1 || st.Pages != 2 {
		t.Fatalf("stats %+v", st)
	}
	if n, _ := (osOps{}).Size(e.dbPath); n != ps {
		t.Fatalf("database seeded with %d bytes", n)
	}
	// Page 5 lies beyond the size of the last commit: not in the image.
	if img := image(t, e); !bytes.Equal(img, p1) {
		t.Fatalf("image of %d bytes", len(img))
	}
	pl2 := &PageList{PageSize: ps, Txs: []Tx{{Pgnos: []uint32{2}, Pages: [][]byte{p2}, DBSize: 2}}}
	if err := e.Commit([idSize]byte{10}, pl2); err != nil {
		t.Fatal(err)
	}
	if img := image(t, e); !bytes.Equal(img, append(append([]byte(nil), p1...), p2...)) {
		t.Fatalf("image of %d bytes", len(img))
	}
	if st := e.Stats(); st.Applies != 2 || st.Pages != 3 {
		t.Fatalf("stats %+v", st)
	}
	// The database file is seeded once.
	if n, _ := (osOps{}).Size(e.dbPath); n != ps {
		t.Fatalf("database file is %d bytes", n)
	}
	// Another page size is refused and changes nothing.
	end := e.tl.end
	bad := &PageList{PageSize: 1024, Txs: []Tx{{Pgnos: []uint32{1}, Pages: [][]byte{filled(1024, 1)}, DBSize: 1}}}
	if err := e.Commit([idSize]byte{11}, bad); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("page size change: %v", err)
	}
	if e.tl.end != end {
		t.Fatal("a refused entry moved the tail")
	}
	// A page list without page 1 on an empty database seeds nothing.
	e2 := testEngine(t, 1<<30)
	no1 := &PageList{PageSize: ps, Txs: []Tx{{Pgnos: []uint32{2}, Pages: [][]byte{p2}, DBSize: 2}}}
	if err := e2.Commit([idSize]byte{12}, no1); err != nil {
		t.Fatal(err)
	}
	if n, _ := (osOps{}).Size(e2.dbPath); n != 0 {
		t.Fatalf("seeded %d bytes without page 1", n)
	}
}

func TestEnginePageImageExcludesUncommittedFrames(t *testing.T) {
	defer nopanic(t)
	e := testEngine(t, 1<<30)
	pl, id := runOK(t, e, "CREATE TABLE t(x)")
	if err := e.Commit(id, pl); err != nil {
		t.Fatal(err)
	}
	before := image(t, e)
	_, id = runOK(t, e, "INSERT INTO t VALUES(1)")
	if during := image(t, e); !bytes.Equal(before, during) {
		t.Fatal("an uncommitted frame is in the page image")
	}
	if err := e.Undo(id); err != nil {
		t.Fatal(err)
	}
	if after := image(t, e); !bytes.Equal(before, after) {
		t.Fatal("Undo changed the page image")
	}
}

func TestEngineCheckpoint(t *testing.T) {
	defer nopanic(t)
	e := testEngine(t, 1<<30)
	pl, id := runOK(t, e, "CREATE TABLE t(x)")
	if err := e.Commit(id, pl); err != nil {
		t.Fatal(err)
	}
	before := image(t, e)

	_, id = runOK(t, e, "INSERT INTO t VALUES(1)")
	if err := e.Checkpoint(); err == nil || !strings.Contains(err.Error(), "pending") {
		t.Fatalf("Checkpoint with a request pending: %v", err)
	}
	e.Undo(id)

	// The threshold is inclusive: a WAL of exactly ckptBytes is checkpointed.
	e.mu.Lock()
	end := e.tl.end
	e.ckptBytes = end + 1
	err := e.maybeCheckpointLocked()
	n1 := e.stats.Checkpoints
	e.ckptBytes = end
	err2 := e.maybeCheckpointLocked()
	n2 := e.stats.Checkpoints
	e.mu.Unlock()
	if err != nil || err2 != nil || n1 != 0 || n2 != 1 {
		t.Fatalf("threshold: %v %v, checkpoints %d then %d", err, err2, n1, n2)
	}
	if e.tl.end != 0 || !e.tl.valid {
		t.Fatalf("tail after a checkpoint: %+v", e.tl)
	}
	if after := image(t, e); !bytes.Equal(before, after) {
		t.Fatal("a checkpoint changed the page image")
	}

	// A checkpoint that errs, and one that does not complete, change nothing.
	e.ckpt = func(*db.Conn) (*db.CheckpointMeta, error) { return nil, errInjected }
	if err := e.Checkpoint(); !errors.Is(err, errInjected) {
		t.Fatalf("failing checkpoint: %v", err)
	}
	e.ckpt = func(*db.Conn) (*db.CheckpointMeta, error) { return &db.CheckpointMeta{Code: 5}, nil }
	if err := e.Checkpoint(); err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("incomplete checkpoint: %v", err)
	}
	if e.Stats().Checkpoints != 1 {
		t.Fatalf("stats %+v", e.Stats())
	}
}

func TestEngineCheckpointsAfterCommit(t *testing.T) {
	defer nopanic(t)
	// Own entry.
	e := testEngine(t, 1)
	pl, id := runOK(t, e, "CREATE TABLE t(x)")
	if err := e.Commit(id, pl); err != nil {
		t.Fatal(err)
	}
	if e.Stats().Checkpoints != 1 {
		t.Fatalf("own commit: %+v", e.Stats())
	}
	// Foreign entry.
	f := testEngine(t, 1)
	if err := f.Commit([idSize]byte{1}, pl); err != nil {
		t.Fatal(err)
	}
	if f.Stats().Checkpoints != 1 {
		t.Fatalf("foreign commit: %+v", f.Stats())
	}
	if !bytes.Equal(image(t, e), image(t, f)) {
		t.Fatal("images differ")
	}
	// A failing checkpoint is the commit's error.
	g := testEngine(t, 1)
	g.ckpt = func(*db.Conn) (*db.CheckpointMeta, error) { return nil, errInjected }
	if err := g.Commit([idSize]byte{1}, pl); !errors.Is(err, errInjected) {
		t.Fatalf("foreign commit: %v", err)
	}
	h := testEngine(t, 1)
	h.ckpt = g.ckpt
	pl2, id2 := runOK(t, h, "CREATE TABLE t(x)")
	if err := h.Commit(id2, pl2); !errors.Is(err, errInjected) {
		t.Fatalf("own commit: %v", err)
	}
}

func TestEngineConnectionErrors(t *testing.T) {
	defer nopanic(t)
	e := testEngine(t, 1<<30)
	cancelled, cancel := context.WithCancel(bg)
	cancel()
	if _, _, _, err := e.Run(cancelled, req("CREATE TABLE t(x)"), false); err == nil {
		t.Error("Run with a cancelled context")
	}
	if _, err := e.Query(cancelled, req("SELECT 1"), false); err == nil {
		t.Error("Query with a cancelled context")
	}

	// A connection that was closed behind the engine's back.
	e.mu.Lock()
	e.openLocked()
	e.sc.Close()
	e.mu.Unlock()
	if err := e.Checkpoint(); err == nil {
		t.Error("Checkpoint on a closed connection")
	}
	e.mu.Lock()
	e.sc = nil
	e.mu.Unlock()

	// A DSN that cannot be opened.
	e.dsn = "file:wf.db?vfs=no-such-vfs"
	if _, _, _, err := e.Run(bg, req("CREATE TABLE t(x)"), false); err == nil {
		t.Error("Run on an unopenable database")
	}
	if _, err := e.Query(bg, req("SELECT 1"), false); err == nil {
		t.Error("Query on an unopenable database")
	}
	if err := e.Checkpoint(); err == nil {
		t.Error("Checkpoint on an unopenable database")
	}
	if e.Stats().Opens != 1 {
		t.Errorf("stats %+v", e.Stats())
	}
}

func corruptWAL(ps uint32) []byte {
	w := buildWAL(ps)
	w[19] ^= 1
	return w
}

// corruptAfterRun is osOps whose WAL reads return a corrupt WAL once armed.
type corruptAfterRun struct {
	osOps
	armed bool
}

func (c *corruptAfterRun) ReadFile(p string) ([]byte, error) {
	if c.armed && strings.HasSuffix(p, "-wal") {
		return corruptWAL(512), nil
	}
	return c.osOps.ReadFile(p)
}

// A WAL that is corrupt only when read back after the request is reported with
// the request's result and no page list.
func TestEngineRunReadsACorruptWALAfterTheRequest(t *testing.T) {
	defer nopanic(t)
	ops := &corruptAfterRun{}
	e, err := newEngine(t.TempDir(), 1<<30, nil, ops)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	pl0, id0 := runOK(t, e, "CREATE TABLE t(x)")
	if err := e.Commit(id0, pl0); err != nil {
		t.Fatal(err)
	}
	ops.armed = true
	res, pl, _, err := e.Run(bg, req("INSERT INTO t VALUES(1)"), false)
	if err == nil || pl != nil || res == nil {
		t.Errorf("Run: res %v, pl %v, err %v", res, pl, err)
	}
	if e.pend != nil {
		t.Error("a pending request was recorded")
	}
}

func TestPageImageWithAnUnknownTail(t *testing.T) {
	defer nopanic(t)
	e := testEngine(t, 1<<30)
	pl, id := runOK(t, e, "CREATE TABLE t(x)", "INSERT INTO t VALUES(1)")
	if err := e.Commit(id, pl); err != nil {
		t.Fatal(err)
	}
	want := image(t, e)
	e.tl = tail{}
	got, err := e.PageImage()
	if err != nil || !bytes.Equal(got, want) {
		t.Errorf("image with an unknown tail differs: %v", err)
	}
}

func TestEngineCorruptWAL(t *testing.T) {
	defer nopanic(t)
	e := testEngine(t, 1<<30)
	os.WriteFile(e.walPath, corruptWAL(4096), 0o644)
	if _, _, _, err := e.Run(bg, req("CREATE TABLE t(x)"), false); err == nil {
		t.Error("Run found no corrupt WAL")
	}
	if _, err := e.PageImage(); err == nil {
		t.Error("PageImage found no corrupt WAL")
	}
	if _, err := e.snapshotBytes(); err == nil {
		t.Error("snapshotBytes found no corrupt WAL")
	}
	pl := &PageList{PageSize: 512, Txs: []Tx{{Pgnos: []uint32{1}, Pages: [][]byte{filled(512, 1)}, DBSize: 1}}}
	if err := e.Commit([idSize]byte{1}, pl); err == nil {
		t.Error("Commit found no corrupt WAL")
	}

	// The WAL goes bad after the tail is known: Run reports it after the request.
	f := testEngine(t, 1<<30)
	if err := f.Commit([idSize]byte{1}, pl); err != nil {
		t.Fatal(err)
	}
	f.closeLocked()
	os.WriteFile(f.walPath, corruptWAL(512), 0o644)
	if _, pl2, _, err := f.Run(bg, req("SELECT 1"), false); err == nil || pl2 != nil {
		t.Errorf("Run over a corrupt WAL: %v, %v", pl2, err)
	}
}

func TestEngineSnapshotAndRestore(t *testing.T) {
	defer nopanic(t)
	e := testEngine(t, 1<<30)
	pl, id := runOK(t, e, "CREATE TABLE t(x)", "INSERT INTO t VALUES(1)")
	if err := e.Commit(id, pl); err != nil {
		t.Fatal(err)
	}
	_, pid := runOK(t, e, "INSERT INTO t VALUES(2)") // uncommitted: not in the snapshot
	snap, err := e.snapshotBytes()
	if err != nil {
		t.Fatal(err)
	}
	e.Undo(pid)

	f := testEngine(t, 1<<30)
	if err := f.restoreBytes(snap); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(image(t, e), image(t, f)) {
		t.Fatal("restored image differs")
	}
	rows, err := f.Query(bg, req("SELECT count(*) FROM t"), false)
	if err != nil || !strings.Contains(rows[0].String(), "1") || strings.Contains(rows[0].String(), "2") {
		t.Fatalf("restored rows: %v, %v", rows, err)
	}
	// Restoring drops a pending request.
	runOK(t, f, "INSERT INTO t VALUES(5)")
	if err := f.restoreBytes(snap); err != nil {
		t.Fatal(err)
	}
	runOK(t, f, "INSERT INTO t VALUES(6)")

	// A snapshot without a WAL removes the engine's.
	d := testEngine(t, 1<<30)
	empty, err := d.snapshotBytes()
	if err != nil || len(empty) != 8 {
		t.Fatalf("empty snapshot: %d bytes, %v", len(empty), err)
	}
	if err := f.restoreBytes(empty); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(f.walPath); !os.IsNotExist(err) {
		t.Errorf("WAL still there: %v", err)
	}
	if len(image(t, f)) != 0 {
		t.Error("image not empty")
	}

	// Refusals.
	with := func(n uint64, tail []byte) []byte {
		return append(append([]byte{0, 0, 0, 0, 0, 0, 0, byte(n)}, filled(int(n), 1)...), tail...)
	}
	for name, b := range map[string][]byte{
		"short":        snap[:7],
		"nil":          nil,
		"size":         {0, 0, 0, 0, 0, 0, 0, 1},
		"size by one":  with(3, nil)[:10],
		"huge size":    {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		"wal 1 byte":   with(0, []byte{1}),
		"wal zeros":    with(0, make([]byte, 40)),
		"wal checksum": with(0, corruptWAL(512)),
	} {
		if err := f.restoreBytes(b); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if err := f.restoreBytes(with(3, nil)); err != nil { // the size is exact
		t.Errorf("exact size refused: %v", err)
	}
	if err := f.restoreBytes(make([]byte, 8)); err != nil {
		t.Errorf("empty snapshot refused: %v", err)
	}

	// A WAL shorter than the committed end.
	g := testEngine(t, 1<<30)
	if err := g.Commit([idSize]byte{1}, pl); err != nil {
		t.Fatal(err)
	}
	os.Truncate(g.walPath, 40)
	if _, err := g.snapshotBytes(); err == nil || !strings.Contains(err.Error(), "shorter") {
		t.Errorf("short WAL: %v", err)
	}
	os.Remove(g.walPath)
	if _, err := g.snapshotBytes(); err == nil {
		t.Error("missing WAL")
	}
}

func TestNewEngine(t *testing.T) {
	defer nopanic(t)
	d := t.TempDir()
	file := filepath.Join(d, "file")
	os.WriteFile(file, nil, 0o644)
	if _, err := NewEngine(filepath.Join(file, "sub"), 0, nil); err == nil {
		t.Error("directory under a file")
	}

	// The directory is created, nested.
	deep := filepath.Join(d, "a", "b")
	if x, err := NewEngine(deep, 0, nil); err != nil {
		t.Error(err)
	} else {
		x.Close()
		if st, err := os.Stat(deep); err != nil || !st.IsDir() {
			t.Errorf("directory not created: %v", err)
		}
	}

	// A leftover database is removed.
	dir := filepath.Join(d, "e")
	os.MkdirAll(dir, 0o755)
	for _, n := range []string{"wf.db", "wf.db-wal", "wf.db-journal"} {
		os.WriteFile(filepath.Join(dir, n), []byte("old"), 0o644)
	}
	e, err := NewEngine(dir, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for _, n := range []string{"wf.db", "wf.db-wal", "wf.db-journal"} {
		if _, err := os.Stat(filepath.Join(dir, n)); !os.IsNotExist(err) {
			t.Errorf("%s survived: %v", n, err)
		}
	}
	if e.ckptBytes != 4<<20 {
		t.Errorf("default threshold %d", e.ckptBytes)
	}
	if e2, _ := NewEngine(t.TempDir(), -5, nil); e2.ckptBytes != 4<<20 {
		t.Errorf("negative threshold %d", e2.ckptBytes)
	} else {
		e2.Close()
	}
	if e3, _ := NewEngine(t.TempDir(), 7, nil); e3.ckptBytes != 7 {
		t.Errorf("explicit threshold %d", e3.ckptBytes)
	} else {
		e3.Close()
	}

	// A path that cannot be removed.
	stuck := filepath.Join(d, "stuck")
	os.MkdirAll(filepath.Join(stuck, "wf.db", "x"), 0o755)
	if _, err := NewEngine(stuck, 0, nil); err == nil {
		t.Error("unremovable database path")
	}

	old := registerVFS
	registerVFS = func(vfs.VFS) (string, *vfs.FS, error) { return "", nil, errInjected }
	defer func() { registerVFS = old }()
	if _, err := NewEngine(t.TempDir(), 0, nil); !errors.Is(err, errInjected) {
		t.Errorf("registration failure: %v", err)
	}
}

func TestEngineQueryReadsCommittedState(t *testing.T) {
	defer nopanic(t)
	e := testEngine(t, 1<<30)
	pl, id := runOK(t, e, "CREATE TABLE t(x)", "INSERT INTO t VALUES(41)")
	if err := e.Commit(id, pl); err != nil {
		t.Fatal(err)
	}
	rows, err := e.Query(bg, &command.Request{Statements: []*command.Statement{{Sql: "SELECT x+1 FROM t"}}}, false)
	if err != nil || len(rows) != 1 || !strings.Contains(rows[0].String(), "42") {
		t.Fatalf("Query: %v, %v", rows, err)
	}
}
