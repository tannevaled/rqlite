package walframe

// Regression tests added after the first mutation sweep of review round 1:
// each pins a refusal that nothing else held.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

// snapshots returns a snapshot whose state is all in the WAL (sw) and one whose
// state is all in the database file (sd), with their decoded parts.
func snapshots(t *testing.T) (sw, sd *snapshotState, wBytes, dBytes []byte) {
	t.Helper()
	a, b := pair(t, 0)
	pl, id := runOK(t, a, "INSERT INTO t VALUES(1)")
	both(t, a, b, id, pl)
	wBytes, err := b.snapshotBytes()
	if err != nil {
		t.Fatal(err)
	}
	if sw, err = parseSnapshot(wBytes); err != nil {
		t.Fatal(err)
	}
	if len(sw.w) == 0 {
		t.Fatalf("expected a snapshot with a WAL: d=%d w=%d", len(sw.d), len(sw.w))
	}
	if err := b.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	dBytes, err = b.snapshotBytes()
	if err != nil {
		t.Fatal(err)
	}
	if sd, err = parseSnapshot(dBytes); err != nil {
		t.Fatal(err)
	}
	if len(sd.d) == 0 || len(sd.w) != 0 {
		t.Fatalf("expected a database-only snapshot: d=%d w=%d", len(sd.d), len(sd.w))
	}
	return
}

// Every check of parseSnapshot refuses its own kind of snapshot, with its own
// reason.
func TestParseSnapshotRefusals(t *testing.T) {
	defer nopanic(t)
	sw, sd, wb, _ := snapshots(t)
	flipped := append([]byte(nil), wb...)
	flipped[len(flipped)/2] ^= 1
	zeros := func(n int) []byte { return make([]byte, n) }
	extra := append(append([]byte(nil), sw.w...), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	for _, c := range []struct {
		name string
		snap []byte
		want string
	}{
		{"checksum", flipped, "checksum"},
		{"short", wb[:10], "header"},
		{"seq 0 with a database", mkSnap(0, 0, 0, 1, []byte{1}, nil), "no page list"},
		{"seq 0 with a WAL", mkSnap(0, 0, 0, 0, nil, []byte{1}), "no page list"},
		{"seq 0 with a page size", mkSnap(0, 4096, 0, 0, nil, nil), "no page list"},
		{"seq 0 with pages", mkSnap(0, 0, 1, 0, nil, nil), "no page list"},
		{"bad page size", mkSnap(sd.seq, 1000, sd.dbPages, uint64(len(sd.d)), sd.d, nil), "page size"},
		{"no pages", mkSnap(sd.seq, 4096, 0, uint64(len(sd.d)), sd.d, nil), "page size"},
		{"not SQLite", mkSnap(sd.seq, 4096, 1, 200, zeros(200), nil), "not a SQLite"},
		{"tiny database", mkSnap(sd.seq, 4096, 1, 50, zeros(50), nil), "not a SQLite"},
		{"database page size", mkSnap(sd.seq, 512, sd.dbPages, uint64(len(sd.d)), sd.d, nil), "does not match the page size"},
		{"database length", mkSnap(sd.seq, 4096, sd.dbPages, uint64(len(sd.d)-1), sd.d[:len(sd.d)-1], nil), "does not match the page size"},
		{"WAL garbage", mkSnap(sw.seq, 4096, sw.dbPages, uint64(len(sd.d)), sd.d, zeros(40)), "WAL:"},
		{"WAL not whole", mkSnap(sw.seq, 4096, sw.dbPages, uint64(len(sd.d)), sd.d, extra), "not whole"},
		{"WAL page size", mkSnap(sw.seq, 512, sw.dbPages, uint64(len(sd.d)), sd.d, sw.w), "does not match"},
		{"no data", mkSnap(sw.seq, 4096, 1, 0, nil, nil), "not a SQLite"},
	} {
		_, err := parseSnapshot(c.snap)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want %q", c.name, err, c.want)
		}
	}
	for name, s := range map[string][]byte{
		"WAL":      mkSnap(sw.seq, 4096, sw.dbPages, uint64(len(sd.d)), sd.d, sw.w),
		"database": mkSnap(sd.seq, 4096, sd.dbPages, uint64(len(sd.d)), sd.d, nil),
		"empty":    mkSnap(0, 0, 0, 0, nil, nil),
	} {
		if _, err := parseSnapshot(s); err != nil {
			t.Errorf("good %s snapshot: %v", name, err)
		}
	}
}

// A snapshot that is well formed but that SQLite cannot read is refused, and the
// node keeps its own state.
func TestRestoreRefusesACorruptDatabase(t *testing.T) {
	defer nopanic(t)
	_, sd, _, _ := snapshots(t)
	_, b := pair(t, 0)
	bad := append([]byte(nil), sd.d...)
	for i := 4096; i < 2*4096 && i < len(bad); i++ {
		bad[i] = 0xFF
	}
	err := b.restoreBytes(mkSnap(sd.seq, 4096, sd.dbPages, uint64(len(bad)), bad, nil))
	if err == nil || !strings.Contains(err.Error(), "does not verify") {
		t.Fatalf("corrupt database: %v", err)
	}
	if b.fatal != nil {
		t.Errorf("the engine stopped: %v", b.fatal)
	}
	integrityOK(t, b)
}

// scratchFail fails one operation on the scratch copy a snapshot is verified in.
type scratchFail struct {
	osOps
	op, suffix string
}

func (s *scratchFail) hit(op, p string) bool {
	return s.op == op && strings.Contains(p, ".verify-") && strings.HasSuffix(p, s.suffix)
}

func (s *scratchFail) Remove(p string) error {
	if s.hit("remove", p) {
		return errInjected
	}
	return s.osOps.Remove(p)
}

func (s *scratchFail) WriteFile(p string, b []byte) error {
	if s.hit("write", p) {
		return errInjected
	}
	return s.osOps.WriteFile(p, b)
}

// Every way the scratch verification can fail refuses the snapshot.
func TestRestoreVerificationFailures(t *testing.T) {
	defer nopanic(t)
	_, _, wb, db := snapshots(t)
	for _, c := range []struct {
		name, op, suffix string
		snap             []byte
	}{
		{"scratch engine", "remove", "wf.db", wb},
		{"database write", "write", "wf.db", db},
		{"WAL write", "write", "-wal", wb},
	} {
		e, err := newEngine(t.TempDir(), 0, nil, &scratchFail{op: c.op, suffix: c.suffix})
		if err != nil {
			t.Fatal(err)
		}
		if err := e.restoreBytes(c.snap); !errors.Is(err, errInjected) {
			t.Errorf("%s: %v", c.name, err)
		}
		if e.seq != 0 {
			t.Errorf("%s: something was installed", c.name)
		}
		e.Close()
	}
	if os.Geteuid() == 0 {
		return
	}
	e := testEngine(t, 0)
	if err := os.Chmod(e.dir, 0o500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(e.dir, 0o700)
	if err := e.restoreBytes(wb); err == nil || !strings.Contains(err.Error(), ".verify-") {
		t.Errorf("no scratch directory: %v", err)
	}
}

// A stopped engine neither commits nor snapshots.
func TestStoppedEngineNeitherCommitsNorSnapshots(t *testing.T) {
	defer nopanic(t)
	a, b := pair(t, 0)
	pl, id := runOK(t, a, "INSERT INTO t VALUES(1)")
	b.Fail(errInjected)
	if err := b.Commit(id, pl); !errors.Is(err, ErrStopped) {
		t.Errorf("Commit: %v", err)
	}
	if _, err := b.snapshotBytes(); !errors.Is(err, ErrStopped) {
		t.Errorf("snapshot: %v", err)
	}
}

// Transactions that cannot be applied are refused whatever else they carry.
func TestMalformedTransactionsAreRefused(t *testing.T) {
	defer nopanic(t)
	_, b := pair(t, 0)
	img := image(t, b)
	pg := img[:4096]
	for name, tx := range map[string]Tx{
		"no pages":   {DBSize: b.dbPages},
		"mismatch":   {Pgnos: []uint32{1, 2}, Pages: [][]byte{pg}, DBSize: 2},
		"no size":    {Pgnos: []uint32{1}, Pages: [][]byte{pg}},
		"no page nr": {Pgnos: []uint32{0}, Pages: [][]byte{pg}, DBSize: 1},
	} {
		evil := based(b, &PageList{PageSize: 4096, Txs: []Tx{tx}})
		if err := b.Commit([idSize]byte{7}, evil); !errors.Is(err, ErrRejected) {
			t.Errorf("%s: %v", name, err)
		}
	}
	integrityOK(t, b)
}

// The first entry must carry page 1 even when its page numbers are within the
// database size it declares.
func TestFirstEntryWithoutPageOneButConsistentSize(t *testing.T) {
	defer nopanic(t)
	c := testEngine(t, 0)
	pg := make([]byte, 4096)
	evil := based(c, &PageList{PageSize: 4096, Txs: []Tx{{Pgnos: []uint32{2, 2}, Pages: [][]byte{pg, pg}, DBSize: 2}}})
	if err := c.Commit([idSize]byte{1}, evil); !errors.Is(err, ErrRejected) || !strings.Contains(err.Error(), "page 1") {
		t.Fatalf("first entry without page 1: %v", err)
	}
	if c.seq != 0 {
		t.Error("the chain moved")
	}
}

// The Leader's own entry, refused by the check, is undone; if the undo fails
// the engine stops and says so.
func TestOwnRejectedEntryWithFailingUndo(t *testing.T) {
	defer nopanic(t)
	tf := &truncFail{}
	a, err := newEngine(t.TempDir(), 1<<40, nil, tf)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	pl, id := runOK(t, a, "CREATE TABLE t(x)")
	if err := a.Commit(id, pl); err != nil {
		t.Fatal(err)
	}
	pl, id = runOK(t, a, "INSERT INTO t VALUES(1)")
	pl.BaseSeq++ // stale
	tf.armed = true
	err = a.Commit(id, pl)
	if !errors.Is(err, ErrRejected) || !errors.Is(err, errInjected) {
		t.Fatalf("Commit: %v", err)
	}
	if _, qerr := a.Query(bg, req("SELECT 1"), false); !errors.Is(qerr, ErrStopped) {
		t.Errorf("Query on the stopped engine: %v", qerr)
	}
}

// corruptNonEmpty serves a corrupt WAL in place of any WAL that holds data, so
// it is the read after the request, not the one before, that sees it.
type corruptNonEmpty struct{ osOps }

func (c *corruptNonEmpty) ReadFile(p string) ([]byte, error) {
	b, err := c.osOps.ReadFile(p)
	if strings.HasSuffix(p, "-wal") && len(b) > 0 {
		return corruptWAL(512), nil
	}
	return b, err
}

// A WAL that cannot be read as the first request is read back is reported, and
// nothing is shipped.
func TestRunReadsACorruptWALOnTheFirstRequest(t *testing.T) {
	defer nopanic(t)
	ops := &corruptNonEmpty{}
	e, err := newEngine(t.TempDir(), 1<<30, nil, ops)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	_, pl, _, err := e.Run(bg, req("CREATE TABLE t(x)"), false)
	if err == nil || pl != nil {
		t.Errorf("Run: pl %v, err %v", pl, err)
	}
}

// shortHeader serves a WAL header that is too short.
type shortHeader struct {
	osOps
	armed bool
}

func (s *shortHeader) ReadRange(p string, off, n int64) ([]byte, error) {
	if s.armed && strings.HasSuffix(p, "-wal") && off == 0 {
		return make([]byte, 10), nil
	}
	return s.osOps.ReadRange(p, off, n)
}

func TestRunWithAShortWALHeader(t *testing.T) {
	defer nopanic(t)
	ops := &shortHeader{}
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
	_, pl, _, err := e.Run(bg, req("INSERT INTO t VALUES(1)"), false)
	if err == nil || pl != nil {
		t.Errorf("Run: pl %v, err %v", pl, err)
	}
}

// forged serves no database file and a given WAL.
type forged struct {
	osOps
	w []byte
}

func (f *forged) ReadFile(p string) ([]byte, error) {
	if strings.HasSuffix(p, "-wal") {
		return f.w, nil
	}
	return nil, nil
}

// A WAL that claims a database larger than the files can hold is refused before
// an image of that size is allocated.
func TestPageImageRefusesAHugeClaim(t *testing.T) {
	defer nopanic(t)
	hdr, tl := newWALHeader(4096, [2]uint32{1, 2})
	w := frameTxs(&tl, hdr, []Tx{{Pgnos: []uint32{1}, Pages: [][]byte{make([]byte, 4096)}, DBSize: 1000}})
	e, err := newEngine(t.TempDir(), 0, nil, &forged{w: w})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, err := e.PageImage(); err == nil || !strings.Contains(err.Error(), "claims") {
		t.Fatalf("PageImage: %v", err)
	}
	hdr, tl = newWALHeader(4096, [2]uint32{1, 2})
	w = frameTxs(&tl, hdr, []Tx{{Pgnos: []uint32{1}, Pages: [][]byte{make([]byte, 4096)}, DBSize: 1}})
	e.ops = &forged{w: w}
	if img, err := e.PageImage(); err != nil || len(img) != 4096 {
		t.Fatalf("PageImage of one page: %d bytes, %v", len(img), err)
	}
}

// Page sizes are validated when a list is encoded and when it is decoded.
func TestPageListPageSizeIsValidated(t *testing.T) {
	defer nopanic(t)
	pg := make([]byte, 1000)
	_, err := MarshalPageList(&PageList{PageSize: 1000, Txs: []Tx{{Pgnos: []uint32{1}, Pages: [][]byte{pg}, DBSize: 1}}})
	if !errors.Is(err, ErrBadPageList) || !strings.Contains(err.Error(), "page size") {
		t.Errorf("Marshal: %v", err)
	}
	good, err := MarshalPageList(&PageList{PageSize: 4096, Txs: []Tx{{Pgnos: []uint32{1}, Pages: [][]byte{make([]byte, 4096)}, DBSize: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint32(good[4:], 1000)
	if _, err := UnmarshalPageList(good); !errors.Is(err, ErrBadPageList) || !strings.Contains(err.Error(), "page size") {
		t.Errorf("Unmarshal: %v", err)
	}
}

// Restore reads at most the bound it was given.
func TestRestoreSnapshotBound(t *testing.T) {
	defer nopanic(t)
	n := NewNode(testEngine(t, 0), nil)
	n.maxSnapshot = 10
	err := n.Restore(io.NopCloser(bytes.NewReader(make([]byte, 11))))
	if err == nil || !strings.Contains(err.Error(), "larger") {
		t.Errorf("11 bytes: %v", err)
	}
	err = n.Restore(io.NopCloser(bytes.NewReader(make([]byte, 10))))
	if err == nil || strings.Contains(err.Error(), "larger") {
		t.Errorf("10 bytes: %v", err)
	}
}

// ReadRange reports a read that fails.
func TestReadRangeErrors(t *testing.T) {
	defer nopanic(t)
	dir := t.TempDir()
	if _, err := (osOps{}).ReadRange(dir, 0, -1); err == nil {
		t.Error("a directory was read as a file")
	}
	f := dir + "/f"
	if err := (osOps{}).WriteFile(f, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := (osOps{}).ReadRange(f+"/g", 0, -1); !errors.Is(err, syscall.ENOTDIR) {
		t.Errorf("a path under a file: %v", err)
	}
}

// Access reports a missing file as absent, not as an error.
func TestVFSAccess(t *testing.T) {
	defer nopanic(t)
	dir := t.TempDir()
	v := &fileVFS{dir: dir}
	if ok, err := v.Access("nope", 0); ok || err != nil {
		t.Errorf("missing: %v %v", ok, err)
	}
	if err := os.WriteFile(dir+"/here", []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if ok, err := v.Access("here", 0); !ok || err != nil {
		t.Errorf("present: %v %v", ok, err)
	}
}

// Opening an open engine reuses its connection: a second connection would
// block on the exclusive lock.
func TestOpenLockedIsIdempotent(t *testing.T) {
	defer nopanic(t)
	e := testEngine(t, 0)
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.openLocked(); err != nil {
		t.Fatal(err)
	}
	first := e.sc
	done := make(chan error, 1)
	go func() { done <- e.openLocked() }()
	select {
	case err := <-done:
		if err != nil || e.sc != first {
			t.Errorf("second open: %v, same connection %v", err, e.sc == first)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the second open blocks: it made a new connection")
	}
}
