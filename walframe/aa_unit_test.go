package walframe

// Unit tests of the pure parts: file operations, the VFS, the WAL format and
// the page-list codec. The file name sorts them before the cluster tests, and
// nopanic turns a panic into a failure: a mutant that panics must fail by an
// assertion, not end the test binary and hide the mutants after it.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"modernc.org/sqlite/vfs"
)

func nopanic(t *testing.T) {
	t.Helper()
	if r := recover(); r != nil {
		t.Fatalf("panic: %v", r)
	}
}

func filled(n int, b byte) []byte { return bytes.Repeat([]byte{b}, n) }

func TestOsOps(t *testing.T) {
	defer nopanic(t)
	var o osOps
	d := t.TempDir()
	p := filepath.Join(d, "f")

	if b, err := o.ReadFile(p); b != nil || err != nil {
		t.Fatalf("ReadFile missing = %v, %v", b, err)
	}
	if err := o.Remove(p); err != nil {
		t.Fatalf("Remove missing: %v", err)
	}
	if err := o.Truncate(p, 0); err != nil {
		t.Fatalf("Truncate missing: %v", err)
	}
	if n, err := o.Size(p); n != 0 || err != nil {
		t.Fatalf("Size missing = %d, %v", n, err)
	}

	if err := o.WriteFile(p, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if b, err := o.ReadFile(p); string(b) != "hello" || err != nil {
		t.Fatalf("ReadFile = %q, %v", b, err)
	}
	if n, err := o.Size(p); n != 5 || err != nil {
		t.Fatalf("Size = %d, %v", n, err)
	}
	if err := o.Truncate(p, 2); err != nil {
		t.Fatal(err)
	}
	if n, _ := o.Size(p); n != 2 {
		t.Fatalf("Size after Truncate = %d", n)
	}
	if err := o.Remove(p); err != nil {
		t.Fatal(err)
	}
	if n, _ := o.Size(p); n != 0 {
		t.Fatal("file still there")
	}

	q := filepath.Join(d, "g")
	if err := o.WriteAtTruncate(q, []byte("abc"), 2, 4); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(q); !bytes.Equal(b, []byte{0, 0, 'a', 'b'}) {
		t.Fatalf("WriteAtTruncate wrote %q", b)
	}
	if err := o.WriteAtTruncate(q, []byte("abc"), -1, 4); err == nil {
		t.Fatal("negative offset accepted")
	}
	if err := o.WriteAtTruncate(q, []byte("abc"), 0, -1); err == nil {
		t.Fatal("negative size accepted")
	}

	// A directory is not a file, and a regular file is not a directory.
	sub := filepath.Join(d, "dir")
	if err := os.MkdirAll(filepath.Join(sub, "x"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := o.WriteFile(q, nil); err != nil {
		t.Fatal(err)
	}
	under := filepath.Join(q, "x") // ENOTDIR
	if _, err := o.ReadFile(sub); err == nil {
		t.Error("ReadFile of a directory succeeded")
	}
	if err := o.WriteFile(sub, nil); err == nil {
		t.Error("WriteFile of a directory succeeded")
	}
	if err := o.Remove(sub); err == nil {
		t.Error("Remove of a non-empty directory succeeded")
	}
	if err := o.Truncate(sub, 0); err == nil {
		t.Error("Truncate of a directory succeeded")
	}
	if err := o.WriteAtTruncate(sub, nil, 0, 0); err == nil {
		t.Error("WriteAtTruncate of a directory succeeded")
	}
	if _, err := o.Size(under); err == nil {
		t.Error("Size under a regular file succeeded")
	}
	if err := o.Remove(under); err == nil {
		t.Error("Remove under a regular file succeeded")
	}
	if err := o.Truncate(under, 0); err == nil {
		t.Error("Truncate under a regular file succeeded")
	}
	if b, err := o.ReadFile(under); err == nil {
		t.Errorf("ReadFile under a regular file = %v", b)
	}
}

func TestFileVFS(t *testing.T) {
	defer nopanic(t)
	d := t.TempDir()
	v := &fileVFS{dir: d}

	if _, _, err := v.Open("a.db", 0); err == nil {
		t.Fatal("Open without create of a missing file succeeded")
	}
	f, fl, err := v.Open("/some/where/a.db", vfs.OpenCreate|vfs.OpenReadWrite) // only the base name counts
	if err != nil || fl != vfs.OpenCreate|vfs.OpenReadWrite {
		t.Fatalf("Open = %v, %v", fl, err)
	}
	if _, err := os.Stat(filepath.Join(d, "a.db")); err != nil {
		t.Fatal(err)
	}
	if n, err := f.WriteAt([]byte("hello"), 0); n != 5 || err != nil {
		t.Fatalf("WriteAt = %d, %v", n, err)
	}
	if err := f.Sync(0); err != nil {
		t.Fatal(err)
	}
	if n, err := f.Size(); n != 5 || err != nil {
		t.Fatalf("Size = %d, %v", n, err)
	}
	p := make([]byte, 5)
	if n, err := f.ReadAt(p, 0); n != 5 || err != nil || string(p) != "hello" {
		t.Fatalf("ReadAt = %d, %v, %q", n, err, p)
	}
	if err := f.Truncate(2); err != nil {
		t.Fatal(err)
	}
	if n, _ := f.Size(); n != 2 {
		t.Fatalf("Size after Truncate = %d", n)
	}

	v.fenced.Store(true)
	if _, err := f.WriteAt([]byte("x"), 0); !errors.Is(err, errFenced) {
		t.Errorf("fenced WriteAt = %v", err)
	}
	if err := f.Truncate(0); !errors.Is(err, errFenced) {
		t.Errorf("fenced Truncate = %v", err)
	}
	if err := v.Delete("a.db", false); !errors.Is(err, errFenced) {
		t.Errorf("fenced Delete = %v", err)
	}
	if n, _ := f.Size(); n != 2 {
		t.Errorf("fenced operations reached the file: size %d", n)
	}
	v.fenced.Store(false)

	if ok, err := v.Access("a.db", 0); !ok || err != nil {
		t.Errorf("Access existing = %v, %v", ok, err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Size(); err == nil {
		t.Error("Size after Close succeeded")
	}

	ro, _, err := v.Open("a.db", vfs.OpenReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ro.WriteAt([]byte("x"), 0); err == nil {
		t.Error("WriteAt on a read-only open succeeded")
	}
	ro.Close()

	if err := v.Delete("a.db", false); err != nil {
		t.Fatal(err)
	}
	if ok, err := v.Access("a.db", 0); ok || err != nil {
		t.Errorf("Access missing = %v, %v", ok, err)
	}
	if err := v.Delete("a.db", false); err == nil {
		t.Error("Delete of a missing file succeeded")
	}

	file := filepath.Join(d, "plain")
	os.WriteFile(file, nil, 0o644)
	bad := &fileVFS{dir: file} // every path is under a regular file: ENOTDIR
	if ok, err := bad.Access("x", 0); ok || err == nil {
		t.Errorf("Access under a regular file = %v, %v", ok, err)
	}
	if s, err := v.FullPathname("n"); s != "n" || err != nil {
		t.Errorf("FullPathname = %q, %v", s, err)
	}
}

// --- WAL format ---

func buildWAL(ps uint32, txs ...Tx) []byte {
	hdr, t := newWALHeader(ps, [2]uint32{7, 9})
	return frameTxs(&t, hdr, txs)
}

func oneTx(ps int, pg uint32, commit uint32, b byte) Tx {
	return Tx{Pgnos: []uint32{pg}, Pages: [][]byte{filled(ps, b)}, DBSize: commit}
}

func TestValidPageSize(t *testing.T) {
	defer nopanic(t)
	for n, want := range map[uint32]bool{0: false, 256: false, 511: false, 512: true, 513: false, 1000: false,
		4096: true, 65536: true, 131072: false, 65535: false} {
		if got := validPageSize(n); got != want {
			t.Errorf("validPageSize(%d) = %v", n, got)
		}
	}
}

func TestScanWALHeaderErrors(t *testing.T) {
	defer nopanic(t)
	good := buildWAL(512)
	if len(good) != walHeaderSize {
		t.Fatalf("header only is %d bytes", len(good))
	}
	if s, err := scanWAL(good); err != nil || s.CommitEnd != walHeaderSize || len(s.Valid) != 0 {
		t.Fatalf("header-only scan = %+v, %v", s, err)
	}
	if _, err := scanWAL(good[:walHeaderSize-1]); !errors.Is(err, errNoHeader) {
		t.Errorf("short header: %v", err)
	}
	if _, err := scanWAL(nil); !errors.Is(err, errNoHeader) {
		t.Errorf("nil: %v", err)
	}
	if _, err := scanWAL(make([]byte, 64)); !errors.Is(err, errNoHeader) {
		t.Errorf("zeros: %v", err)
	}
	bad := append([]byte(nil), good...)
	binary.BigEndian.PutUint32(bad[8:], 1000)
	if _, err := scanWAL(bad); err == nil || errors.Is(err, errNoHeader) || !strings.Contains(err.Error(), "page size") {
		t.Errorf("bad page size: %v", err)
	}
	bad = append([]byte(nil), good...)
	bad[19] ^= 1 // a salt byte: the header checksum no longer matches
	if _, err := scanWAL(bad); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Errorf("bad checksum: %v", err)
	}
	if _, err := scanOrEmpty(bad); err == nil {
		t.Error("scanOrEmpty swallowed a checksum error")
	}
	if _, err := scanOrEmpty(append(good[:0:0], bad[:8]...)); err != nil {
		t.Errorf("scanOrEmpty short: %v", err)
	}
	if s, err := scanOrEmpty(nil); err != nil || s.CommitEnd != 0 {
		t.Errorf("scanOrEmpty nil = %+v, %v", s, err)
	}
	if s, err := scanOrEmpty(good); err != nil || s.CommitEnd != walHeaderSize || s.Header.PageSize != 512 {
		t.Errorf("scanOrEmpty good = %+v, %v", s, err)
	}
}

func TestScanWALFrames(t *testing.T) {
	defer nopanic(t)
	const ps = 512
	fsz := int64(frameHeaderSize + ps)
	w := buildWAL(ps,
		Tx{Pgnos: []uint32{1, 2}, Pages: [][]byte{filled(ps, 1), filled(ps, 2)}, DBSize: 2},
		oneTx(ps, 2, 2, 3))
	s, err := scanWAL(w)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Valid) != 3 || s.CommitEnd != walHeaderSize+3*fsz || s.CommitEnd != int64(len(w)) {
		t.Fatalf("scan: %d frames, end %d, len %d", len(s.Valid), s.CommitEnd, len(w))
	}
	if s.Valid[0].Commit != 0 || s.Valid[1].Commit != 2 || s.Valid[2].Commit != 2 ||
		s.Valid[0].Pgno != 1 || s.Valid[1].Pgno != 2 || s.Valid[0].Offset != walHeaderSize {
		t.Fatalf("frames: %+v", s.Valid)
	}

	// A frame in the middle of a transaction is not valid until its commit.
	s2, _ := scanWAL(w[:walHeaderSize+fsz])
	if len(s2.Valid) != 0 || s2.CommitEnd != walHeaderSize {
		t.Errorf("uncommitted frame counted: %+v", s2)
	}
	// A partial last frame stops the scan; the committed prefix stays.
	s2, _ = scanWAL(w[:len(w)-1])
	if len(s2.Valid) != 2 || s2.CommitEnd != walHeaderSize+2*fsz {
		t.Errorf("partial frame: %d frames, end %d", len(s2.Valid), s2.CommitEnd)
	}
	// The last frame exactly fits.
	s2, _ = scanWAL(w)
	if len(s2.Valid) != 3 {
		t.Errorf("exact fit: %d frames", len(s2.Valid))
	}
	// A bad frame checksum, and a foreign salt, each stop the scan before the frame.
	for name, off := range map[string]int64{"checksum": 16, "salt1": 8, "salt2": 12} {
		b := append([]byte(nil), w...)
		b[walHeaderSize+2*fsz+off+3] ^= 1
		s2, _ = scanWAL(b)
		if len(s2.Valid) != 2 || s2.CommitEnd != walHeaderSize+2*fsz {
			t.Errorf("%s: %d frames, end %d", name, len(s2.Valid), s2.CommitEnd)
		}
	}
	// Page contents are covered by the checksum.
	b := append([]byte(nil), w...)
	b[walHeaderSize+2*fsz+frameHeaderSize+5] ^= 1
	if s2, _ = scanWAL(b); len(s2.Valid) != 2 {
		t.Errorf("corrupt page: %d frames", len(s2.Valid))
	}
	// Frame header fields before the checksum are covered too.
	b = append([]byte(nil), w...)
	b[walHeaderSize+2*fsz+3] ^= 1
	if s2, _ = scanWAL(b); len(s2.Valid) != 2 {
		t.Errorf("corrupt frame header: %d frames", len(s2.Valid))
	}
}

func TestScanWALLittleEndian(t *testing.T) {
	defer nopanic(t)
	const ps = 512
	be := binary.BigEndian
	hdr := make([]byte, walHeaderSize)
	be.PutUint32(hdr[0:], walMagicLE)
	be.PutUint32(hdr[4:], 3007000)
	be.PutUint32(hdr[8:], ps)
	be.PutUint32(hdr[16:], 5)
	be.PutUint32(hdr[20:], 6)
	tl := tail{valid: true, pageSize: ps, bigEnd: false, salt: [2]uint32{5, 6}, end: walHeaderSize}
	tl.c1, tl.c2 = cksum(false, hdr[:24], 0, 0)
	be.PutUint32(hdr[24:], tl.c1)
	be.PutUint32(hdr[28:], tl.c2)
	w := frameTxs(&tl, hdr, []Tx{oneTx(ps, 1, 1, 9)})
	s, err := scanWAL(w)
	if err != nil || len(s.Valid) != 1 {
		t.Fatalf("LE scan: %+v, %v", s, err)
	}
	if got := tailFromScan(s); got.bigEnd || got.c1 != tl.c1 || got.c2 != tl.c2 || got.end != tl.end {
		t.Errorf("tail = %+v, want %+v", got, tl)
	}
	// The same bytes read as big endian must not validate.
	if c1, c2 := cksum(true, hdr[:24], 0, 0); c1 == tl.c1 && c2 == tl.c2 {
		t.Error("endianness does not change the checksum")
	}
}

func TestNewWALHeaderAndFrameTxs(t *testing.T) {
	defer nopanic(t)
	hdr, tl := newWALHeader(1024, [2]uint32{3, 4})
	if len(hdr) != walHeaderSize || tl.end != walHeaderSize || !tl.valid || !tl.bigEnd || tl.pageSize != 1024 || tl.salt != [2]uint32{3, 4} {
		t.Fatalf("header %d bytes, tail %+v", len(hdr), tl)
	}
	s, err := scanWAL(hdr)
	if err != nil || s.Header.PageSize != 1024 || s.Header.Salt1 != 3 || s.Header.Salt2 != 4 ||
		s.Header.Version != 3007000 || s.Header.Magic != walMagicBE || s.Header.CkptSeq != 0 {
		t.Fatalf("header scan: %+v, %v", s.Header, err)
	}
	if tl.c1 != s.Header.Cksum1 || tl.c2 != s.Header.Cksum2 {
		t.Error("tail checksum is not the header's")
	}
	before := tl
	out := frameTxs(&tl, nil, []Tx{{Pgnos: []uint32{4, 2}, Pages: [][]byte{filled(1024, 1), filled(1024, 2)}, DBSize: 7}})
	if len(out) != 2*(frameHeaderSize+1024) || tl.end != before.end+int64(len(out)) || (tl.c1 == before.c1 && tl.c2 == before.c2) {
		t.Fatalf("frameTxs: %d bytes, tail %+v", len(out), tl)
	}
	if binary.BigEndian.Uint32(out[4:]) != 0 || binary.BigEndian.Uint32(out[frameHeaderSize+1024+4:]) != 7 {
		t.Error("only the last frame of a transaction carries the database size")
	}
	if binary.BigEndian.Uint32(out[0:]) != 4 || binary.BigEndian.Uint32(out[frameHeaderSize+1024:]) != 2 {
		t.Error("page numbers")
	}
	if got := frameTxs(&tl, nil, nil); len(got) != 0 {
		t.Error("no transaction, some bytes")
	}
}

func TestExtractTxs(t *testing.T) {
	defer nopanic(t)
	const ps = 512
	fsz := int64(frameHeaderSize + ps)
	w := buildWAL(ps,
		Tx{Pgnos: []uint32{1, 2}, Pages: [][]byte{filled(ps, 1), filled(ps, 2)}, DBSize: 2},
		oneTx(ps, 3, 3, 3), oneTx(ps, 1, 3, 4))
	s, _ := scanWAL(w)
	all := extractTxs(w, s, walHeaderSize, s.CommitEnd)
	if len(all) != 3 || len(all[0].Pgnos) != 2 || all[0].DBSize != 2 || all[1].Pgnos[0] != 3 || all[2].Pages[0][0] != 4 ||
		all[0].Pages[1][0] != 2 || len(all[0].Pages[0]) != ps {
		t.Fatalf("all: %+v", all)
	}
	// [from, to): from is inclusive, to exclusive, both on frame boundaries.
	got := extractTxs(w, s, walHeaderSize+2*fsz, walHeaderSize+3*fsz)
	if len(got) != 1 || got[0].Pgnos[0] != 3 {
		t.Errorf("second tx only: %+v", got)
	}
	got = extractTxs(w, s, walHeaderSize+3*fsz, s.CommitEnd)
	if len(got) != 1 || got[0].Pages[0][0] != 4 {
		t.Errorf("last tx only: %+v", got)
	}
	got = extractTxs(w, s, walHeaderSize, walHeaderSize+2*fsz)
	if len(got) != 1 || len(got[0].Pgnos) != 2 {
		t.Errorf("first tx only: %+v", got)
	}
	if got = extractTxs(w, s, s.CommitEnd, s.CommitEnd); len(got) != 0 {
		t.Errorf("empty range: %+v", got)
	}
	// The pages are copies.
	all[1].Pages[0][0] = 99
	if w[walHeaderSize+2*fsz+frameHeaderSize] == 99 {
		t.Error("extracted page aliases the WAL")
	}
}

func TestTailFromScan(t *testing.T) {
	defer nopanic(t)
	w := buildWAL(512, oneTx(512, 1, 1, 1))
	s, _ := scanWAL(w)
	tl := tailFromScan(s)
	if !tl.valid || tl.end != int64(len(w)) || tl.pageSize != 512 || tl.salt != [2]uint32{7, 9} || !tl.bigEnd ||
		tl.c1 != s.Valid[0].C1 || tl.c2 != s.Valid[0].C2 {
		t.Errorf("tail = %+v", tl)
	}
	s, _ = scanWAL(buildWAL(512))
	tl = tailFromScan(s)
	if tl.c1 != s.Header.Cksum1 || tl.c2 != s.Header.Cksum2 || tl.end != walHeaderSize {
		t.Errorf("header-only tail = %+v", tl)
	}
	if e := tailFromScan(walScan{}); !e.valid || e.end != 0 {
		t.Errorf("empty tail = %+v", e)
	}
}

// --- page-list codec ---

func plBytes(ps uint32, parts ...uint32) []byte {
	b := append([]byte(pageListMagic), 0, 0, 0, 0)
	binary.BigEndian.PutUint32(b[4:], ps)
	for _, p := range parts {
		b = binary.BigEndian.AppendUint32(b, p)
	}
	return b
}

func TestMarshalPageListRefusals(t *testing.T) {
	defer nopanic(t)
	pg := filled(512, 1)
	cases := map[string]*PageList{
		"page size":    {PageSize: 100},
		"short pgnos":  {PageSize: 512, Txs: []Tx{{Pgnos: nil, Pages: [][]byte{pg}, DBSize: 1}}},
		"short pages":  {PageSize: 512, Txs: []Tx{{Pgnos: []uint32{1}, Pages: nil, DBSize: 1}}},
		"empty tx":     {PageSize: 512, Txs: []Tx{{DBSize: 1}}},
		"short page":   {PageSize: 512, Txs: []Tx{{Pgnos: []uint32{1}, Pages: [][]byte{pg[:511]}, DBSize: 1}}},
		"long page":    {PageSize: 512, Txs: []Tx{{Pgnos: []uint32{1}, Pages: [][]byte{append(pg[:512:512], 0)}, DBSize: 1}}},
		"second tx bad": {PageSize: 512, Txs: []Tx{{Pgnos: []uint32{1}, Pages: [][]byte{pg}, DBSize: 1}, {DBSize: 1}}},
	}
	for name, pl := range cases {
		if b, err := MarshalPageList(pl); !errors.Is(err, ErrBadPageList) || b != nil {
			t.Errorf("%s: %v, %v", name, b, err)
		}
	}
	ok := &PageList{PageSize: 512, Txs: []Tx{{Pgnos: []uint32{1}, Pages: [][]byte{pg}, DBSize: 1}}}
	b, err := MarshalPageList(ok)
	if err != nil || !bytes.Equal(b, encodePageList(ok)) {
		t.Fatalf("MarshalPageList: %v", err)
	}
	if want := 12 + 8 + 4 + 512; len(b) != want {
		t.Errorf("len = %d, want %d", len(b), want)
	}
	if b, err := MarshalPageList(&PageList{PageSize: 512}); err != nil || len(b) != 12 {
		t.Errorf("empty list: %d, %v", len(b), err)
	}
}

func TestUnmarshalPageListBoundaries(t *testing.T) {
	defer nopanic(t)
	pg := filled(512, 7)
	one := func() []byte { return append(plBytes(512, 1, 1, 1, 5), pg...) } // 1 tx, dbSize 1, 1 page, pgno 5
	cases := []struct {
		name string
		b    []byte
		ok   bool
	}{
		{"nil", nil, false},
		{"11 bytes", plBytes(512, 0)[:11], false},
		{"header only, no tx", plBytes(512, 0), true},
		{"bad magic", append([]byte("XXXX"), plBytes(512, 0)[4:]...), false},
		{"bad page size", plBytes(1000, 0), false},
		{"tx count without data", plBytes(512, 1), false},
		{"7 bytes of tx header", append(plBytes(512, 1), 0, 0, 0, 1, 0, 0, 0), false},
		{"dbSize 0", append(plBytes(512, 1, 0, 1, 5), pg...), false},
		{"nPg 0", plBytes(512, 1, 1, 0), false},
		{"nPg huge", plBytes(512, 1, 1, 0xffffffff), false},
		{"nPg without pages", plBytes(512, 1, 1, 1, 5), false},
		{"page one byte short", one()[:len(one())-1], false},
		{"pgno 0", append(plBytes(512, 1, 1, 1, 0), pg...), false},
		{"trailing byte", append(one(), 0), false},
		{"one tx", one(), true},
		{"nTx huge", plBytes(512, 0xffffffff), false},
	}
	for _, c := range cases {
		pl, err := UnmarshalPageList(c.b)
		if c.ok != (err == nil) {
			t.Errorf("%s: err = %v", c.name, err)
			continue
		}
		if err != nil && (!errors.Is(err, ErrBadPageList) || pl != nil) {
			t.Errorf("%s: %v %v", c.name, pl, err)
		}
	}
	pl, _ := UnmarshalPageList(one())
	if pl.PageSize != 512 || len(pl.Txs) != 1 || pl.Txs[0].DBSize != 1 || pl.Txs[0].Pgnos[0] != 5 || !bytes.Equal(pl.Txs[0].Pages[0], pg) {
		t.Errorf("decoded %+v", pl)
	}
	in := one()
	pl, _ = UnmarshalPageList(in)
	in[len(in)-1] ^= 0xff
	if pl.Txs[0].Pages[0][511] != 7 {
		t.Error("decoded page aliases the input")
	}
	empty, err := UnmarshalPageList(plBytes(512, 0))
	if err != nil || empty.PageSize != 512 || len(empty.Txs) != 0 {
		t.Errorf("empty list: %+v, %v", empty, err)
	}
}

func TestCommandCodec(t *testing.T) {
	defer nopanic(t)
	pl := &PageList{PageSize: 512, Txs: []Tx{{Pgnos: []uint32{2}, Pages: [][]byte{filled(512, 3)}, DBSize: 2}}}
	id := [idSize]byte{1, 2, 3, 15: 16}
	b := marshalCommand(id, pl)
	if b[0] != cmdPages || !bytes.Equal(b[1:1+idSize], id[:]) {
		t.Fatalf("header: %v", b[:1+idSize])
	}
	gid, gpl, err := unmarshalCommand(b)
	if err != nil || gid != id || gpl.Txs[0].Pgnos[0] != 2 {
		t.Fatalf("roundtrip: %v %+v %v", gid, gpl, err)
	}
	for name, bad := range map[string][]byte{
		"nil": nil, "short": b[:idSize], "no body": b[:1+idSize-1],
		"type": append([]byte{9}, b[1:]...), "body": append(append([]byte{cmdPages}, id[:]...), 'x'),
	} {
		if _, _, err := unmarshalCommand(bad); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	// 17 bytes is a command whose page list is empty: refused as a page list, not as a header.
	if _, _, err := unmarshalCommand(b[:1+idSize]); err == nil || !errors.Is(err, ErrBadPageList) {
		t.Errorf("17 bytes: %v", err)
	}
	// A command header of exactly 17 bytes passes the header test.
	if _, _, err := unmarshalCommand(append(b[:1+idSize:1+idSize], encodePageList(&PageList{PageSize: 512})...)); err != nil {
		t.Errorf("empty list command: %v", err)
	}
}
