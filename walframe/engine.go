package walframe

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/db"
	"modernc.org/sqlite/vfs"
)

// Errors returned by the engine.
var (
	// ErrRefused is returned for a statement this mode cannot replicate.
	ErrRefused = errors.New("walframe: statement refused in WAL-frame replication mode")
	// ErrTxAcrossRequests is returned when a request leaves a transaction open.
	ErrTxAcrossRequests = errors.New("walframe: a transaction cannot span requests")
)

// refused matches the statements that write pages or change file-level
// settings outside the replicated WAL, which would make the nodes diverge or
// break the single exclusive connection (see doc.go). A text match is a
// prototype shortcut: it can be bypassed, an authorizer hook cannot.
var refused = regexp.MustCompile(`(?is)^\s*(attach\b|pragma\s+([a-z_]+\.)?(journal_mode|wal_checkpoint|wal_autocheckpoint|locking_mode|page_size|auto_vacuum|synchronous|writable_schema|temp_store|mmap_size)\b)`)

// Stats are counters of the engine, for tests and measurement.
type Stats struct {
	Opens       int // connections opened (each one runs WAL recovery)
	Applies     int // foreign page lists appended
	OwnCommits  int // own entries confirmed
	Undos       int
	Checkpoints int
	Pages       int // pages appended by Applies
}

// tail is the Raft-committed end of THIS node's WAL: where the next
// replicated frame goes, under which salts and checksum chain. end == 0
// means there is no WAL.
type tail struct {
	valid    bool
	end      int64
	pageSize uint32
	salt     [2]uint32
	c1, c2   uint32
	bigEnd   bool
}

type pending struct {
	id  [idSize]byte
	end int64
	// sc is the scan of the WAL as the Leader left it, for the new tail.
	tail tail
}

// Engine is one node's database: a directory holding the database file and
// its WAL, accessed through a VFS this package owns, with at most one SQLite
// connection open (exclusive locking, no shared memory).
type Engine struct {
	dir       string
	logger    *log.Logger
	v         *fileVFS
	vname     string
	fsys      *vfs.FS
	ckptBytes int64

	mu      sync.Mutex
	sc      *db.SingleConn
	tl      tail
	pend    *pending
	stats   Stats
	dsn     string
	dbPath  string
	walPath string
}

// NewEngine creates the directory's database from scratch: any file from a
// previous run is removed, because state is rebuilt from the Raft snapshot and
// log (see doc.go). ckptBytes is the WAL size above which a node-local
// checkpoint runs; zero means 4 MiB.
func NewEngine(dir string, ckptBytes int64, logger *log.Logger) (*Engine, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	if ckptBytes <= 0 {
		ckptBytes = 4 << 20
	}
	e := &Engine{dir: dir, logger: logger, ckptBytes: ckptBytes, v: &fileVFS{dir: dir}}
	e.dbPath = filepath.Join(dir, "wf.db")
	e.walPath = e.dbPath + "-wal"
	for _, p := range []string{e.dbPath, e.walPath, e.dbPath + "-journal"} {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	name, fsys, err := vfs.Register(e.v)
	if err != nil {
		return nil, err
	}
	e.vname, e.fsys = name, fsys
	e.dsn = "file:wf.db?vfs=" + name
	return e, nil
}

// Close closes the connection (fenced) and unregisters the VFS.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closeLocked()
	return e.fsys.Close()
}

// Stats returns a copy of the counters.
func (e *Engine) Stats() Stats {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stats
}

func (e *Engine) openLocked() error {
	if e.sc != nil {
		return nil
	}
	sc, err := db.OpenSingleConn(db.ConnConfig{}, e.dsn, e.logger)
	if err != nil {
		return err
	}
	e.sc = sc
	e.stats.Opens++
	return nil
}

// closeLocked closes the connection with storage FENCED: SQLite's work on
// close (a checkpoint, a WAL delete) must never reach the files, or a
// transaction Raft has not committed could become durable in the database
// file. The close error is the fence working, and is ignored.
func (e *Engine) closeLocked() {
	if e.sc == nil {
		return
	}
	e.v.fenced.Store(true)
	e.sc.Close()
	e.v.fenced.Store(false)
	e.sc = nil
}

// ensureTail loads the committed tail from the WAL file when it is unknown.
func (e *Engine) ensureTailLocked() error {
	if e.tl.valid {
		return nil
	}
	w, err := os.ReadFile(e.walPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	t, err := tailFromWAL(w)
	if err != nil {
		return err
	}
	e.tl = t
	return nil
}

func tailFromWAL(w []byte) (tail, error) {
	s, err := scanWAL(w)
	if err != nil {
		if errors.Is(err, errNoHeader) {
			return tail{valid: true}, nil // no WAL: an empty tail
		}
		return tail{}, err
	}
	return tailFromScan(s), nil
}

func tailFromScan(s walScan) tail {
	t := tail{valid: true, end: s.CommitEnd, pageSize: s.Header.PageSize,
		salt: [2]uint32{s.Header.Salt1, s.Header.Salt2}, c1: s.Header.Cksum1, c2: s.Header.Cksum2,
		bigEnd: s.Header.Magic&1 == 1}
	if n := len(s.Valid); n > 0 {
		t.c1, t.c2 = s.Valid[n-1].C1, s.Valid[n-1].C2
	}
	return t
}

// Run executes req on the Leader's database and returns the responses and, if
// the request changed the database, the page list to replicate with the ID
// that will identify its entry. The frames stay local and UNCOMMITTED until
// Commit(id) is called for them (by the FSM when the entry applies) or Undo(id)
// removes them. The caller must not call Run again, and must not run reads,
// before one of those two.
func (e *Engine) Run(ctx context.Context, req *command.Request, xTime bool) ([]*command.ExecuteQueryResponse, *PageList, [idSize]byte, error) {
	var id [idSize]byte
	for _, st := range req.Statements {
		if refused.MatchString(st.Sql) {
			return nil, nil, id, fmt.Errorf("%w: %.40q", ErrRefused, st.Sql)
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.pend != nil {
		return nil, nil, id, errors.New("walframe: previous request not yet committed or undone")
	}
	if err := e.ensureTailLocked(); err != nil {
		return nil, nil, id, err
	}
	if err := e.openLocked(); err != nil {
		return nil, nil, id, err
	}
	conn, err := e.sc.Acquire(ctx)
	if err != nil {
		return nil, nil, id, err
	}
	res, rerr := conn.Request(ctx, req, xTime)
	// A request that leaves a transaction open would hold frames back from the
	// WAL and let the next request commit them.
	if in, ierr := conn.InTransaction(); ierr == nil && in {
		conn.Request(context.Background(), &command.Request{Statements: []*command.Statement{{Sql: "ROLLBACK"}}}, false)
		rerr = errors.Join(rerr, ErrTxAcrossRequests)
	}
	conn.Close()

	w, err := os.ReadFile(e.walPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return res, nil, id, errors.Join(rerr, err)
	}
	s, err := scanWAL(w)
	if err != nil {
		return res, nil, id, rerr // no WAL: nothing was written
	}
	from := e.tl.end
	if from == 0 || s.Header.Salt1 != e.tl.salt[0] || s.Header.Salt2 != e.tl.salt[1] {
		from = walHeaderSize // a new WAL generation
	}
	if s.CommitEnd <= from {
		return res, nil, id, rerr
	}
	txs := extractTxs(w, s, from, s.CommitEnd)
	if len(txs) == 0 {
		return res, nil, id, rerr
	}
	if _, err := rand.Read(id[:]); err != nil {
		return res, nil, id, err
	}
	e.pend = &pending{id: id, end: s.CommitEnd, tail: tailFromScan(s)}
	return res, &PageList{PageSize: s.Header.PageSize, Txs: txs}, id, rerr
}

// Undo removes the local frames of request id, which Raft did not commit:
// fenced close, truncate the WAL to the Raft-committed end, and leave the
// connection closed (the next use reopens, running WAL recovery on the
// truncated file). Truncating under an open connection would leave it serving
// the removed frames from its heap wal-index; closing unfenced would
// checkpoint them into the database file. No-op if id is not the pending one
// (it was committed, or already undone).
func (e *Engine) Undo(id [idSize]byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.pend == nil || e.pend.id != id {
		return nil
	}
	return e.undoLocked()
}

func (e *Engine) undoLocked() error {
	e.closeLocked()
	e.pend = nil
	e.stats.Undos++
	if err := os.Truncate(e.walPath, e.tl.end); err != nil && !errors.Is(err, os.ErrNotExist) {
		e.tl.valid = false
		return err
	}
	return nil
}

// Commit is called by the FSM for every replicated entry on every node.
// For the Leader's own entry it only moves the committed end; for any other
// entry it appends the pages to this node's WAL.
func (e *Engine) Commit(id [idSize]byte, pl *PageList) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.pend != nil && e.pend.id == id {
		e.tl = e.pend.tail
		e.pend = nil
		e.stats.OwnCommits++
		return e.maybeCheckpointLocked()
	}
	if e.pend != nil {
		// Another node's entry arrives while this node still holds local
		// frames nobody committed: they are orphans, the entry goes first.
		if err := e.undoLocked(); err != nil {
			return err
		}
	}
	if err := e.ensureTailLocked(); err != nil {
		return err
	}
	if e.tl.end != 0 && e.tl.pageSize != pl.PageSize {
		return fmt.Errorf("walframe: page size %d does not match the WAL's %d", pl.PageSize, e.tl.pageSize)
	}
	// The connection's heap wal-index would not see the new frames.
	e.closeLocked()
	if err := e.appendLocked(pl); err != nil {
		e.tl.valid = false
		return err
	}
	return e.maybeCheckpointLocked()
}

// appendLocked re-frames pl into this node's own WAL at the committed end,
// with this node's salts and checksums (dqlite's vfsWalAppend).
func (e *Engine) appendLocked(pl *PageList) error {
	t := e.tl
	var out []byte
	fresh := t.end == 0
	if fresh {
		var salt [8]byte
		if _, err := rand.Read(salt[:]); err != nil {
			return err
		}
		t = tail{valid: true, pageSize: pl.PageSize, bigEnd: true,
			salt: [2]uint32{binary.BigEndian.Uint32(salt[:4]), binary.BigEndian.Uint32(salt[4:])}}
		hdr := make([]byte, walHeaderSize)
		be := binary.BigEndian
		be.PutUint32(hdr[0:], walMagicBE)
		be.PutUint32(hdr[4:], 3007000)
		be.PutUint32(hdr[8:], pl.PageSize)
		be.PutUint32(hdr[16:], t.salt[0])
		be.PutUint32(hdr[20:], t.salt[1])
		t.c1, t.c2 = cksum(true, hdr[:24], 0, 0)
		be.PutUint32(hdr[24:], t.c1)
		be.PutUint32(hdr[28:], t.c2)
		out = hdr
		t.end = walHeaderSize
	}
	be := binary.BigEndian
	start := t.end
	for _, tx := range pl.Txs {
		for i, pg := range tx.Pages {
			fh := make([]byte, frameHeaderSize)
			be.PutUint32(fh[0:], tx.Pgnos[i])
			if i == len(tx.Pages)-1 {
				be.PutUint32(fh[4:], tx.DBSize)
			}
			be.PutUint32(fh[8:], t.salt[0])
			be.PutUint32(fh[12:], t.salt[1])
			t.c1, t.c2 = cksum(t.bigEnd, fh[:8], t.c1, t.c2)
			t.c1, t.c2 = cksum(t.bigEnd, pg, t.c1, t.c2)
			be.PutUint32(fh[16:], t.c1)
			be.PutUint32(fh[20:], t.c2)
			out = append(out, fh...)
			out = append(out, pg...)
			t.end += int64(len(fh) + len(pg))
		}
		e.stats.Pages += len(tx.Pages)
	}
	// SQLite deletes a WAL it finds next to an empty database file
	// (pagerOpenWalIfPresent: nPage==0). A node that has only ever applied
	// page lists has no database file, so seed it with page 1 as the first
	// transaction wrote it; the WAL overlays it anyway.
	if st, serr := os.Stat(e.dbPath); serr != nil || st.Size() == 0 {
		for _, tx := range pl.Txs {
			for i, pg := range tx.Pages {
				if tx.Pgnos[i] == 1 {
					if err := os.WriteFile(e.dbPath, pg, 0o644); err != nil {
						return err
					}
					goto seeded
				}
			}
		}
	}
seeded:
	f, err := os.OpenFile(e.walPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	off := start
	if fresh {
		off = 0 // out starts with the new header
	}
	if _, err := f.WriteAt(out, off); err != nil {
		return err
	}
	if err := f.Truncate(t.end); err != nil { // drop any stale tail beyond the new end
		return err
	}
	e.tl = t
	e.stats.Applies++
	return nil
}

// maybeCheckpointLocked runs a node-local TRUNCATE checkpoint when the WAL is
// large. Checkpoints are never replicated: the page list is independent of
// each node's WAL layout.
func (e *Engine) maybeCheckpointLocked() error {
	if e.pend != nil || e.tl.end < e.ckptBytes {
		return nil
	}
	return e.checkpointLocked()
}

func (e *Engine) checkpointLocked() error {
	if err := e.openLocked(); err != nil {
		return err
	}
	conn, err := e.sc.Acquire(context.Background())
	if err != nil {
		return err
	}
	meta, err := conn.Checkpoint(db.CheckpointTruncate)
	conn.Close()
	if err != nil {
		return err
	}
	if !meta.Success() {
		return fmt.Errorf("walframe: checkpoint incomplete (%s)", meta)
	}
	e.closeLocked()
	e.tl = tail{valid: true}
	e.stats.Checkpoints++
	return nil
}

// Checkpoint runs a node-local checkpoint now (a no-op with a request pending).
func (e *Engine) Checkpoint() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.pend != nil {
		return errors.New("walframe: request pending")
	}
	return e.checkpointLocked()
}

// Query runs read-only statements on this node's committed state. The caller
// serialises it against Leader requests (see Node).
func (e *Engine) Query(ctx context.Context, req *command.Request, xTime bool) ([]*command.QueryRows, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.openLocked(); err != nil {
		return nil, err
	}
	conn, err := e.sc.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return conn.Query(ctx, req, xTime)
}

// Time-measuring helper for callers: reopen cost of the connection now.
func (e *Engine) Reopen() (time.Duration, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closeLocked()
	t0 := time.Now()
	err := e.openLocked()
	return time.Since(t0), err
}

// PageImage returns the database as SQLite sees its committed pages: the
// database file overlaid with the committed frames of the WAL, truncated to
// the size of the last commit. It is computed from the files, independent of
// any SQLite connection, so equal images mean equal pages.
func (e *Engine) PageImage() ([]byte, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.ensureTailLocked(); err != nil {
		return nil, err
	}
	d, err := os.ReadFile(e.dbPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	w, err := os.ReadFile(e.walPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	s, err := scanWAL(w)
	if err != nil {
		return d, nil
	}
	ps := int64(s.Header.PageSize)
	size := int64(len(d))
	for _, f := range s.Valid {
		if f.Offset >= e.tl.end {
			break
		}
		if f.Commit != 0 {
			size = int64(f.Commit) * ps
		}
	}
	img := make([]byte, size)
	copy(img, d)
	for _, f := range s.Valid {
		if f.Offset >= e.tl.end {
			break
		}
		o := (int64(f.Pgno) - 1) * ps
		if o+ps <= size {
			copy(img[o:o+ps], w[f.Offset+frameHeaderSize:f.Offset+frameHeaderSize+ps])
		}
	}
	return img, nil
}

// snapshotBytes captures the committed state: the database file and the
// committed prefix of the WAL. Local uncommitted frames are excluded.
func (e *Engine) snapshotBytes() ([]byte, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.ensureTailLocked(); err != nil {
		return nil, err
	}
	d, err := os.ReadFile(e.dbPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	var w []byte
	if e.tl.end > 0 {
		w, err = os.ReadFile(e.walPath)
		if err != nil {
			return nil, err
		}
		if int64(len(w)) < e.tl.end {
			return nil, errors.New("walframe: WAL shorter than its committed end")
		}
		w = w[:e.tl.end]
	}
	b := binary.BigEndian.AppendUint64(nil, uint64(len(d)))
	b = append(b, d...)
	return append(b, w...), nil
}

// restoreBytes replaces this node's database with a snapshot.
func (e *Engine) restoreBytes(b []byte) error {
	if len(b) < 8 {
		return errors.New("walframe: short snapshot")
	}
	n := binary.BigEndian.Uint64(b)
	if n > uint64(len(b)-8) {
		return errors.New("walframe: bad snapshot")
	}
	d, w := b[8:8+n], b[8+n:]
	if len(w) > 0 {
		if _, err := scanWAL(w); err != nil {
			return fmt.Errorf("walframe: snapshot WAL: %w", err)
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closeLocked()
	e.pend = nil
	if err := os.WriteFile(e.dbPath, d, 0o644); err != nil {
		return err
	}
	if len(w) == 0 {
		if err := os.Remove(e.walPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	} else if err := os.WriteFile(e.walPath, w, 0o644); err != nil {
		return err
	}
	e.tl = tail{}
	return e.ensureTailLocked()
}
