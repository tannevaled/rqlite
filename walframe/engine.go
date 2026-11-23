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
	"sync"

	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/db"
	"modernc.org/sqlite/vfs"
)

// Errors returned by the engine.
var (
	// ErrTxAcrossRequests is returned when a request leaves a transaction open.
	ErrTxAcrossRequests = errors.New("walframe: a transaction cannot span requests")
)

// registerVFS is vfs.Register, replaceable by tests.
var registerVFS = vfs.Register

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
	// tail is the tail of the WAL as the Leader left it.
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
	guard     db.Guard
	ops       fileOps
	// ckpt runs the node-local checkpoint; tests replace it.
	ckpt func(*db.Conn) (*db.CheckpointMeta, error)

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
	return newEngine(dir, ckptBytes, logger, osOps{})
}

func newEngine(dir string, ckptBytes int64, logger *log.Logger, ops fileOps) (*Engine, error) {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	if ckptBytes <= 0 {
		ckptBytes = 4 << 20
	}
	e := &Engine{dir: dir, logger: logger, ckptBytes: ckptBytes, v: &fileVFS{dir: dir}, ops: ops,
		ckpt: func(c *db.Conn) (*db.CheckpointMeta, error) { return c.Checkpoint(db.CheckpointTruncate) }}
	e.dbPath = filepath.Join(dir, "wf.db")
	e.walPath = e.dbPath + "-wal"
	for _, p := range []string{e.dbPath, e.walPath, e.dbPath + "-journal"} {
		if err := e.ops.Remove(p); err != nil {
			return nil, err
		}
	}
	name, fsys, err := registerVFS(e.v)
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
	// The guard is an SQLite authorizer refusing, while a user request runs,
	// the statements that write pages or change file-level settings outside
	// the replicated WAL (see doc.go).
	sc, err := db.OpenSingleConn(db.ConnConfig{Guard: &e.guard}, e.dsn, e.logger)
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

// ensureTailLocked loads the committed tail from the WAL file when it is unknown.
func (e *Engine) ensureTailLocked() error {
	if e.tl.valid {
		return nil
	}
	w, err := e.ops.ReadFile(e.walPath)
	if err != nil {
		return err
	}
	s, err := scanOrEmpty(w)
	if err != nil {
		return err
	}
	e.tl = tailFromScan(s)
	return nil
}

// tailFromScan is the tail after the last commit of s; the empty scan gives
// the empty tail.
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
	e.guard.Set(true)
	res, rerr := conn.Request(ctx, req, xTime)
	e.guard.Set(false)
	// A request that leaves a transaction open would hold frames back from the
	// WAL and let the next request commit them.
	if in, ierr := conn.InTransaction(); ierr == nil && in {
		conn.Request(context.Background(), &command.Request{Statements: []*command.Statement{{Sql: "ROLLBACK"}}}, false)
		rerr = errors.Join(rerr, ErrTxAcrossRequests)
	}
	conn.Close()

	w, err := e.ops.ReadFile(e.walPath)
	if err != nil {
		return res, nil, id, errors.Join(rerr, err)
	}
	s, err := scanOrEmpty(w)
	if err != nil {
		return res, nil, id, errors.Join(rerr, err)
	}
	// Frames before the committed end are Raft's; a WAL with no frames (or
	// none past it) means this request wrote nothing.
	from := max(e.tl.end, walHeaderSize)
	if s.CommitEnd <= from {
		return res, nil, id, rerr
	}
	rand.Read(id[:]) // never fails
	e.pend = &pending{id: id, end: s.CommitEnd, tail: tailFromScan(s)}
	return res, &PageList{PageSize: s.Header.PageSize, Txs: extractTxs(w, s, from, s.CommitEnd)}, id, rerr
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
	if err := e.ops.Truncate(e.walPath, e.tl.end); err != nil {
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
	off := t.end
	if t.end == 0 {
		var salt [8]byte
		rand.Read(salt[:]) // never fails
		out, t = newWALHeader(pl.PageSize, [2]uint32{binary.BigEndian.Uint32(salt[:4]), binary.BigEndian.Uint32(salt[4:])})
		off = 0 // out starts with the new header
	}
	out = frameTxs(&t, out, pl.Txs)
	pages := 0
	for _, tx := range pl.Txs {
		pages += len(tx.Pages)
	}
	if err := e.seedDB(pl); err != nil {
		return err
	}
	// Truncate drops any stale tail beyond the new end.
	if err := e.ops.WriteAtTruncate(e.walPath, out, off, t.end); err != nil {
		return err
	}
	e.tl = t
	e.stats.Applies++
	e.stats.Pages += pages
	return nil
}

// seedDB gives an empty database file page 1. SQLite deletes a WAL it finds
// next to an empty database file (pagerOpenWalIfPresent: nPage==0), and a node
// that has only ever applied page lists has no database file; page 1 as the
// first transaction wrote it is enough, the WAL overlays it anyway.
func (e *Engine) seedDB(pl *PageList) error {
	n, err := e.ops.Size(e.dbPath)
	if err != nil || n > 0 {
		return err
	}
	for _, tx := range pl.Txs {
		for i, pg := range tx.Pages {
			if tx.Pgnos[i] == 1 {
				return e.ops.WriteFile(e.dbPath, pg)
			}
		}
	}
	return nil
}

// maybeCheckpointLocked runs a node-local TRUNCATE checkpoint when the WAL is
// large. Checkpoints are never replicated: the page list is independent of
// each node's WAL layout.
func (e *Engine) maybeCheckpointLocked() error {
	if e.tl.end < e.ckptBytes {
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
	meta, err := e.ckpt(conn)
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

// Checkpoint runs a node-local checkpoint now (refused with a request pending).
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
	e.guard.Set(true)
	defer e.guard.Set(false)
	return conn.Query(ctx, req, xTime)
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
	d, err := e.ops.ReadFile(e.dbPath)
	if err != nil {
		return nil, err
	}
	w, err := e.ops.ReadFile(e.walPath)
	if err != nil {
		return nil, err
	}
	s, err := scanOrEmpty(w)
	if err != nil {
		return nil, err
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
	d, err := e.ops.ReadFile(e.dbPath)
	if err != nil {
		return nil, err
	}
	var w []byte
	if e.tl.end > 0 {
		w, err = e.ops.ReadFile(e.walPath)
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
	if err := e.ops.WriteFile(e.dbPath, d); err != nil {
		return err
	}
	if len(w) == 0 {
		if err := e.ops.Remove(e.walPath); err != nil {
			return err
		}
	} else if err := e.ops.WriteFile(e.walPath, w); err != nil {
		return err
	}
	e.tl = tail{}
	return e.ensureTailLocked()
}
