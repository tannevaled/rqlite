package walframe

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/db"
	"modernc.org/sqlite/vfs"
)

// Errors returned by the engine.
var (
	// ErrTxAcrossRequests is returned when a request leaves a transaction open.
	ErrTxAcrossRequests = errors.New("walframe: a transaction cannot span requests")

	// ErrRejected wraps every refusal of an entry that is DETERMINISTIC: the same
	// entry is refused by every node, because the refusal depends only on the
	// entry and on the replicated state (see Engine.Commit). A rejected entry
	// changes nothing; the node carries on.
	ErrRejected = errors.New("walframe: entry rejected")

	// ErrStaleBase is the ErrRejected of an entry computed on a state that is
	// not the one it applies to.
	ErrStaleBase = fmt.Errorf("%w: stale base", ErrRejected)

	// ErrEntryTooLarge is returned by Run, before anything is proposed, for a
	// request whose page list would exceed the entry bound.
	ErrEntryTooLarge = errors.New("walframe: request changes too many pages for one Raft entry")

	// ErrStopped wraps the failure that stopped an engine: it serves nothing
	// until a Restore rebuilds it.
	ErrStopped = errors.New("walframe: engine stopped after a failure")
)

// Defaults of the configurable bounds.
const (
	defaultMaxEntryBytes = 8 << 20
	defaultQueryTimeout  = 30 * time.Second
	defaultMaxSnapshot   = 1 << 32
	sqliteHeaderMagic    = "SQLite format 3\x00"
	snapshotMagic        = "WFS1"
	snapshotHeaderSize   = 4 + 8 + HashSize + 4 + 4 + 8
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
	Rejected    int // entries refused deterministically
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

	// The replicated state every entry is checked against. It is a function of
	// the entries applied, so it is the same on every node at the same index,
	// and a snapshot carries it.
	seq      uint64         // page lists applied
	head     [HashSize]byte // hash chain over their IDs
	pageSize uint32         // fixed by the first page list, 0 before
	dbPages  uint32         // database size in pages after the last transaction

	// fatal, once set, stops the engine: a failure left the files in a state
	// this node cannot vouch for, and only a Restore rebuilds them.
	fatal error
	// onStop, if set, is called (in its own goroutine) when the engine stops. The
	// Node uses it to give up Raft leadership: a stopped Leader fails every write.
	onStop func()

	maxEntry     int64
	queryTimeout time.Duration
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
		maxEntry: defaultMaxEntryBytes, queryTimeout: defaultQueryTimeout,
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

// SetMaxEntryBytes bounds the size of the Raft entry one request may produce;
// Run refuses a larger one before anything is proposed. Zero or less removes
// the bound.
func (e *Engine) SetMaxEntryBytes(n int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.maxEntry = n
}

// SetQueryTimeout bounds the time one read may hold the engine, and so the
// time the FSM can wait behind it. Zero or less removes the bound.
func (e *Engine) SetQueryTimeout(d time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.queryTimeout = d
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

// Fatal returns the failure that stopped the engine, nil while it runs.
func (e *Engine) Fatal() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.fatal
}

// Fail stops the engine with err (the first failure is kept): no Run, Commit,
// Query, Checkpoint or snapshot is served until a Restore.
func (e *Engine) Fail(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.failLocked(err)
}

func (e *Engine) failLocked(err error) {
	if e.fatal == nil && err != nil {
		e.fatal = err
		e.logger.Printf("walframe: engine stopped: %v", err)
		if e.onStop != nil {
			// Not under e.mu: the hook talks to Raft, whose FSM goroutine may be
			// waiting for e.mu.
			go e.onStop()
		}
	}
}

// stoppedLocked is the error every entry point returns once the engine stopped.
func (e *Engine) stoppedLocked() error {
	if e.fatal == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrStopped, e.fatal)
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
	if err := e.stoppedLocked(); err != nil {
		return nil, nil, id, err
	}
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

	pl, id, err := e.collectLocked()
	if err != nil {
		return res, nil, [idSize]byte{}, errors.Join(rerr, err)
	}
	return res, pl, id, rerr
}

// collectLocked looks at what the request left in the WAL past the committed
// end. It reads only that region (the frames before it are Raft's), so its
// cost follows the request, not the history. Nothing there means the request
// wrote nothing; anything there that is not shipped (a rolled-back spill, an
// oversized request, an unreadable WAL) is cut off before returning, so that
// the next request can never ship it.
func (e *Engine) collectLocked() (*PageList, [idSize]byte, error) {
	var id [idSize]byte
	var (
		frames    []frame
		commitEnd int64
		endTail   tail
		buf       []byte
		base      int64
		ps        uint32
		size      int64
		from      int64
	)
	if e.tl.end == 0 {
		w, err := e.ops.ReadFile(e.walPath)
		if err != nil {
			return nil, id, e.discardLocked(err)
		}
		s, err := scanOrEmpty(w)
		if err != nil {
			return nil, id, e.discardLocked(err)
		}
		frames, commitEnd, endTail, buf, ps, size = s.Valid, s.CommitEnd, tailFromScan(s), w, s.Header.PageSize, int64(len(w))
		from = walHeaderSize
	} else {
		hdr, err := e.ops.ReadRange(e.walPath, 0, walHeaderSize)
		if err == nil && !walHeaderIs(hdr, e.tl) {
			err = errors.New("walframe: the WAL header is not the committed one")
		}
		var region []byte
		if err == nil {
			region, err = e.ops.ReadRange(e.walPath, e.tl.end, -1)
		}
		if err != nil {
			return nil, id, e.discardLocked(err)
		}
		frames, commitEnd, endTail = scanTail(region, e.tl)
		buf, base, ps, size, from = region, e.tl.end, e.tl.pageSize, e.tl.end+int64(len(region)), e.tl.end
	}
	if commitEnd <= from {
		if size > e.tl.end {
			return nil, id, e.discardLocked(nil) // frames of a rolled-back transaction
		}
		return nil, id, nil
	}
	if e.maxEntry > 0 {
		if est := int64(len(frames))*(int64(ps)+4) + pageListHeaderSize; est > e.maxEntry {
			return nil, id, e.discardLocked(fmt.Errorf("%w: %d bytes, the bound is %d", ErrEntryTooLarge, est, e.maxEntry))
		}
	}
	rand.Read(id[:]) // never fails
	e.pend = &pending{id: id, end: commitEnd, tail: endTail}
	return &PageList{PageSize: ps, BaseSeq: e.seq, BaseHash: e.head,
		Txs: extractTxsAt(buf, base, frames, int64(ps), from, commitEnd)}, id, nil
}

// walHeaderIs reports whether hdr is the header of the WAL tail t belongs to.
func walHeaderIs(hdr []byte, t tail) bool {
	if len(hdr) < walHeaderSize {
		return false
	}
	be := binary.BigEndian
	return be.Uint32(hdr[8:]) == t.pageSize && be.Uint32(hdr[16:]) == t.salt[0] && be.Uint32(hdr[20:]) == t.salt[1]
}

// discardLocked cuts the WAL back to the Raft-committed end, with the
// connection closed (fenced), and returns cause. If the cut fails the engine
// stops: it cannot tell what the WAL holds past the committed end.
func (e *Engine) discardLocked(cause error) error {
	e.closeLocked()
	if err := e.ops.Truncate(e.walPath, e.tl.end); err != nil {
		err = fmt.Errorf("walframe: cannot cut the WAL back to its committed end: %w", err)
		e.failLocked(err)
		return errors.Join(cause, err)
	}
	return cause
}

// Undo removes the local frames of request id, which Raft did not commit:
// fenced close, truncate the WAL to the Raft-committed end, and leave the
// connection closed (the next use reopens, running WAL recovery on the
// truncated file). Truncating under an open connection would leave it serving
// the removed frames from its heap wal-index; closing unfenced would
// checkpoint them into the database file. No-op if id is not the pending one
// (it was committed, or already undone). If the truncation fails the engine
// stops: the committed end is never re-derived from a WAL that holds frames
// Raft refused.
func (e *Engine) Undo(id [idSize]byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.pend == nil || e.pend.id != id {
		return nil
	}
	return e.undoLocked()
}

func (e *Engine) undoLocked() error {
	e.pend = nil
	e.stats.Undos++
	return e.discardLocked(nil)
}

// checkLocked refuses, deterministically, an entry that does not apply to the
// replicated state: a base other than the node's current one, a page size other
// than the cluster's, a first entry without page 1, a page beyond the database
// size, a database size larger than the pages supplied can make. All of these
// are functions of the entry and of (seq, head, pageSize, dbPages), which every
// node has equal at equal indices, so every node refuses alike.
//
// Same idea as the checks dqlite makes on a frame list (vfsWalAppend,
// canonical/dqlite src/vfs.c) and as LiteFS's pre-apply checksum (superfly/litefs
// db.go, ApplyLTX: "pre-apply checksum mismatch"): an entry only applies on the
// state it was computed from.
func (e *Engine) checkLocked(pl *PageList) error {
	if pl.BaseSeq != e.seq || pl.BaseHash != e.head {
		return fmt.Errorf("%w: built on state %d, this node is at %d", ErrStaleBase, pl.BaseSeq, e.seq)
	}
	if e.pageSize != 0 && pl.PageSize != e.pageSize {
		return fmt.Errorf("%w: page size %d, the database's is %d", ErrRejected, pl.PageSize, e.pageSize)
	}
	if len(pl.Txs) == 0 {
		return fmt.Errorf("%w: no transaction", ErrRejected)
	}
	// Before the first entry the database has no pages, so the growth rule below
	// also makes the first entry carry page 1.
	pages := e.dbPages
	for _, tx := range pl.Txs {
		if len(tx.Pgnos) != len(tx.Pages) || len(tx.Pgnos) == 0 || tx.DBSize == 0 {
			return fmt.Errorf("%w: malformed transaction", ErrRejected)
		}
		for i, pg := range tx.Pgnos {
			if pg == 0 || pg > tx.DBSize {
				return fmt.Errorf("%w: page %d beyond the database size %d", ErrRejected, pg, tx.DBSize)
			}
			if len(tx.Pages[i]) != int(pl.PageSize) {
				return fmt.Errorf("%w: page of %d bytes", ErrRejected, len(tx.Pages[i]))
			}
		}
		if tx.DBSize > pages {
			// Every page the database gains must be supplied once. Page numbers
			// repeat in real commits (a page written twice), so the count is of
			// DISTINCT new pages: repeating one must not inflate it.
			seen := make(map[uint32]struct{}, len(tx.Pgnos))
			for _, pg := range tx.Pgnos {
				if pg > pages {
					seen[pg] = struct{}{}
				}
			}
			have := uint64(len(seen))
			// SQLite never writes the lock-byte page.
			if lp := lockBytePage(pl.PageSize); lp > pages && lp <= tx.DBSize {
				if _, ok := seen[lp]; !ok {
					have++
				}
			}
			if _, ok := seen[1]; pages == 0 && !ok {
				return fmt.Errorf("%w: the first entry does not carry page 1", ErrRejected)
			}
			if have < uint64(tx.DBSize-pages) {
				return fmt.Errorf("%w: database grows from %d to %d pages with %d new pages supplied", ErrRejected, pages, tx.DBSize, have)
			}
		}
		pages = tx.DBSize
	}
	return nil
}

// lockBytePage is the number of the page that holds SQLite's lock byte (offset
// 0x40000000); SQLite never writes it, so a database that grows past it has one
// page no entry supplies. The page size has been validated by then.
func lockBytePage(pageSize uint32) uint32 {
	return uint32(0x40000000/uint64(pageSize)) + 1
}

// advanceLocked moves the replicated state past an applied entry.
func (e *Engine) advanceLocked(id [idSize]byte, pl *PageList) {
	e.seq++
	e.head = chainHash(e.head, id)
	e.pageSize = pl.PageSize
	e.dbPages = pl.Txs[len(pl.Txs)-1].DBSize
}

// Commit is called by the FSM for every replicated entry on every node.
// An entry that does not apply to the node's state is rejected (ErrRejected),
// on every node alike, and changes nothing. For the Leader's own entry it then
// only moves the committed end; for any other entry it appends the pages to
// this node's WAL. Any other error stops the engine.
func (e *Engine) Commit(id [idSize]byte, pl *PageList) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.stoppedLocked(); err != nil {
		return err
	}
	err := e.commitLocked(id, pl)
	if err != nil && !errors.Is(err, ErrRejected) {
		e.failLocked(err)
	}
	return err
}

func (e *Engine) commitLocked(id [idSize]byte, pl *PageList) error {
	own := e.pend != nil && e.pend.id == id
	if err := e.checkLocked(pl); err != nil {
		e.stats.Rejected++
		if own {
			if uerr := e.undoLocked(); uerr != nil {
				return errors.Join(err, uerr)
			}
		}
		return err
	}
	if own {
		e.tl = e.pend.tail
		e.pend = nil
		e.stats.OwnCommits++
		e.advanceLocked(id, pl)
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
	// (The page size needs no second check here: checkLocked fixed it.)
	// The connection's heap wal-index would not see the new frames.
	e.closeLocked()
	if err := e.appendLocked(pl); err != nil {
		return err
	}
	e.advanceLocked(id, pl)
	return e.maybeCheckpointLocked()
}

// appendLocked re-frames pl into this node's own WAL at the committed end,
// with this node's salts and checksums (dqlite's vfsWalAppend). The page images
// were checked end to end by the entry's checksum before they got here.
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
// first transaction wrote it is enough, the WAL overlays it anyway. checkLocked
// guarantees the first entry carries page 1.
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
	if err := e.stoppedLocked(); err != nil {
		return err
	}
	if e.pend != nil {
		return errors.New("walframe: request pending")
	}
	return e.checkpointLocked()
}

// Query runs read-only statements on this node's committed state. The caller
// serialises it against Leader requests (see Node). The connection is read-write
// (it is the one connection of the node), so read-only is enforced by the guard:
// SQLite refuses, at prepare time, every statement that is not a read, whatever
// its spelling. A read holds the engine for at most the query timeout, so the
// FSM waits behind it for no longer than that.
func (e *Engine) Query(ctx context.Context, req *command.Request, xTime bool) ([]*command.QueryRows, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.stoppedLocked(); err != nil {
		return nil, err
	}
	if err := e.openLocked(); err != nil {
		return nil, err
	}
	conn, err := e.sc.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if e.queryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, e.queryTimeout)
		defer cancel()
	}
	e.guard.SetReadOnly(true)
	rows, qerr := conn.Query(ctx, req, xTime)
	e.guard.SetReadOnly(false)
	if in, ierr := conn.InTransaction(); ierr == nil && in {
		conn.Request(context.Background(), &command.Request{Statements: []*command.Statement{{Sql: "ROLLBACK"}}}, false)
	}
	return rows, qerr
}

// PageImage returns the database as SQLite sees its committed pages: the
// database file overlaid with the committed frames of the WAL, truncated to
// the size of the last commit. It is computed from the files, independent of
// any SQLite connection, so equal images mean equal pages.
func (e *Engine) PageImage() ([]byte, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
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
	end := e.tl.end
	if !e.tl.valid {
		end = s.CommitEnd // the tail is unknown: the WAL's last commit is it
	}
	ps := int64(s.Header.PageSize)
	size := int64(len(d))
	for _, f := range s.Valid {
		if f.Offset >= end {
			break
		}
		if f.Commit != 0 {
			size = int64(f.Commit) * ps
		}
	}
	if size > int64(len(d))+int64(len(w)) {
		return nil, fmt.Errorf("walframe: the WAL claims a database of %d bytes, the files hold %d", size, len(d)+len(w))
	}
	img := make([]byte, size)
	copy(img, d)
	for _, f := range s.Valid {
		if f.Offset >= end {
			break
		}
		o := (int64(f.Pgno) - 1) * ps
		if o+ps <= size {
			copy(img[o:o+ps], w[f.Offset+frameHeaderSize:f.Offset+frameHeaderSize+ps])
		}
	}
	return img, nil
}

// snapshotBytes captures the committed state: the replicated state (sequence,
// hash chain, page size, size), the database file and the committed prefix of
// the WAL, with a SHA-256 over all of it. Local uncommitted frames are
// excluded.
//
//	"WFS1" | seq u64 | head [32] | pageSize u32 | dbPages u32 | len(db) u64 | db | wal | sha256(all before)
func (e *Engine) snapshotBytes() ([]byte, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.stoppedLocked(); err != nil {
		return nil, err
	}
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
	b := make([]byte, 0, snapshotHeaderSize+len(d)+len(w)+HashSize)
	b = append(b, snapshotMagic...)
	b = binary.BigEndian.AppendUint64(b, e.seq)
	b = append(b, e.head[:]...)
	b = binary.BigEndian.AppendUint32(b, e.pageSize)
	b = binary.BigEndian.AppendUint32(b, e.dbPages)
	b = binary.BigEndian.AppendUint64(b, uint64(len(d)))
	b = append(b, d...)
	b = append(b, w...)
	sum := sha256.Sum256(b)
	return append(b, sum[:]...), nil
}

// snapshotState is a decoded, checked snapshot.
type snapshotState struct {
	seq      uint64
	head     [HashSize]byte
	pageSize uint32
	dbPages  uint32
	d, w     []byte
}

// parseSnapshot checks a snapshot's checksum, header and the files' headers
// against each other, without touching any engine.
func parseSnapshot(b []byte) (*snapshotState, error) {
	bad := func(why string, a ...any) (*snapshotState, error) {
		return nil, fmt.Errorf("walframe: bad snapshot: "+why, a...)
	}
	if len(b) < snapshotHeaderSize+HashSize || string(b[:4]) != snapshotMagic {
		return bad("header")
	}
	body, trailer := b[:len(b)-HashSize], b[len(b)-HashSize:]
	if sum := sha256.Sum256(body); !bytes.Equal(sum[:], trailer) {
		return bad("checksum mismatch")
	}
	be := binary.BigEndian
	s := &snapshotState{seq: be.Uint64(body[4:]), pageSize: be.Uint32(body[4+8+HashSize:]), dbPages: be.Uint32(body[4+8+HashSize+4:])}
	copy(s.head[:], body[12:])
	n := be.Uint64(body[snapshotHeaderSize-8:])
	rest := body[snapshotHeaderSize:]
	if n > uint64(len(rest)) {
		return bad("database part longer than the snapshot")
	}
	s.d, s.w = rest[:n], rest[n:]
	if s.seq == 0 {
		if len(s.d) != 0 || len(s.w) != 0 || s.pageSize != 0 || s.dbPages != 0 {
			return bad("no page list applied, yet it holds data")
		}
		return s, nil
	}
	ps := int64(s.pageSize)
	if !validPageSize(s.pageSize) || s.dbPages == 0 {
		return bad("page size %d, %d pages", s.pageSize, s.dbPages)
	}
	// The database part must hold a database. SQLite deletes a WAL that sits
	// beside an empty database file, so a snapshot with an empty database part
	// and a WAL would install as an empty database and pass integrity_check.
	if len(s.d) < 100 || string(s.d[:16]) != sqliteHeaderMagic {
		return bad("the database is not a SQLite database (%d bytes)", len(s.d))
	}
	dps := int64(be.Uint16(s.d[16:]))
	if dps == 1 {
		dps = 65536
	}
	if dps != ps || int64(len(s.d))%ps != 0 {
		return bad("the database file does not match the page size %d", ps)
	}
	// The size the database has after the last committed transaction: the last
	// commit frame of the WAL if it has one, the file's size otherwise.
	want := uint32(int64(len(s.d)) / ps)
	if len(s.w) > 0 {
		sc, err := scanWAL(s.w)
		if err != nil {
			return bad("WAL: %v", err)
		}
		if int64(sc.Header.PageSize) != ps || sc.CommitEnd != int64(len(s.w)) {
			return bad("the WAL does not match the page size or is not whole")
		}
		if n := len(sc.Valid); n > 0 {
			want = sc.Valid[n-1].Commit
		}
	}
	// The header fields are installed as they stand and every later page list is
	// checked against them: they must agree with the files they describe.
	if s.dbPages != want {
		return bad("%d pages declared, the files hold %d", s.dbPages, want)
	}
	if s.head == ([HashSize]byte{}) {
		return bad("no head hash for a database at sequence %d", s.seq)
	}
	return s, nil
}

// verifySnapshot opens the snapshot's files in a scratch engine and runs
// PRAGMA integrity_check, so that nothing is installed that SQLite cannot read.
func (e *Engine) verifySnapshot(s *snapshotState) error {
	dir, err := os.MkdirTemp(e.dir, ".verify-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	se, err := newEngine(dir, 0, nil, e.ops)
	if err != nil {
		return err
	}
	defer se.Close()
	if err := se.ops.WriteFile(se.dbPath, s.d); err != nil {
		return err
	}
	if len(s.w) > 0 {
		if err := se.ops.WriteFile(se.walPath, s.w); err != nil {
			return err
		}
	}
	rows, err := se.Query(context.Background(), &command.Request{Statements: []*command.Statement{{Sql: "PRAGMA integrity_check"}}}, false)
	if err != nil || len(rows) != 1 || rows[0].Error != "" || len(rows[0].Values) != 1 || rows[0].Values[0].Parameters[0].GetS() != "ok" {
		return fmt.Errorf("integrity_check: %v (%v)", rows, err)
	}
	return nil
}

// restoreBytes replaces this node's database with a snapshot, which is checked
// (checksum, headers, integrity_check on a scratch copy) BEFORE anything of this
// node is touched. A restore also clears a stop.
func (e *Engine) restoreBytes(b []byte) error {
	s, err := parseSnapshot(b)
	if err != nil {
		return err
	}
	if len(s.d) > 0 || len(s.w) > 0 {
		if err := e.verifySnapshot(s); err != nil {
			return fmt.Errorf("walframe: snapshot does not verify: %w", err)
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closeLocked()
	e.pend = nil
	e.tl = tail{}
	e.fatal = nil
	if err := e.ops.WriteFile(e.dbPath, s.d); err != nil {
		e.failLocked(err)
		return err
	}
	if len(s.w) == 0 {
		if err := e.ops.Remove(e.walPath); err != nil {
			e.failLocked(err)
			return err
		}
	} else if err := e.ops.WriteFile(e.walPath, s.w); err != nil {
		e.failLocked(err)
		return err
	}
	e.seq, e.head, e.pageSize, e.dbPages = s.seq, s.head, s.pageSize, s.dbPages
	if err := e.ensureTailLocked(); err != nil {
		e.failLocked(err)
		return err
	}
	return nil
}
