package walframe

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	command "github.com/rqlite/rqlite/v10/command/proto"
)

// ErrNotLeader is returned by Execute on a node that is not the Leader.
var ErrNotLeader = errors.New("walframe: not the leader")

// Node is one cluster member: a raft.FSM over an Engine, and the Leader-side
// request path. Create the Raft instance with the Node as its FSM, then call
// SetRaft.
type Node struct {
	eng *Engine
	log *log.Logger

	// reqMu serialises Leader requests and every read, and is held from the
	// moment a request runs locally until Raft has committed or refused its
	// entry. So no read can observe frames Raft has not committed.
	reqMu sync.Mutex

	raft         *raft.Raft
	applyTimeout time.Duration

	// afterLocalExec, if set, is called on the Leader between local execution
	// and the Raft proposal. For tests of lost leadership.
	afterLocalExec func()
	// behind and barrier, if set, replace the Leader's check that it has applied
	// its whole log and the barrier that makes it so. For tests: a real lag is
	// not reproducible on demand.
	behind  func() bool
	barrier func() error
	// bmu guards barrierTerm and pending: a barrier runs in a goroutine of its own,
	// which outlives a request that gave up waiting for it.
	bmu sync.Mutex
	// barrierTerm is the Raft term in which this node last ran its barrier to
	// completion; terms start at 1.
	barrierTerm uint64
	// pending is the barrier in flight, if any: every request of the term waits
	// for that one instead of appending a Barrier entry of its own.
	pending *barrierRun
	// verify, if set, replaces raft's VerifyLeader. For tests.
	verify func() error
	// transfer, if set, replaces the leadership transfer; watchDone is closed when
	// the goroutine watching Raft's state returns (at Raft's shutdown). For tests.
	// backoff, if non-zero, replaces stepDownBackoff; backoffMax replaces
	// stepDownBackoffMax.
	transfer   func() error
	watchDone  chan struct{}
	backoff    time.Duration
	backoffMax time.Duration
	// transfers counts the leadership transfers a stopped node has made since it
	// last was rebuilt.
	transfers atomic.Int32

	fatalMu sync.Mutex
	fatal   error

	maxSnapshot int64 // largest snapshot Restore reads; for tests, a variable
}

// NewNode wraps an Engine.
func NewNode(e *Engine, logger *log.Logger) *Node {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	n := &Node{eng: e, log: logger, applyTimeout: 10 * time.Second, maxSnapshot: defaultMaxSnapshot}
	e.mu.Lock()
	e.onStop = n.stepDown
	e.mu.Unlock()
	return n
}

// Engine returns the node's engine.
func (n *Node) Engine() *Engine { return n.eng }

// SetRaft gives the node the Raft instance it replicates through.
func (n *Node) SetRaft(r *raft.Raft) {
	n.raft = r
	n.watchLeadership(r)
}

// Fatal returns the error that made this node refuse an entry, if any. A node
// that cannot apply a committed entry has diverged and must be rebuilt from a
// snapshot (it is not applied half-way: the engine reports before touching).
func (n *Node) Fatal() error {
	n.fatalMu.Lock()
	defer n.fatalMu.Unlock()
	return n.fatal
}

// Execute runs req on the Leader and replicates the pages it changed.
// A request that changes nothing is not replicated. If Raft refuses the entry
// (lost leadership, timeout) the local change is undone and the error returned;
// as in any Raft system the entry may nevertheless commit later, through a new
// Leader, in which case this node applies it like a follower.
func (n *Node) Execute(ctx context.Context, req *command.Request) ([]*command.ExecuteQueryResponse, error) {
	n.reqMu.Lock()
	defer n.reqMu.Unlock()
	if n.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}
	// A new Leader must have applied every entry of previous terms before it
	// runs anything against its database. Raft's AppliedIndex cannot say so: it
	// moves when a batch is queued to the FSM goroutine, not when the FSM has
	// applied it (hashicorp/raft v1.8.0 raft.go:1362). A Barrier, whose future
	// answers after the FSM has applied everything before it, can, and within one
	// term every later entry is this node's own, applied before its request
	// returns. So: once per term.
	if err := n.ensureBarrier(ctx); err != nil {
		return nil, err
	}
	res, pl, id, err := n.eng.Run(ctx, req, false)
	if pl == nil {
		// Nothing to replicate: the request may still have READ, and what it read
		// is only as good as this node's leadership. Prove it, as a linearizable
		// Query does, before the client sees it.
		if err == nil && len(res) > 0 {
			if verr := n.verifyLeader(ctx); verr != nil {
				return nil, verr
			}
		}
		return res, err
	}
	if n.afterLocalExec != nil {
		n.afterLocalExec()
	}
	f := n.raft.Apply(marshalCommand(id, pl), n.applyTimeout)
	if ferr := f.Error(); ferr != nil {
		if uerr := n.eng.Undo(id); uerr != nil {
			return nil, fmt.Errorf("walframe: %w (and undo failed: %v)", ferr, uerr)
		}
		return nil, fmt.Errorf("walframe: entry not committed, local change undone: %w", ferr)
	}
	if rerr, ok := f.Response().(error); ok && rerr != nil {
		if errors.Is(rerr, ErrRejected) {
			// Nothing was applied anywhere: the client must not see the local result.
			return nil, rerr
		}
		return res, rerr
	}
	return res, err
}

// needBarrier reports whether this Leader has yet to run its barrier in term.
func (n *Node) needBarrier(term uint64) bool {
	if n.behind != nil {
		return n.behind()
	}
	n.bmu.Lock()
	defer n.bmu.Unlock()
	return term != n.barrierTerm
}

// stepDown gives up Raft leadership: a Leader whose engine stopped would fail
// every write and keep the cluster from electing a healthy one. It cannot help
// a single-node cluster. A failed transfer is retried, and watchLeadership calls
// it again each time the node is elected while stopped.
func (n *Node) stepDown() {
	r := n.raft
	var err error
	for i := 0; i < stepDownTries; i++ {
		if r == nil || r.State() != raft.Leader {
			return
		}
		if err = n.transferFn(r); err == nil {
			n.transfers.Add(1)
			return
		}
		time.Sleep(stepDownPause)
	}
	n.log.Printf("walframe: stopped, and cannot give up leadership: %v", err)
}

// transferFn hands the leadership over; a test replaces it to count the calls.
func (n *Node) transferFn(r *raft.Raft) error {
	if n.transfer != nil {
		return n.transfer()
	}
	return r.LeadershipTransfer().Error()
}

// A failed transfer (no peer ready, an election in progress) is retried.
//
// The transfers themselves are bounded, because a node cannot tell a healthy
// peer from a stopped one: when every node is stopped, each hands the
// leadership to another that does the same, and every handover costs a term, a
// write to the stable store and a log entry on every node. After a transfer
// the next one waits stepDownBackoff, then twice that, and so on; and a node
// gives up after stepDownBudget successful transfers since it was last rebuilt
// (Restore): it stays Leader, refusing everything (a stopped engine serves
// nothing), and says so in the log. That is a cluster waiting for an operator,
// not one that churns.
const (
	stepDownTries      = 5
	stepDownPause      = 100 * time.Millisecond
	stepDownBudget     = 3
	stepDownBackoff    = time.Second
	stepDownBackoffMax = 30 * time.Second
)

// backoffBefore is the wait before the next transfer, given that done have
// already been made: none for the first, then doubling from the base.
func (n *Node) backoffBefore(done int32) time.Duration {
	if done <= 0 {
		return 0
	}
	base, max := n.backoff, n.backoffMax
	if base == 0 {
		base = stepDownBackoff
	}
	if max == 0 {
		max = stepDownBackoffMax
	}
	d := base
	for i := int32(1); i < done && d < max; i++ {
		d *= 2
	}
	return min(d, max)
}

// pauseWhileLeader waits d, and returns false as soon as r is no longer Leader.
func pauseWhileLeader(r *raft.Raft, d time.Duration) bool {
	for end := time.Now().Add(d); ; {
		if r.State() != raft.Leader {
			return false
		}
		left := time.Until(end)
		if left <= 0 {
			return true
		}
		time.Sleep(min(left, 20*time.Millisecond))
	}
}

// watchLeadership steps down every time this node becomes Leader while its engine
// is stopped: the stop hook fires once, but a stopped node is elected again
// whenever its log is as current as the others'.
func (n *Node) watchLeadership(r *raft.Raft) {
	ch := make(chan raft.Observation, 16)
	obs := raft.NewObserver(ch, false, func(o *raft.Observation) bool {
		_, ok := o.Data.(raft.RaftState)
		return ok
	})
	r.RegisterObserver(obs)
	done := make(chan struct{})
	n.watchDone = done
	go func() {
		defer close(done)
		defer r.DeregisterObserver(obs)
		for o := range ch {
			st := o.Data.(raft.RaftState)
			if st == raft.Shutdown {
				return
			}
			if st == raft.Leader && n.eng.Fatal() != nil {
				done := n.transfers.Load()
				if done >= stepDownBudget {
					n.log.Printf("walframe: stopped, Leader again, and out of leadership transfers (%d): waiting for a rebuild", done)
					continue
				}
				if pauseWhileLeader(r, n.backoffBefore(done)) {
					n.stepDown()
				}
			}
		}
	}()
}

// ensureBarrier runs this term's barrier if it has not run, under reqMu. The
// context bounds the wait.
func (n *Node) ensureBarrier(ctx context.Context) error {
	if !n.needBarrier(n.raft.CurrentTerm()) {
		return nil
	}
	if err := n.barrierFn(ctx); err != nil {
		return fmt.Errorf("walframe: barrier: %w", err)
	}
	return nil
}

// barrierRun is one barrier in flight.
type barrierRun struct {
	term uint64
	done chan struct{}
	err  error // read after done is closed
}

// barrierFn waits for this term's barrier, starting it if none is in flight.
// The barrier runs in a goroutine of its own and is SHARED: a request whose
// context ends first leaves it running, and the next request joins it instead
// of appending another Barrier entry to the log. At most one is in flight, and
// a failed one is forgotten so that the next request tries again.
func (n *Node) barrierFn(ctx context.Context) error {
	term := n.raft.CurrentTerm()
	n.bmu.Lock()
	b := n.pending
	if b == nil || b.term != term {
		b = &barrierRun{term: term, done: make(chan struct{})}
		n.pending = b
		go n.runBarrier(b)
	}
	n.bmu.Unlock()
	select {
	case <-b.done:
		return b.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// runBarrier runs b to its end and records the outcome, whoever is waiting.
func (n *Node) runBarrier(b *barrierRun) {
	var err error
	if n.barrier != nil {
		err = n.barrier()
	} else {
		err = n.raft.Barrier(n.applyTimeout).Error()
	}
	n.bmu.Lock()
	b.err = err
	if err == nil && b.term > n.barrierTerm {
		n.barrierTerm = b.term
	}
	if n.pending == b {
		n.pending = nil
	}
	n.bmu.Unlock()
	close(b.done)
}

// verifyLeader proves, by a round of heartbeats, that this node is still the
// Leader of a quorum: what it read before this call cannot have been behind a
// newer commit. The wait is bounded by ctx.
func (n *Node) verifyLeader(ctx context.Context) error {
	if n.raft == nil {
		return ErrNotLeader
	}
	f := n.verify
	if f == nil {
		f = func() error { return n.raft.VerifyLeader().Error() }
	}
	done := make(chan error, 1)
	go func() { done <- f() }()
	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("%w: %v", ErrNotLeader, err)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Consistency is the guarantee a read asks for, after rqlite's read levels.
type Consistency int

const (
	// Linearizable, the zero value and so the default: the read is served by the
	// Leader, after its barrier of the term and a quorum check (VerifyLeader). It
	// reflects every write acknowledged before it started. On any other node it
	// is refused with ErrNotLeader.
	Linearizable Consistency = iota
	// Weak: served by the Leader after its barrier, with no quorum check. A
	// Leader that has lost its quorum steps down by itself after
	// LeaderLeaseTimeout, so a Weak read can be stale for that long and no
	// longer. On any other node it is refused with ErrNotLeader.
	Weak
	// None: served from this node's local state, whatever its role. It can be
	// arbitrarily stale (a follower is behind by its replication lag, a cut-off
	// node by the length of the cut). The caller must opt in to that.
	None
)

// Query runs read-only statements with Linearizable consistency. See QueryLevel.
func (n *Node) Query(ctx context.Context, req *command.Request) ([]*command.QueryRows, error) {
	return n.QueryLevel(ctx, req, Linearizable)
}

// QueryLevel runs read-only statements on this node's committed state at the
// given consistency level. On the Leader (Weak and Linearizable) the state is
// first brought up to date, as for Execute: a new Leader's FSM may not have
// applied the log yet.
func (n *Node) QueryLevel(ctx context.Context, req *command.Request, level Consistency) ([]*command.QueryRows, error) {
	n.reqMu.Lock()
	defer n.reqMu.Unlock()
	switch level {
	case None:
	case Weak, Linearizable:
		if n.raft == nil || n.raft.State() != raft.Leader {
			return nil, ErrNotLeader
		}
		if err := n.ensureBarrier(ctx); err != nil {
			return nil, err
		}
		if level == Linearizable {
			if err := n.verifyLeader(ctx); err != nil {
				return nil, err
			}
		}
	default:
		return nil, fmt.Errorf("walframe: unknown consistency level %d", int(level))
	}
	return n.eng.Query(ctx, req, false)
}

// Apply implements raft.FSM.
func (n *Node) Apply(l *raft.Log) any {
	if l.Type != raft.LogCommand {
		return nil
	}
	id, pl, err := unmarshalCommand(l.Data)
	if err != nil {
		// The bytes decode the same way on every node, and nothing has been
		// touched: that is a refusal, like a stale base, not a node that has lost
		// track of its state. Stopping here would stop every node at once.
		err = fmt.Errorf("%w: %v", ErrRejected, err)
	} else {
		err = n.eng.Commit(id, pl)
	}
	if err != nil {
		if errors.Is(err, ErrRejected) {
			// Refused by every node alike, nothing changed: the node carries on and
			// the Leader that proposed it tells its client.
			n.log.Printf("walframe: entry %d rejected: %v", l.Index, err)
			return err
		}
		// Anything else: this node cannot vouch for its state. It stops (no
		// further apply, no read) until a Restore rebuilds it.
		n.log.Printf("walframe: cannot apply entry %d: %v", l.Index, err)
		n.fatalMu.Lock()
		if n.fatal == nil { // the first error is the cause; keeping all would grow without bound
			n.fatal = err
		}
		n.fatalMu.Unlock()
		n.eng.Fail(err)
		return err
	}
	return nil
}

// Snapshot implements raft.FSM: the committed database file and WAL prefix,
// copied now so that Persist can run while the node keeps applying.
func (n *Node) Snapshot() (raft.FSMSnapshot, error) {
	b, err := n.eng.snapshotBytes()
	if err != nil {
		return nil, err
	}
	return &snap{b: b}, nil
}

// Restore implements raft.FSM.
func (n *Node) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	b, err := io.ReadAll(io.LimitReader(rc, n.maxSnapshot+1))
	if err != nil {
		return err
	}
	if int64(len(b)) > n.maxSnapshot {
		return fmt.Errorf("walframe: snapshot larger than %d bytes", n.maxSnapshot)
	}
	err = n.eng.restoreBytes(b)
	if err == nil {
		n.fatalMu.Lock()
		n.fatal = nil
		n.fatalMu.Unlock()
		n.transfers.Store(0)
	}
	return err
}

type snap struct{ b []byte }

func (s *snap) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, bytes.NewReader(s.b)); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snap) Release() {}
