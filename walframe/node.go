package walframe

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
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
	// barrierTerm is the Raft term in which this node last ran its barrier
	// (under reqMu); terms start at 1.
	barrierTerm uint64

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
		if err = r.LeadershipTransfer().Error(); err == nil {
			return
		}
		time.Sleep(stepDownPause)
	}
	n.log.Printf("walframe: stopped, and cannot give up leadership: %v", err)
}

// A failed transfer (no peer ready, an election in progress) is retried.
const (
	stepDownTries = 5
	stepDownPause = 100 * time.Millisecond
)

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
	go func() {
		defer r.DeregisterObserver(obs)
		for o := range ch {
			st := o.Data.(raft.RaftState)
			if st == raft.Shutdown {
				return
			}
			if st == raft.Leader && n.eng.Fatal() != nil {
				n.stepDown()
			}
		}
	}()
}

// ensureBarrier runs this term's barrier if it has not run, under reqMu. The
// context bounds the wait.
func (n *Node) ensureBarrier(ctx context.Context) error {
	term := n.raft.CurrentTerm()
	if !n.needBarrier(term) {
		return nil
	}
	if err := n.barrierFn(ctx); err != nil {
		return fmt.Errorf("walframe: barrier: %w", err)
	}
	n.barrierTerm = term
	return nil
}

func (n *Node) barrierFn(ctx context.Context) error {
	if n.barrier != nil {
		return n.barrier()
	}
	f := n.raft.Barrier(n.applyTimeout)
	done := make(chan error, 1)
	go func() { done <- f.Error() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Query runs read-only statements on this node's committed state. On the Leader
// that state is first brought up to date, as for Execute: a new Leader's FSM may
// not have applied the log yet.
func (n *Node) Query(ctx context.Context, req *command.Request) ([]*command.QueryRows, error) {
	n.reqMu.Lock()
	defer n.reqMu.Unlock()
	if n.raft != nil && n.raft.State() == raft.Leader {
		if err := n.ensureBarrier(ctx); err != nil {
			return nil, err
		}
	}
	return n.eng.Query(ctx, req, false)
}

// Apply implements raft.FSM.
func (n *Node) Apply(l *raft.Log) any {
	if l.Type != raft.LogCommand {
		return nil
	}
	id, pl, err := unmarshalCommand(l.Data)
	if err == nil {
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
