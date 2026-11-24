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

	fatalMu sync.Mutex
	fatal   error

	maxSnapshot int64 // largest snapshot Restore reads; for tests, a variable
}

// NewNode wraps an Engine.
func NewNode(e *Engine, logger *log.Logger) *Node {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	return &Node{eng: e, log: logger, applyTimeout: 10 * time.Second, maxSnapshot: defaultMaxSnapshot}
}

// Engine returns the node's engine.
func (n *Node) Engine() *Engine { return n.eng }

// SetRaft gives the node the Raft instance it replicates through.
func (n *Node) SetRaft(r *raft.Raft) { n.raft = r }

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
	// runs anything against its database.
	if n.behindFn() {
		if err := n.barrierFn(); err != nil {
			return nil, fmt.Errorf("walframe: barrier: %w", err)
		}
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

// behind reports whether the log holds entries this node has not applied.
func behind(applied, last uint64) bool { return applied < last }

func (n *Node) behindFn() bool {
	if n.behind != nil {
		return n.behind()
	}
	return behind(n.raft.AppliedIndex(), n.raft.LastIndex())
}

func (n *Node) barrierFn() error {
	if n.barrier != nil {
		return n.barrier()
	}
	return n.raft.Barrier(n.applyTimeout).Error()
}

// Query runs read-only statements on this node's committed state.
func (n *Node) Query(ctx context.Context, req *command.Request) ([]*command.QueryRows, error) {
	n.reqMu.Lock()
	defer n.reqMu.Unlock()
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
		n.fatal = errors.Join(n.fatal, err)
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
