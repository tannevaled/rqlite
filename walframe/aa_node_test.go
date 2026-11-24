package walframe

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/v10/db"
)

type fakeSink struct {
	bytes.Buffer
	failWrite, failClose bool
	canceled, closed     bool
}

func (s *fakeSink) Write(p []byte) (int, error) {
	if s.failWrite {
		return 0, errInjected
	}
	return s.Buffer.Write(p)
}
func (s *fakeSink) Close() error {
	s.closed = true
	if s.failClose {
		return errInjected
	}
	return nil
}
func (s *fakeSink) ID() string    { return "fake" }
func (s *fakeSink) Cancel() error { s.canceled = true; return nil }

func TestNeedBarrier(t *testing.T) {
	defer nopanic(t)
	n := &Node{barrierTerm: 3}
	for _, c := range []struct {
		term uint64
		want bool
	}{{2, true}, {3, false}, {4, true}} {
		if got := n.needBarrier(c.term); got != c.want {
			t.Errorf("needBarrier(%d) = %v", c.term, got)
		}
	}
}

func TestNodeExecuteOnAFollower(t *testing.T) {
	defer nopanic(t)
	c := newCluster(t, 3, 1<<30)
	l := c.leader()
	for _, tn := range c.nodes {
		if tn == l {
			continue
		}
		if _, err := tn.n.Execute(bg, req("CREATE TABLE t(x)")); !errors.Is(err, ErrNotLeader) {
			t.Errorf("follower Execute: %v", err)
		}
	}
	if l.n.Engine() == nil {
		t.Error("Engine()")
	}
}

func TestNodeBarrier(t *testing.T) {
	defer nopanic(t)
	c := newCluster(t, 1, 1<<30)
	n := c.leader().n
	calls := 0
	n.barrier = func() error { calls++; return errInjected }

	// Not behind: the barrier is not asked for.
	n.behind = func() bool { return false }
	c.mustExec(req("CREATE TABLE t(x)"))
	if calls != 0 {
		t.Fatalf("barrier called %d times with nothing to apply", calls)
	}
	// Behind: the barrier runs and its failure refuses the request, which
	// changes nothing.
	n.behind = func() bool { return true }
	_, err := n.Execute(bg, req("INSERT INTO t VALUES(1)"))
	if !errors.Is(err, errInjected) || !strings.Contains(err.Error(), "barrier") || calls != 1 {
		t.Fatalf("Execute behind: %v, %d barrier calls", err, calls)
	}
	if n.eng.Stats().OwnCommits != 1 || n.eng.pend != nil {
		t.Fatalf("a refused request ran: %+v", n.eng.Stats())
	}
	// Behind, with a barrier that succeeds.
	n.barrier = func() error { calls++; return nil }
	if _, err := n.Execute(bg, req("INSERT INTO t VALUES(1)")); err != nil || calls != 2 {
		t.Fatalf("Execute after the barrier: %v, %d calls", err, calls)
	}
	// The real ones.
	n.behind, n.barrier = nil, nil
	if _, err := n.Execute(bg, req("INSERT INTO t VALUES(2)")); err != nil {
		t.Fatal(err)
	}
	if n.needBarrier(n.raft.CurrentTerm()) {
		t.Error("a quiet leader is behind")
	}
	if err := n.barrierFn(); err != nil {
		t.Errorf("real barrier: %v", err)
	}
}

func TestNodeExecuteResults(t *testing.T) {
	defer nopanic(t)
	c := newCluster(t, 3, 1<<30)
	l := c.leader().n
	c.mustExec(req("CREATE TABLE t(x)"))

	// A statement error replicates nothing.
	before := l.eng.Stats().OwnCommits
	res, err := l.Execute(bg, req("INSERT INTO nosuch VALUES(1)"))
	if err != nil || len(res) != 1 || res[0].GetE().Error == "" || l.eng.Stats().OwnCommits != before {
		t.Fatalf("failing statement: %v, %v", res, err)
	}
	// A request that leaves a transaction open: the committed part replicates,
	// the error is reported.
	res, err = l.Execute(bg, req("INSERT INTO t VALUES(7)", "BEGIN"))
	if !errors.Is(err, ErrTxAcrossRequests) || res == nil || l.eng.Stats().OwnCommits != before+1 {
		t.Fatalf("open transaction: %v, %v", res, err)
	}
	c.converge()
	c.assertIdentical("tx across requests", "SELECT x FROM t")
}

func TestNodeApplyRefusals(t *testing.T) {
	defer nopanic(t)
	c := newCluster(t, 1, 1<<30)
	n := c.leader().n
	if got := n.Apply(&raft.Log{Type: raft.LogNoop}); got != nil || n.Fatal() != nil {
		t.Fatalf("non-command entry: %v, %v", got, n.Fatal())
	}
	c.mustExec(req("CREATE TABLE t(x)")) // the WAL now has a page size
	if got := n.Apply(&raft.Log{Type: raft.LogCommand, Index: 3, Data: marshalCommand([idSize]byte{1}, based(n.eng, &PageList{PageSize: 512,
		Txs: []Tx{{Pgnos: []uint32{1}, Pages: [][]byte{filled(512, 1)}, DBSize: 1}}}))}); !errors.Is(asErr(got), ErrRejected) || n.Fatal() != nil {
		t.Fatalf("an entry of another page size: %v, fatal %v", got, n.Fatal())
	}
	// A rejected entry is not fatal: the node still serves.
	c.mustExec(req("INSERT INTO t VALUES(1)"))
	// A garbage entry cannot be vouched for: the node stops.
	if got := n.Apply(&raft.Log{Type: raft.LogCommand, Data: []byte("garbage")}); got == nil || n.Fatal() == nil {
		t.Fatalf("garbage entry: %v, %v", got, n.Fatal())
	}
	if _, err := n.Query(bg, req("SELECT 1")); err == nil {
		t.Fatal("a stopped node served a read")
	}
}

func TestNodeCheckpointFailureIsFatal(t *testing.T) {
	defer nopanic(t)
	c := newCluster(t, 1, 1)
	n := c.leader().n
	n.eng.ckpt = func(*db.Conn) (*db.CheckpointMeta, error) { return nil, errInjected }
	_, err := n.Execute(bg, req("CREATE TABLE t(x)"))
	if !errors.Is(err, errInjected) {
		t.Fatalf("Execute: %v", err)
	}
	if !errors.Is(n.Fatal(), errInjected) {
		t.Fatalf("Fatal: %v", n.Fatal())
	}
}

func TestNodeRaftRefusesTheEntry(t *testing.T) {
	defer nopanic(t)
	for _, undoFails := range []bool{false, true} {
		c := newCluster(t, 1, 1<<30)
		l := c.leader()
		c.mustExec(req("CREATE TABLE t(x)"))
		l.n.afterLocalExec = func() {
			l.r.Shutdown().Error()
			if undoFails {
				l.n.eng.mu.Lock()
				l.n.eng.ops = &faultOps{failAt: 1}
				l.n.eng.mu.Unlock()
			}
		}
		_, err := l.n.Execute(bg, req("INSERT INTO t VALUES(1)"))
		if err == nil {
			t.Fatal("Execute succeeded without Raft")
		}
		if got := strings.Contains(err.Error(), "undo failed"); got != undoFails {
			t.Errorf("undoFails=%v: %v", undoFails, err)
		}
		if !undoFails && !strings.Contains(err.Error(), "local change undone") {
			t.Errorf("error: %v", err)
		}
		if undoFails && !errors.Is(err, raft.ErrRaftShutdown) {
			t.Errorf("the Raft error is lost: %v", err)
		}
		if l.n.eng.pend != nil {
			t.Error("a request is still pending")
		}
	}
}

func TestNodeSnapshotRestorePersist(t *testing.T) {
	defer nopanic(t)
	c := newCluster(t, 1, 1<<30)
	n := c.leader().n
	c.mustExec(req("CREATE TABLE t(x)", "INSERT INTO t VALUES(1)"))
	fs, err := n.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Release()
	sink := &fakeSink{}
	if err := fs.Persist(sink); err != nil || !sink.closed || sink.canceled || sink.Len() < 8 {
		t.Fatalf("Persist: %v closed=%v canceled=%v len=%d", err, sink.closed, sink.canceled, sink.Len())
	}
	img := image(t, n.eng)
	if err := n.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(img, image(t, n.eng)) {
		t.Error("restore changed the image")
	}

	bad := &fakeSink{failWrite: true}
	if err := fs.Persist(bad); !errors.Is(err, errInjected) || !bad.canceled || bad.closed {
		t.Errorf("failing write: %v canceled=%v closed=%v", err, bad.canceled, bad.closed)
	}
	bad = &fakeSink{failClose: true}
	if err := fs.Persist(bad); !errors.Is(err, errInjected) || bad.canceled {
		t.Errorf("failing close: %v canceled=%v", err, bad.canceled)
	}

	closed := false
	rc := struct {
		io.Reader
		io.Closer
	}{iotest.ErrReader(errInjected), closerFunc(func() error { closed = true; return nil })}
	if err := n.Restore(rc); !errors.Is(err, errInjected) || !closed {
		t.Errorf("failing reader: %v closed=%v", err, closed)
	}
	if err := n.Restore(io.NopCloser(strings.NewReader("short"))); err == nil {
		t.Error("a short snapshot was restored")
	}
}

type closerFunc func() error

func (f closerFunc) Close() error { return f() }

func TestNodeSnapshotOfACorruptWAL(t *testing.T) {
	defer nopanic(t)
	c := newCluster(t, 1, 1<<30)
	n := c.leader().n
	os.WriteFile(n.eng.walPath, corruptWAL(4096), 0o644)
	if _, err := n.Snapshot(); err == nil {
		t.Error("Snapshot of a corrupt WAL")
	}
}
