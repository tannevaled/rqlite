package walframe

// Follower-apply benchmarks: the statement path (the Store: every node replays
// the SQL) against walframe (every node appends the pages), on a 3-node
// in-process cluster. Run with GOMAXPROCS=1 (-cpu 1) so the followers' apply
// work competes with the Leader's for one core: ns/op is then the cluster's
// total cost per write, followers included, and allocs/op counts all three
// nodes. The timed loop ends only when every follower has applied the last
// entry.
//
// Caveat on the substrates: the Store runs hashicorp/raft on a BoltDB log and
// TCP on localhost; the walframe cluster runs the same raft library on an
// in-memory log and an in-memory transport (the harness the correctness tests
// use). The Store's figures therefore include log fsyncs and sockets that
// walframe's do not. Read the follower-read rows and allocs/op, not only ns/op.
//
// The variants:
//
//	statement          Store, a read on both followers after every write
//	statement-noread   Store, followers never read
//	walframe-reopen    walframe, a read on both followers after every write:
//	                   each apply closed the connection, so each read reopens
//	                   SQLite and recovers the WAL
//	walframe-noread    walframe, followers never read: no connection to reopen
//
// Keeping the connection open across applies is not a variant: see
// TestKeepingTheConnectionOpenServesStalePages.

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"testing"
	"time"

	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/store"
)

type benchLoad struct {
	name  string
	setup func() []*command.Request
	op    func(i int) *command.Request
}

func benchLoads() []benchLoad {
	table := "CREATE TABLE t(id INTEGER PRIMARY KEY, x INTEGER)"
	return []benchLoad{
		{
			name:  "Insert1",
			setup: func() []*command.Request { return []*command.Request{req(table)} },
			op: func(i int) *command.Request {
				return req(fmt.Sprintf("INSERT INTO t(x) VALUES(%d)", i))
			},
		},
		{
			name:  "Tx100",
			setup: func() []*command.Request { return []*command.Request{req(table)} },
			op: func(i int) *command.Request {
				r := &command.Request{Transaction: true}
				for j := 0; j < 100; j++ {
					r.Statements = append(r.Statements, &command.Statement{Sql: fmt.Sprintf("INSERT INTO t(x) VALUES(%d)", i*100+j)})
				}
				return r
			},
		},
		{
			// ~1 MB of rows, every page rewritten by each UPDATE.
			name: "Update1MB",
			setup: func() []*command.Request {
				fill := &command.Request{Transaction: true}
				for j := 0; j < 1000; j++ {
					fill.Statements = append(fill.Statements, &command.Statement{Sql: "INSERT INTO t(x, pad) VALUES(0, zeroblob(1000))"})
				}
				return []*command.Request{req("CREATE TABLE t(id INTEGER PRIMARY KEY, x INTEGER, pad BLOB)"), fill}
			},
			op: func(int) *command.Request { return req("UPDATE t SET x = x + 1") },
		},
	}
}

const benchRead = "SELECT count(*) FROM t"

// cpuTime is this process's user plus system CPU time. ns/op of the Store
// includes the Raft log's fsync waits, which are not CPU; cpu-ns/op is the
// work the cluster did per write, the figure that bounds throughput once the
// disk is out of the way.
// (cpuTime is in bench_cpu_unix_test.go; other systems report zero.)
func reportCPU(b *testing.B, start time.Duration) {
	b.ReportMetric(float64(cpuTime()-start)/float64(b.N), "cpu-ns/op")
}

func benchRequests(b *testing.B, w benchLoad) []*command.Request {
	reqs := make([]*command.Request, b.N)
	for i := range reqs {
		reqs[i] = w.op(i)
	}
	return reqs
}

func runWalframe(b *testing.B, w benchLoad, reads bool) {
	c := newCluster(b, 3, 0)
	l := c.leader()
	for _, r := range w.setup() {
		c.mustExec(r)
	}
	c.converge()
	var followers []*tnode
	for _, tn := range c.nodes {
		if tn != l {
			followers = append(followers, tn)
		}
	}
	reqs := benchRequests(b, w)
	rd := req(benchRead)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	cpu0 := cpuTime()
	for i := 0; i < b.N; i++ {
		if _, err := l.n.Execute(ctx, reqs[i]); err != nil {
			b.Fatal(err)
		}
		if reads {
			for _, f := range followers {
				if _, err := f.n.Query(ctx, rd); err != nil {
					b.Fatal(err)
				}
			}
		}
	}
	c.converge()
	b.StopTimer()
	reportCPU(b, cpu0)
	for _, tn := range c.nodes {
		if f := tn.n.Fatal(); f != nil {
			b.Fatalf("node %s diverged: %v", tn.id, f)
		}
	}
}

type benchLayer struct{ net.Listener }

func (benchLayer) Dial(addr string, d time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, d)
}

func newStoreCluster(b *testing.B) []*store.Store {
	var ss []*store.Store
	for i := 0; i < 3; i++ {
		ln, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			b.Fatal(err)
		}
		s := store.New(&store.Config{
			DBConf: store.NewDBConfig(),
			Dir:    b.TempDir(),
			ID:     fmt.Sprintf("s%d", i),
			Logger: log.New(io.Discard, "", 0),
		}, benchLayer{ln})
		if err := s.Open(); err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { s.Close(true); ln.Close() })
		if i == 0 {
			if err := s.Bootstrap(store.NewServer(s.ID(), s.Addr(), true)); err != nil {
				b.Fatal(err)
			}
		} else if err := ss[0].Join(&command.JoinRequest{Id: s.ID(), Address: s.Addr(), Voter: true}); err != nil {
			b.Fatal(err)
		}
		if _, err := s.WaitForLeader(10 * time.Second); err != nil {
			b.Fatal(err)
		}
		ss = append(ss, s)
	}
	return ss
}

func storeExec(b *testing.B, s *store.Store, r *command.Request) uint64 {
	res, idx, err := s.Execute(context.Background(), &command.ExecuteRequest{Request: r})
	if err != nil {
		b.Fatal(err)
	}
	for _, x := range res {
		if e := x.GetE(); e != nil && e.Error != "" {
			b.Fatalf("statement error: %s", e.Error)
		}
	}
	return idx
}

func waitStores(b *testing.B, idx uint64, ss ...*store.Store) {
	deadline := time.Now().Add(30 * time.Second)
	for _, s := range ss {
		for s.DBAppliedIndex() < idx {
			if time.Now().After(deadline) {
				b.Fatalf("store %s applied %d, want %d", s.ID(), s.DBAppliedIndex(), idx)
			}
			time.Sleep(time.Millisecond)
		}
	}
}

func runStatement(b *testing.B, w benchLoad, reads bool) {
	ss := newStoreCluster(b)
	var idx uint64
	for _, r := range w.setup() {
		idx = storeExec(b, ss[0], r)
	}
	waitStores(b, idx, ss...)
	reqs := benchRequests(b, w)
	qr := &command.QueryRequest{Request: req(benchRead), Level: command.ConsistencyLevel_NONE}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	cpu0 := cpuTime()
	for i := 0; i < b.N; i++ {
		idx = storeExec(b, ss[0], reqs[i])
		if reads {
			for _, f := range ss[1:] {
				if _, _, _, err := f.Query(ctx, qr); err != nil {
					b.Fatal(err)
				}
			}
		}
	}
	waitStores(b, idx, ss...)
	b.StopTimer()
	reportCPU(b, cpu0)
}

func BenchmarkFollowerApply(b *testing.B) {
	variants := []struct {
		name string
		run  func(*testing.B, benchLoad, bool)
		read bool
	}{
		{"statement", runStatement, true},
		{"walframe-reopen", runWalframe, true},
		{"statement-noread", runStatement, false},
		{"walframe-noread", runWalframe, false},
	}
	for _, w := range benchLoads() {
		b.Run(w.name, func(b *testing.B) {
			for _, v := range variants {
				b.Run(v.name, func(b *testing.B) { v.run(b, w, v.read) })
			}
		})
	}
}

// Keeping the connection open across applies (the third variant) would save
// the reopen, but it cannot serve correct reads: the VFS has no shared memory,
// so SQLite's wal-index lives on the connection's heap and does not see frames
// appended to the WAL file behind its back. This test appends a page list the
// way Commit does, minus the close, and reads through the old connection.
func TestKeepingTheConnectionOpenServesStalePages(t *testing.T) {
	defer nopanic(t)
	leader := testEngine(t, 1<<30)
	follower := testEngine(t, 1<<30)
	pl1, id1 := runOK(t, leader, "CREATE TABLE t(x)", "INSERT INTO t VALUES(1)")
	if err := leader.Commit(id1, pl1); err != nil {
		t.Fatal(err)
	}
	pl2, id2 := runOK(t, leader, "INSERT INTO t VALUES(2)")
	if err := leader.Commit(id2, pl2); err != nil {
		t.Fatal(err)
	}
	if err := follower.Commit(id1, pl1); err != nil {
		t.Fatal(err)
	}
	count := func() int64 {
		t.Helper()
		rows, err := follower.Query(bg, req("SELECT count(*) FROM t"), false)
		if err != nil || len(rows) != 1 || rows[0].Error != "" {
			t.Fatalf("query: %v %v", rows, err)
		}
		return rows[0].Values[0].Parameters[0].GetI()
	}
	if n := count(); n != 1 {
		t.Fatalf("before the second apply: %d rows", n)
	}
	// The second apply WITHOUT closing the connection.
	follower.mu.Lock()
	err := follower.appendLocked(pl2)
	follower.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if n := count(); n != 1 {
		t.Fatalf("connection kept open sees %d rows: it was expected to serve the stale 1", n)
	}
	// What Commit does: close, so the next read reopens and recovers the WAL.
	follower.mu.Lock()
	follower.closeLocked()
	follower.mu.Unlock()
	if n := count(); n != 2 {
		t.Fatalf("after a reopen: %d rows, want 2", n)
	}
}
