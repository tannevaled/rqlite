package db

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"log"
	"path/filepath"
	"strings"
	"testing"

	command "github.com/rqlite/rqlite/v10/command/proto"
	sqlite3 "modernc.org/sqlite/lib"
)

func TestGuardDenies(t *testing.T) {
	for _, c := range []struct {
		op   int32
		z1   string
		deny bool
	}{
		{sqlite3.SQLITE_ATTACH, "other.db", true},
		{sqlite3.SQLITE_ATTACH, "", false}, // VACUUM's private database
		{sqlite3.SQLITE_DETACH, "", true},
		{sqlite3.SQLITE_DETACH, "x", true},
		{sqlite3.SQLITE_PRAGMA, "journal_mode", true},
		{sqlite3.SQLITE_PRAGMA, "JOURNAL_MODE", true},
		{sqlite3.SQLITE_PRAGMA, "Wal_Checkpoint", true},
		{sqlite3.SQLITE_PRAGMA, "writable_schema", true},
		{sqlite3.SQLITE_PRAGMA, "table_info", false},
		{sqlite3.SQLITE_PRAGMA, "", true}, // allowlist: an unnamed pragma is refused
		{sqlite3.SQLITE_PRAGMA, "query_only", true},
		{sqlite3.SQLITE_PRAGMA, "foreign_keys", true},
		{sqlite3.SQLITE_PRAGMA, "hard_heap_limit", true},
		{sqlite3.SQLITE_PRAGMA, "user_version", false},
		{sqlite3.SQLITE_UPDATE, "sqlite_dbpage", true},
		{sqlite3.SQLITE_INSERT, "SQLITE_DBPAGE", true},
		{sqlite3.SQLITE_UPDATE, "t", false},
		{sqlite3.SQLITE_SELECT, "journal_mode", false},
		{sqlite3.SQLITE_INSERT, "other.db", false},
		{sqlite3.SQLITE_READ, "", false},
	} {
		if got := guardDenies(c.op, c.z1); got != c.deny {
			t.Errorf("guardDenies(%d, %q) = %v", c.op, c.z1, got)
		}
	}
}

func TestGuardDeniesReadMode(t *testing.T) {
	for _, c := range []struct {
		op   int32
		z1   string
		deny bool
	}{
		{sqlite3.SQLITE_SELECT, "", false},
		{sqlite3.SQLITE_READ, "t", false},
		{sqlite3.SQLITE_FUNCTION, "abs", false},
		{sqlite3.SQLITE_RECURSIVE, "", false},
		{sqlite3.SQLITE_PRAGMA, "table_info", false},
		{sqlite3.SQLITE_PRAGMA, "integrity_check", false},
		{sqlite3.SQLITE_PRAGMA, "user_version", true},
		{sqlite3.SQLITE_PRAGMA, "query_only", true},
		{sqlite3.SQLITE_INSERT, "t", true},
		{sqlite3.SQLITE_UPDATE, "t", true},
		{sqlite3.SQLITE_DELETE, "t", true},
		{sqlite3.SQLITE_TRANSACTION, "BEGIN", true},
		{sqlite3.SQLITE_SAVEPOINT, "BEGIN", true},
		{sqlite3.SQLITE_CREATE_TABLE, "x", true},
		{sqlite3.SQLITE_CREATE_TEMP_TABLE, "x", true},
		{sqlite3.SQLITE_ATTACH, "", true},
		{sqlite3.SQLITE_ANALYZE, "t", true},
	} {
		if got := guardDeniesMode(guardRead, c.op, c.z1, ""); got != c.deny {
			t.Errorf("read mode: guardDeniesMode(%d, %q) = %v", c.op, c.z1, got)
		}
	}
	if !guardDeniesMode(guardWrite, sqlite3.SQLITE_CREATE_VTABLE, "v", "rtree") ||
		guardDeniesMode(guardWrite, sqlite3.SQLITE_CREATE_VTABLE, "v", "FTS5") {
		t.Error("virtual-table module allowlist")
	}
}

// A connection with no guard registered is never refused.
func TestGuardCallbackWithoutAGuard(t *testing.T) {
	var got int32 = -1
	func() {
		defer func() { recover() }()
		got = guardCallback(nil, 0xdead, sqlite3.SQLITE_DETACH, 0, 0, 0, 0)
	}()
	if got != sqlite3.SQLITE_OK {
		t.Errorf("unregistered connection: verdict %d", got)
	}
}

type fakeConn struct {
	execFail  int // fail the Nth ExecContext (1-based)
	execs     int
	queryErr  error
	rows      driver.Rows
	queriedBy string
}

func (f *fakeConn) ExecContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Result, error) {
	f.execs++
	if f.execs == f.execFail {
		return nil, errors.New("exec refused")
	}
	return nil, nil
}

func (f *fakeConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	f.queriedBy = q
	return f.rows, f.queryErr
}

type fakeRows struct {
	val driver.Value
	err error
}

func (r *fakeRows) Columns() []string { return []string{"journal_mode"} }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.err != nil {
		return r.err
	}
	dest[0] = r.val
	return nil
}

func TestSetExclusiveWAL(t *testing.T) {
	ctx := context.Background()
	if err := setExclusiveWAL(ctx, &fakeConn{rows: &fakeRows{val: "wal"}}); err != nil {
		t.Errorf("wal: %v", err)
	}
	for n := 1; n <= 3; n++ {
		err := setExclusiveWAL(ctx, &fakeConn{execFail: n, rows: &fakeRows{val: "wal"}})
		if err == nil || !strings.Contains(err.Error(), "exec refused") || !strings.HasPrefix(err.Error(), "PRAGMA ") {
			t.Errorf("pragma %d failing: %v", n, err)
		}
	}
	// Rows are present, so only the error can refuse (nil rows would panic a mutant).
	if err := setExclusiveWAL(ctx, &fakeConn{queryErr: errors.New("q"), rows: &fakeRows{val: "wal"}}); err == nil || !strings.Contains(err.Error(), "cannot enable WAL") {
		t.Errorf("query failing with rows: %v", err)
	}
	if err := setExclusiveWAL(ctx, &fakeConn{rows: &fakeRows{err: errors.New("r")}}); err == nil || !strings.Contains(err.Error(), "cannot read journal mode") {
		t.Errorf("row failing: %v", err)
	}
	if err := setExclusiveWAL(ctx, &fakeConn{rows: &fakeRows{val: "delete"}}); err == nil || !strings.Contains(err.Error(), "want wal") {
		t.Errorf("delete mode: %v", err)
	}
	if err := setExclusiveWAL(ctx, &fakeConn{rows: &fakeRows{val: []byte("wal")}}); err == nil {
		t.Error("a non-string mode was accepted")
	}
}

func TestDefensiveFailureRefusesTheConnection(t *testing.T) {
	old := defensiveOn
	defer func() { defensiveOn = old }()
	defensiveOn = func(rawConn) error { return errors.New("no defensive") }
	var g Guard
	sc, err := OpenSingleConn(ConnConfig{Guard: &g}, "file:"+filepath.Join(t.TempDir(), "d.db"), nil)
	if err == nil {
		sc.Close()
		t.Fatal("connection opened although DEFENSIVE failed")
	}
	if !strings.Contains(err.Error(), "no defensive") {
		t.Errorf("error: %v", err)
	}
}

func TestGuardOffRefusesNothing(t *testing.T) {
	for _, op := range []int32{sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_DETACH, sqlite3.SQLITE_PRAGMA, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_CREATE_VTABLE} {
		if guardDeniesMode(guardOff, op, "sqlite_dbpage", "rtree") {
			t.Errorf("off mode denies op %d", op)
		}
	}
}

func TestOpenSingleConnRefusesWhatIsNotWAL(t *testing.T) {
	// An in-memory database cannot be in WAL mode.
	if sc, err := OpenSingleConn(ConnConfig{}, ":memory:", log.New(io.Discard, "", 0)); err == nil {
		sc.Close()
		t.Fatal("in-memory database accepted")
	} else if !strings.Contains(err.Error(), "journal_mode") {
		t.Fatalf("error: %v", err)
	}
	if sc, err := OpenSingleConn(ConnConfig{}, "file:"+filepath.Join(t.TempDir(), "no", "such", "x.db"), nil); err == nil {
		sc.Close()
		t.Fatal("database in a missing directory accepted")
	}
}

func TestGuardOnARealConnection(t *testing.T) {
	var g Guard
	sc, err := OpenSingleConn(ConnConfig{Guard: &g}, "file:"+filepath.Join(t.TempDir(), "g.db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Close()
	conn, err := sc.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	run := func(sql string) string {
		t.Helper()
		res, err := conn.Request(context.Background(), &command.Request{Statements: []*command.Statement{{Sql: sql}}}, false)
		if err != nil {
			return err.Error()
		}
		if e := res[0].GetE(); e != nil {
			return e.Error
		}
		if q := res[0].GetQ(); q != nil && q.Error != "" {
			return q.Error
		}
		return ""
	}

	if e := run("CREATE TABLE t(x)"); e != "" {
		t.Fatal(e)
	}
	refused := []string{
		"PRAGMA journal_mode=delete",
		"PRAGMA/**/journal_mode=delete",
		"pragma  JOURNAL_MODE",
		"PRAGMA [main].[wal_checkpoint]",
		"SELECT 1; ATTACH 'x.db' AS x",
		"ATTACH 'x.db' AS x",
		"DETACH x",
	}
	g.Set(true)
	for _, s := range refused {
		if e := run(s); !strings.Contains(e, "not authorized") {
			t.Errorf("guard on, %q: %q", s, e)
		}
	}
	for _, s := range []string{"INSERT INTO t VALUES(1)", "SELECT * FROM t", "PRAGMA table_info(t)", "VACUUM"} {
		if e := run(s); e != "" {
			t.Errorf("guard on, %q refused: %s", s, e)
		}
	}
	// DEFENSIVE and the authorizer both keep sqlite_dbpage read-only.
	for _, s := range []string{"UPDATE sqlite_dbpage SET data=zeroblob(4096) WHERE pgno=1", "PRAGMA query_only=1", "PRAGMA foreign_keys=OFF"} {
		if e := run(s); !strings.Contains(e, "not authorized") {
			t.Errorf("guard on, %q: %q", s, e)
		}
	}
	g.SetReadOnly(true)
	for _, s := range []string{"INSERT INTO t VALUES(2)", "DELETE FROM t RETURNING x", "BEGIN", "SAVEPOINT a", "PRAGMA user_version=3", "CREATE TEMP TABLE z(a)"} {
		if e := run(s); !strings.Contains(e, "not authorized") {
			t.Errorf("read-only guard, %q: %q", s, e)
		}
	}
	if e := run("SELECT * FROM t"); e != "" {
		t.Errorf("read-only guard refused a SELECT: %s", e)
	}
	g.Set(false)
	if e := run("PRAGMA wal_checkpoint(PASSIVE)"); strings.Contains(e, "not authorized") {
		t.Errorf("guard off still refuses: %s", e)
	}
	g.Set(true)
	g.Set(false)
	if e := run("PRAGMA journal_mode"); strings.Contains(e, "not authorized") {
		t.Errorf("guard off again still refuses: %s", e)
	}
}
