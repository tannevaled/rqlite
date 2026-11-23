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
		{sqlite3.SQLITE_PRAGMA, "", false},
		{sqlite3.SQLITE_SELECT, "journal_mode", false},
		{sqlite3.SQLITE_INSERT, "other.db", false},
		{sqlite3.SQLITE_READ, "", false},
	} {
		if got := guardDenies(c.op, c.z1); got != c.deny {
			t.Errorf("guardDenies(%d, %q) = %v", c.op, c.z1, got)
		}
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
	if err := setExclusiveWAL(ctx, &fakeConn{queryErr: errors.New("q")}); err == nil || !strings.Contains(err.Error(), "cannot enable WAL") {
		t.Errorf("query failing: %v", err)
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
