package db

import (
	"strings"
	"sync"
	"sync/atomic"

	"modernc.org/libc"
	sqlite3 "modernc.org/sqlite/lib"
)

// Guard is a SQLite authorizer for a connection of a SingleConn. While it is
// on, the connection refuses ATTACH, DETACH and the PRAGMAs that change
// file-level settings or write pages outside the replicated path
// (journal_mode, wal_checkpoint, ...). The authorizer runs inside SQLite at
// prepare time, so comments, spacing, case and multi-statement strings cannot
// get past it, unlike a text match.
//
// It is off while the owner runs its own statements (a checkpoint is a PRAGMA).
type Guard struct {
	on atomic.Bool
}

// Set turns the guard on or off.
func (g *Guard) Set(on bool) { g.on.Store(on) }

// guardedPragmas are refused while a Guard is on. Names are compared without
// regard to case, as SQLite does.
var guardedPragmas = map[string]struct{}{
	"journal_mode": {}, "wal_checkpoint": {}, "wal_autocheckpoint": {}, "locking_mode": {},
	"page_size": {}, "auto_vacuum": {}, "synchronous": {}, "writable_schema": {},
	"temp_store": {}, "mmap_size": {}, "schema_version": {}, "journal_size_limit": {},
	"max_page_count": {}, "temp_store_directory": {},
	"data_store_directory": {}, "trusted_schema": {}, "cell_size_check": {},
}

// guardDenies reports whether the guard refuses an authorizer action. z1 is the
// action's first argument: for PRAGMA the pragma's name, for ATTACH the file
// name.
//
// VACUUM attaches a private temporary database, with an empty file name, and
// SQLite offers that to the authorizer; it is the one ATTACH let through
// (SQLite gives ATTACH no second argument to tell them apart). A user can
// spell it too, ATTACH ” AS x: a private, empty, per-connection database
// that is not replicated. Data put there exists on the Leader only, so
// statements that use it would diverge; this is a known limit of the prototype.
func guardDenies(op int32, z1 string) bool {
	switch op {
	case sqlite3.SQLITE_ATTACH:
		return z1 != ""
	case sqlite3.SQLITE_DETACH:
		return true
	case sqlite3.SQLITE_PRAGMA:
		_, bad := guardedPragmas[strings.ToLower(z1)]
		return bad
	}
	return false
}

var guards = struct {
	sync.RWMutex
	m map[uintptr]*Guard
}{m: make(map[uintptr]*Guard)}

// installGuard makes g the authorizer of the connection.
func (r rawConn) installGuard(g *Guard) {
	guards.Lock()
	guards.m[r.db] = g
	guards.Unlock()
	sqlite3.Xsqlite3_set_authorizer(r.tls, r.db, cFuncPointer(guardCallback), r.db)
}

func guardCallback(tls *libc.TLS, pArg uintptr, op int32, z1, _, _, _ uintptr) int32 {
	guards.RLock()
	g := guards.m[pArg]
	guards.RUnlock()
	if g == nil || !g.on.Load() {
		return sqlite3.SQLITE_OK
	}
	var s1 string
	if op == sqlite3.SQLITE_PRAGMA || op == sqlite3.SQLITE_ATTACH {
		s1 = libc.GoString(z1) // GoString of a NULL pointer is ""
	}
	if guardDenies(op, s1) {
		return sqlite3.SQLITE_DENY
	}
	return sqlite3.SQLITE_OK
}
