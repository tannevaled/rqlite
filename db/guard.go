package db

import (
	"strings"
	"sync"
	"sync/atomic"

	"modernc.org/libc"
	sqlite3 "modernc.org/sqlite/lib"
)

// Guard is a SQLite authorizer for a connection of a SingleConn. It has three
// modes:
//
//   - off: the owner runs its own statements (a checkpoint is a PRAGMA).
//   - write (Set(true)): a user request that may write. ATTACH, DETACH, every
//     PRAGMA not on an allowlist, writes to sqlite_dbpage and virtual-table
//     modules not on an allowlist are refused.
//   - read (SetReadOnly(true)): a user read. Only SELECT-class actions are
//     allowed: every write, BEGIN, SAVEPOINT, DDL, ATTACH and every PRAGMA
//     outside the read-only allowlist is refused.
//
// The authorizer runs inside SQLite at prepare time, so comments, spacing, case
// and multi-statement strings cannot get past it, unlike a text match. The
// pragma lists are ALLOWLISTS: a pragma nobody thought of is refused.
type Guard struct {
	mode atomic.Int32
}

const (
	guardOff int32 = iota
	guardWrite
	guardRead
)

// Set turns the write-mode guard on or off.
func (g *Guard) Set(on bool) {
	if on {
		g.mode.Store(guardWrite)
	} else {
		g.mode.Store(guardOff)
	}
}

// SetReadOnly turns the read-only guard on or off.
func (g *Guard) SetReadOnly(on bool) {
	if on {
		g.mode.Store(guardRead)
	} else {
		g.mode.Store(guardOff)
	}
}

// readPragmas are the pragmas allowed in every guarded mode: they only report.
// Names are compared without regard to case, as SQLite does.
var readPragmas = map[string]struct{}{
	"table_info": {}, "table_xinfo": {}, "table_list": {}, "index_list": {},
	"index_info": {}, "index_xinfo": {}, "foreign_key_list": {}, "foreign_key_check": {},
	"integrity_check": {}, "quick_check": {}, "collation_list": {}, "database_list": {},
	"function_list": {}, "module_list": {}, "pragma_list": {}, "compile_options": {},
	"data_version": {}, "freelist_count": {}, "page_count": {},
}

// writePragmas are allowed in write mode on top of readPragmas: their effect is
// on pages the replication carries (the header, sqlite_stat1), never on the
// connection or the files.
var writePragmas = map[string]struct{}{
	"user_version": {}, "application_id": {}, "optimize": {},
}

// vtabModules are the virtual-table modules a user may create: their shadow
// tables are ordinary tables in the replicated pages.
var vtabModules = map[string]struct{}{"fts3": {}, "fts4": {}, "fts5": {}}

// guardDenies is guardDeniesMode for the write mode, without a second argument.
func guardDenies(op int32, z1 string) bool { return guardDeniesMode(guardWrite, op, z1, "") }

// guardDeniesMode reports whether the guard refuses an authorizer action. z1
// and z2 are the action's first two arguments: for PRAGMA the pragma's name,
// for ATTACH the file name, for CREATE_VTABLE the table and the module, for a
// write the table.
//
// VACUUM attaches a private temporary database, with an empty file name, and
// SQLite offers that to the authorizer; it is the one ATTACH let through in
// write mode (SQLite gives ATTACH no second argument to tell them apart). A user
// can spell it too, ATTACH ” AS x: a private, empty, per-connection database
// that is not replicated. Data put there exists on the Leader only, so
// statements that use it would diverge; this is a known limit of the prototype.
func guardDeniesMode(mode, op int32, z1, z2 string) bool {
	switch mode {
	case guardRead:
		switch op {
		case sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION, sqlite3.SQLITE_RECURSIVE:
			return false
		case sqlite3.SQLITE_PRAGMA:
			_, ok := readPragmas[strings.ToLower(z1)]
			return !ok
		}
		return true
	case guardWrite:
		switch op {
		case sqlite3.SQLITE_ATTACH:
			return z1 != ""
		case sqlite3.SQLITE_DETACH:
			return true
		case sqlite3.SQLITE_PRAGMA:
			n := strings.ToLower(z1)
			_, r := readPragmas[n]
			_, w := writePragmas[n]
			return !r && !w
		case sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_INSERT, sqlite3.SQLITE_DELETE:
			return strings.EqualFold(z1, "sqlite_dbpage")
		case sqlite3.SQLITE_CREATE_VTABLE:
			_, ok := vtabModules[strings.ToLower(z2)]
			return !ok
		}
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

func guardCallback(tls *libc.TLS, pArg uintptr, op int32, z1, z2, _, _ uintptr) int32 {
	guards.RLock()
	g := guards.m[pArg]
	guards.RUnlock()
	if g == nil {
		return sqlite3.SQLITE_OK
	}
	mode := g.mode.Load()
	if mode == guardOff {
		return sqlite3.SQLITE_OK
	}
	var s1, s2 string
	// GoString of a NULL pointer is "".
	switch op {
	case sqlite3.SQLITE_PRAGMA, sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_INSERT, sqlite3.SQLITE_DELETE:
		s1 = libc.GoString(z1)
	case sqlite3.SQLITE_CREATE_VTABLE:
		s1, s2 = libc.GoString(z1), libc.GoString(z2)
	}
	if guardDeniesMode(mode, op, s1, s2) {
		return sqlite3.SQLITE_DENY
	}
	return sqlite3.SQLITE_OK
}
