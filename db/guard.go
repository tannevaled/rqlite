package db

import (
	"strings"
	"sync"
	"sync/atomic"

	"modernc.org/libc"
	sqlite3 "modernc.org/sqlite/lib"
)

// Guard is a SQLite authorizer for a connection of a SingleConn. It has four
// modes:
//
//   - off: the owner runs its own statements (a checkpoint is a PRAGMA).
//   - write (Set(true)): a user request that may write. An ALLOWLIST: only the
//     action codes in writeActions are let through, on the schema "main" alone;
//     every other code, known or not yet invented, is refused. A user can never
//     ATTACH or DETACH.
//   - vacuum (SetVacuum(true)): write mode, plus the ATTACH of a private empty
//     database and the schema vacuum_* that SQLite's own VACUUM uses. Only the
//     engine selects it, around a statement it has recognised as a plain
//     VACUUM; no SQL a client sends can reach it.
//   - read (SetReadOnly(true)): a user read. Only SELECT-class actions are
//     allowed: every write, BEGIN, SAVEPOINT, DDL, ATTACH and every PRAGMA
//     outside the read-only allowlist is refused.
//
// The authorizer runs inside SQLite at prepare time, so comments, spacing, case
// and multi-statement strings cannot get past it, unlike a text match. Every list
// here is an ALLOWLIST: what nobody thought of is refused.
type Guard struct {
	mode atomic.Int32
}

const (
	guardOff int32 = iota
	guardWrite
	guardRead
	guardVacuum
)

// Set turns the write-mode guard on or off.
func (g *Guard) Set(on bool) {
	if on {
		g.mode.Store(guardWrite)
	} else {
		g.mode.Store(guardOff)
	}
}

// SetVacuum turns the vacuum-mode guard on or off. It is for the engine's own
// plain VACUUM and must never be selected for a statement a client wrote beyond
// that one word.
func (g *Guard) SetVacuum(on bool) {
	if on {
		g.mode.Store(guardVacuum)
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
// tables are ordinary tables in the replicated pages. FTS3 is not compiled into
// this build, so only fts4 and fts5 can exist; fts3 is listed for a build that
// has it.
var vtabModules = map[string]struct{}{"fts3": {}, "fts4": {}, "fts5": {}}

// Action classes of the write allowlist.
//
//   - freeActions touch no schema: they read, or they name a transaction.
//   - schemaActions change or inspect a database named by the authorizer's
//     third argument; they are let through on "main" only.
//
// Everything else is refused: COPY, the CREATE_TEMP_* and DROP_TEMP_* codes
// (nothing in the temp schema is a replicated page), ATTACH, DETACH, and any
// code SQLite adds later. ALTER_TABLE, PRAGMA, CREATE_VTABLE and ATTACH are
// handled by name in guardDeniesMode because their arguments differ.
var freeActions = map[int32]struct{}{
	sqlite3.SQLITE_SELECT: {}, sqlite3.SQLITE_READ: {}, sqlite3.SQLITE_FUNCTION: {},
	sqlite3.SQLITE_RECURSIVE: {}, sqlite3.SQLITE_TRANSACTION: {}, sqlite3.SQLITE_SAVEPOINT: {},
}

var schemaActions = map[int32]struct{}{
	sqlite3.SQLITE_INSERT: {}, sqlite3.SQLITE_UPDATE: {}, sqlite3.SQLITE_DELETE: {},
	sqlite3.SQLITE_CREATE_TABLE: {}, sqlite3.SQLITE_CREATE_INDEX: {},
	sqlite3.SQLITE_CREATE_TRIGGER: {}, sqlite3.SQLITE_CREATE_VIEW: {},
	sqlite3.SQLITE_DROP_TABLE: {}, sqlite3.SQLITE_DROP_INDEX: {},
	sqlite3.SQLITE_DROP_TRIGGER: {}, sqlite3.SQLITE_DROP_VIEW: {},
	sqlite3.SQLITE_REINDEX: {}, sqlite3.SQLITE_ANALYZE: {}, sqlite3.SQLITE_DROP_VTABLE: {},
}

// guardDenies is guardDeniesMode for the write mode on an unqualified name.
func guardDenies(op int32, z1 string) bool { return guardDeniesMode(guardWrite, op, z1, "", "") }

// guardDeniesMode reports whether the guard refuses an authorizer action. z1,
// z2 and z3 are the action's first three arguments: for PRAGMA the pragma's
// name, its argument and the schema; for ATTACH the file name; for ALTER_TABLE
// the schema and the table; for CREATE_VTABLE the table, the module and the
// schema; for a write the table, nothing and the schema.
//
// VACUUM attaches a private empty database, with the literal name ”, and
// copies into it; SQLite offers that to the authorizer, with nothing to tell it
// from a client's own ATTACH ”. So a client's ATTACH is refused in every
// guarded mode but vacuum, which the engine selects for a plain VACUUM it has
// recognised itself.
func guardDeniesMode(mode, op int32, z1, z2, z3 string) bool {
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
	case guardWrite, guardVacuum:
		return guardDeniesWrite(mode == guardVacuum, op, z1, z2, z3)
	}
	return false
}

// guardDeniesWrite is the write allowlist; vacuum adds VACUUM's own needs.
func guardDeniesWrite(vacuum bool, op int32, z1, z2, z3 string) bool {
	if _, ok := freeActions[op]; ok {
		return false
	}
	switch op {
	case sqlite3.SQLITE_ATTACH:
		return !vacuum || z1 != ""
	case sqlite3.SQLITE_ALTER_TABLE:
		return deniedSchema(vacuum, z1)
	case sqlite3.SQLITE_PRAGMA:
		n := strings.ToLower(z1)
		_, r := readPragmas[n]
		_, w := writePragmas[n]
		return (!r && !w) || deniedSchema(vacuum, z3)
	case sqlite3.SQLITE_CREATE_VTABLE:
		_, ok := vtabModules[strings.ToLower(z2)]
		return !ok || deniedSchema(vacuum, z3)
	}
	if _, ok := schemaActions[op]; !ok {
		return true // DETACH, COPY, every temp action, and any code not listed
	}
	switch op {
	case sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE:
		if strings.EqualFold(z1, "sqlite_dbpage") {
			return true
		}
	}
	return deniedSchema(vacuum, z3)
}

// deniedSchema reports whether a write to schema s is refused: only "main" is
// replicated. An empty name is main. VACUUM's private copy, vacuum_<hex> (vacuum_db
// in older SQLite), exists only while the engine's own VACUUM runs.
func deniedSchema(vacuum bool, s string) bool {
	if s == "" || strings.EqualFold(s, "main") {
		return false
	}
	return !(vacuum && strings.HasPrefix(strings.ToLower(s), "vacuum_"))
}

// guardDeniesNullAttach reports whether the guard refuses an ATTACH whose file
// name reached the authorizer as a NULL pointer. SQLite passes the name only
// when it is a string LITERAL (VACUUM's own ATTACH is the literal ”); an
// expression ('p' || ”), a bound parameter or any computed name arrives as NULL,
// which GoString turns into "" and would pass for VACUUM's. Such a name can open
// any file, so a NULL is always refused in the guarded write modes.
func guardDeniesNullAttach(mode, op int32, z1 uintptr) bool {
	return (mode == guardWrite || mode == guardVacuum) && op == sqlite3.SQLITE_ATTACH && z1 == 0
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

func guardCallback(tls *libc.TLS, pArg uintptr, op int32, z1, z2, z3, _ uintptr) int32 {
	guards.RLock()
	g := guards.m[pArg]
	guards.RUnlock()
	if g == nil {
		return sqlite3.SQLITE_OK
	}
	// In off mode guardDeniesMode refuses nothing, so there is no separate path.
	mode := g.mode.Load()
	if mode == guardOff {
		return sqlite3.SQLITE_OK
	}
	// The hot path: reading is allowed in every guarded mode, and decoding the
	// arguments of every column read would only cost.
	switch op {
	case sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION, sqlite3.SQLITE_RECURSIVE:
		return sqlite3.SQLITE_OK
	}
	// GoString of a NULL pointer is "".
	if guardDeniesNullAttach(mode, op, z1) ||
		guardDeniesMode(mode, op, libc.GoString(z1), libc.GoString(z2), libc.GoString(z3)) {
		return sqlite3.SQLITE_DENY
	}
	return sqlite3.SQLITE_OK
}
