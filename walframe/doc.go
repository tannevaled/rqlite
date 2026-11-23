// Package walframe is a PROTOTYPE of WAL-frame replication (dqlite style):
// the Leader runs a request against SQLite and replicates the physical pages
// it changed, so every node holds byte-identical pages whatever the SQL did
// (random(), randomblob(?), now(), extensions, ...). Not for merge; it exists
// to measure feasibility on pure Go (modernc.org/sqlite, CGO_ENABLED=0).
//
// # Design
//
//   - One exclusive connection per node (PRAGMA locking_mode=EXCLUSIVE,
//     journal_mode=WAL set after open), because the modernc VFS hook has no
//     shared memory.
//   - The Leader runs the request, then parses its own WAL from the last
//     Raft-committed end and extracts the committed transactions as a page
//     list (see PageList). It holds the request mutex, which every read also
//     takes, until Raft commits or refuses the entry.
//   - If Raft refuses the entry (lost leadership, timeout), the Leader undoes:
//     fence the VFS, close the connection, truncate the WAL to the committed
//     end, reopen lazily. Never a truncation under an open connection.
//   - Followers re-frame the pages into their own WAL with their own salts and
//     checksums, closing the connection first (lazy reopen on the next read).
//     A follower that never reads never pays SQLite WAL recovery.
//   - Checkpoints are node-local and never replicated.
//   - Snapshot = database file plus committed WAL prefix.
//
// # Refused in this mode
//
// ATTACH; PRAGMA journal_mode, wal_checkpoint, wal_autocheckpoint,
// locking_mode, page_size, auto_vacuum, synchronous, writable_schema,
// temp_store, mmap_size; a transaction left open across requests. These are
// matched by a regexp, which comments or odd spacing may evade: an authorizer
// hook would be the real gate. Node-local page writers (auto-VACUUM,
// auto-optimize) are refused or absent; user-issued VACUUM, ANALYZE and PRAGMA
// optimize go through Execute and are replicated as pages (VACUUM ships
// roughly the whole database).
//
// # Limits
//
// No restart persistence (state is rebuilt from the Raft snapshot and log); no
// concurrent readers; snapshots are built in memory; the Leader scans its WAL
// on every request; an entry whose future failed may still commit through a
// new Leader. The flag is not wired into Store or rqlited.
package walframe
