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
//     Raft-committed end (an incremental scan, not the whole WAL) and extracts
//     the committed transactions as a page list (see PageList). It holds the
//     request mutex until Raft commits or refuses the entry.
//   - If Raft refuses the entry (lost leadership, timeout), the Leader undoes:
//     fence the VFS, close the connection, truncate the WAL to the committed
//     end, reopen lazily. Never a truncation under an open connection. The
//     committed end is never re-derived from the WAL; if the truncate fails the
//     engine is FATAL (ErrStopped on Run, Query, Checkpoint) until Restore.
//   - Followers re-frame the pages into their own WAL with their own salts and
//     checksums, closing the connection first (lazy reopen on the next read).
//   - Checkpoints are node-local and never replicated.
//   - Snapshot (WFS1) = database file plus committed WAL prefix, with a
//     checksum.
//
// # Base check (stale base)
//
// Every page list carries the base it was built on: the sequence number and a
// SHA-256 hash chain of the entries applied so far (BaseSeq, BaseHash). The
// FSM refuses a mismatch on every node alike (ErrStaleBase, wrapped in
// ErrRejected): deterministic, nothing applied, and the Leader returns the
// error to the client instead of acknowledging. This is the same idea as the
// pre-apply checksum of LiteFS (superfly/litefs db.go:2397-2404, "pre-apply
// checksum mismatch": hdr.PreApplyChecksum against prevPos.PostApplyChecksum;
// store.go:1557 verifies it before applying an LTX). dqlite src/vfs.c is cited
// only for the WAL checksum chain (the new frames' checksum is seeded from the
// WAL header or the last frame, lines ~2729-2745, vfsFrameFill ~2786); it has
// no explicit base check. Every command ends with a SHA-256 trailer so a bit
// flip is refused.
//
// # Guard
//
// Statements go through a SQLite authorizer (db.Guard) at prepare time, so
// comments, spacing, case and multi-statement strings cannot get past it.
// Write mode refuses ATTACH/DETACH, writes to sqlite_dbpage and every PRAGMA
// not on an allowlist (an allowlist, not a denylist); virtual table modules
// are allowlisted; SQLITE_DBCONFIG_DEFENSIVE is on. Read mode (Query, on every
// node) allows only SELECT, READ, FUNCTION, RECURSIVE and allowlisted
// pragmas: BEGIN, DML, DDL, SAVEPOINT and query_only are refused, queries run
// under a timeout (SetQueryTimeout) and roll back any open transaction.
//
// # Failure states
//
// An entry the FSM refuses (ErrRejected: stale base, bad page size, bad DBSize,
// bit flip, empty list, first entry without page 1) is deterministic and not
// fatal. Any other apply error stops the node (Node.Apply returns it, the
// engine is ErrStopped) until Restore installs a snapshot. Snapshots are
// validated before anything is installed: checksum, SQLite header, WAL scan,
// and integrity_check on a scratch copy.
//
// # Bounds
//
// An entry larger than SetMaxEntryBytes is refused before it is proposed
// (ErrEntryTooLarge). The WAL is truncated after a rolled-back spill. The page
// size is fixed by the first entry and checked on every entry; DBSize must be
// at most the pages known plus the pages supplied.
//
// # Limits
//
// No restart persistence (state is rebuilt from the Raft snapshot and log); no
// concurrent readers; snapshots are built in memory; temp and ” databases are
// Leader-only (not replicated, so statements using them diverge); a read can
// delay the FSM at most by the query timeout; the DBSize growth rule is
// conservative; an entry whose future failed may still commit through a new
// Leader. A 1 MB update still allocates about 15.6 MB/op across a 3-node
// cluster (the entry is copied several times per node: extract, marshal, hash,
// raft encode, decode, re-frame); this is a copy remainder, not a rescan.
// Not covered by tests: the Restore over-limit snapshot, the PageImage size
// bound, a dedicated scanTail unit test, a snapshot fuzz target. The flag is
// not wired into Store or rqlited.
package walframe
