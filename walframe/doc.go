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
// Write mode is an allowlist that denies by default: SELECT, READ, FUNCTION,
// RECURSIVE, TRANSACTION and SAVEPOINT, and on schema main only INSERT, UPDATE,
// DELETE, CREATE and DROP of TABLE, INDEX, TRIGGER and VIEW, DROP_VTABLE,
// REINDEX, ANALYZE, ALTER_TABLE, the pragmas user_version, application_id and
// optimize, and CREATE VIRTUAL TABLE for fts3/fts4/fts5. Every action that names
// a schema is checked against it, so temp, an attached database or a
// vacuum_* name is refused; so are COPY, DETACH, writes to sqlite_dbpage and
// any action code not listed (including one a later SQLite adds). A client's SQL
// never gets ATTACH: the one ATTACH ” that VACUUM needs is let through only
// while the engine runs a statement it recognised as exactly one plain VACUUM
// (Guard.SetVacuum), and the vacuum_* schema names are accepted only then.
// SQLITE_DBCONFIG_DEFENSIVE is on. Read mode (on every node) allows only SELECT,
// READ, FUNCTION, RECURSIVE and the read-only pragmas: BEGIN, DML, DDL,
// SAVEPOINT and query_only are refused, queries run under a timeout
// (SetQueryTimeout, which bounds Run as well) and roll back any open
// transaction.
//
// # Read consistency
//
// Node.QueryLevel mirrors rqlite's levels. None reads this node's state, on any
// node, and may be stale. Weak requires the node to be the Raft Leader and to
// have run its barrier in this term; Raft steps a Leader down after
// LeaderLeaseTimeout without a quorum, so a Weak read can be stale for at most
// about that long. Linearizable (the zero value, and the level of Node.Query)
// also asks Raft to verify leadership with a quorum before reading. A read-only
// request sent through Execute is checked like a Linearizable read. There is no
// "strong" level: writes already go through the log. One barrier is shared by all
// the requests of a term, even when some of them give up waiting for it.
//
// # Failure states
//
// An entry the FSM refuses (ErrRejected: stale base, bad page size, bad DBSize,
// bit flip, empty list, first entry without page 1, and bytes that do not decode
// at all) is deterministic and not fatal: it decodes the same on every node and
// changes nothing. Any other apply error stops the node (Node.Apply returns it,
// the engine is ErrStopped) until Restore installs a snapshot. A stopped Leader
// hands the leadership over, at most stepDownBudget times (with a growing pause
// between handovers) until a Restore: a cluster whose nodes all stopped waits
// for an operator and does not elect in a loop.
//
// A stopped node that is still Leader once the budget is spent refuses every
// write although the other nodes are healthy: an availability cost, reached
// only by an engine-fatal apply failure (a disk error, corruption), never by a
// client statement. It is kept on purpose. Leaving the voter set (RemoveServer,
// or DemoteVoter) is a configuration change that shrinks the quorum, so a
// bug that stops nodes one after another would shrink the cluster to a single
// voter, and nothing would put the node back after its Restore; shutting the
// Raft down loses the node's log and snapshots on the same condition. The
// operator's remedy is a Restore on the stopped node, or removing it from the
// cluster. Snapshots are
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
// concurrent readers; snapshots are built in memory; a single-node cluster cannot step
// down; a request waiting for the request mutex ignores its context; a read can
// delay the FSM at most by the query timeout; the DBSize growth rule is
// conservative; an entry whose future failed may still commit through a new
// Leader. A 1 MB update still allocates about 15.6 MB/op across a 3-node
// cluster (the entry is copied several times per node: extract, marshal, hash,
// raft encode, decode, re-frame); this is a copy remainder, not a rescan.
// Not covered by tests: the Restore over-limit snapshot, the PageImage size
// bound, a dedicated scanTail unit test, a snapshot fuzz target. The flag is
// not wired into Store or rqlited.
package walframe
