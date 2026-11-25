package db

import (
	"context"
	"database/sql"
	"fmt"
	"log"
)

// SingleConn is a database with exactly one connection, in exclusive WAL
// mode, opened on a DSN of the caller's choosing (typically one naming a VFS
// registered with the driver). It exists for the walframe package: a VFS with
// no shared memory cannot run WAL with more than one connection.
type SingleConn struct {
	pool
}

// OpenSingleConn opens the one connection and checks it. cfg.ExclusiveWAL is
// forced on, and checkpoint-on-close is forced off.
func OpenSingleConn(cfg ConnConfig, dsn string, logger *log.Logger) (*SingleConn, error) {
	cfg.ExclusiveWAL = true
	cfg.ChkOnClose = CnkOnCloseModeDisabled
	sqlDB := sql.OpenDB(cfg.connector(dsn))
	sqlDB.SetConnMaxLifetime(0)
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)
	if err := sqlDB.Ping(); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("single-connection database: %w", err)
	}
	return &SingleConn{pool{sqlDB: sqlDB, dsn: dsn, logger: logger}}, nil
}

// Acquire borrows the connection, waiting for it. The caller must close it
// (which returns it, it does not close the database).
func (s *SingleConn) Acquire(ctx context.Context) (*Conn, error) {
	return s.pool.acquire(ctx)
}

// Close closes the database and its connection.
func (s *SingleConn) Close() error {
	return s.pool.close()
}
