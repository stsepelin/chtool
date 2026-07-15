package rebuild

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// Record is one appended state entry. Records for an op are returned in
// chronological order; the orchestrator interprets them (latest phase,
// completed chunks, boundary T).
type Record struct {
	OpID     string
	SpecHash string
	Phase    string
	Status   string
	Cursor   string
	Detail   string
}

// StateStore persists rebuild progress so a run resumes after any interruption.
// It is an append-only log: Append adds an entry, Records returns an op's
// entries oldest-first. Implement it over any store; a ClickHouse-backed
// default is provided by NewSQLStore.
type StateStore interface {
	Ensure(ctx context.Context) error
	Append(ctx context.Context, r Record) error
	Records(ctx context.Context, opID string) ([]Record, error)
	SpecHashSeen(ctx context.Context, specHash string) (bool, error)
}

const DefaultTable = "_chtool_ops"

type SQLStore struct {
	conn    Conn
	table   string
	mu      sync.Mutex
	ensured bool
	seq     atomic.Uint64 // tiebreaks appends that share a ts within one process
}

// NewSQLStore returns a store writing to table (may be db-qualified, e.g.
// "analytics._chtool_ops"). An empty table uses DefaultTable.
func NewSQLStore(conn Conn, table string) *SQLStore {
	if table == "" {
		table = DefaultTable
	}
	return &SQLStore{conn: conn, table: table}
}

// Ensure creates the state table if absent. The DDL runs at most once per store
// after it first succeeds, keeping it off the per-append hot path.
func (s *SQLStore) Ensure(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ensured {
		return nil
	}
	// ts is stamped by the server via DEFAULT now64(3): binding a client
	// time.Now() truncates to whole seconds when written, collapsing distinct
	// phase records onto the same tick. seq tiebreaks the ts sort so rapid
	// same-tick appends still read back in append order — without it the
	// orchestrator's "latest phase = records[len-1]" can resolve to a
	// non-terminal phase. seq is monotonic within a process; across a resume the
	// later ts already orders the new records after the old.
	ddl := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		ts        DateTime64(3) DEFAULT now64(3),
		seq       UInt64,
		op_id     String,
		spec_hash String,
		phase     String,
		status    String,
		cursor    String,
		detail    String
	) ENGINE = MergeTree ORDER BY (ts, seq)`, s.table)
	if err := s.conn.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("ensure %s: %w", s.table, err)
	}
	s.ensured = true
	return nil
}

func (s *SQLStore) Append(ctx context.Context, r Record) error {
	if err := s.Ensure(ctx); err != nil {
		return err
	}
	// ts is omitted so the server stamps it with DEFAULT now64(3) (real
	// millisecond precision); binding a Go time truncates to whole seconds.
	q := fmt.Sprintf(
		"INSERT INTO %s (seq, op_id, spec_hash, phase, status, cursor, detail) VALUES (?,?,?,?,?,?,?)", s.table)
	if err := s.conn.Exec(ctx, q, s.seq.Add(1), r.OpID, r.SpecHash, r.Phase, r.Status, r.Cursor, r.Detail); err != nil {
		return fmt.Errorf("append %s: %w", s.table, err)
	}
	return nil
}

func (s *SQLStore) Records(ctx context.Context, opID string) ([]Record, error) {
	if err := s.Ensure(ctx); err != nil {
		return nil, err
	}
	rows, err := s.conn.Query(ctx,
		fmt.Sprintf("SELECT op_id, spec_hash, phase, status, cursor, detail FROM %s WHERE op_id = ? ORDER BY ts ASC, seq ASC", s.table), opID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Record
	for rows.Next() {
		var r Record
		if err := rows.Scan(&r.OpID, &r.SpecHash, &r.Phase, &r.Status, &r.Cursor, &r.Detail); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *SQLStore) SpecHashSeen(ctx context.Context, specHash string) (bool, error) {
	if err := s.Ensure(ctx); err != nil {
		return false, err
	}
	rows, err := s.conn.Query(ctx, fmt.Sprintf("SELECT count() FROM %s WHERE spec_hash = ?", s.table), specHash)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	var n uint64
	if rows.Next() {
		if err := rows.Scan(&n); err != nil {
			return false, err
		}
	}
	return n > 0, rows.Err()
}
