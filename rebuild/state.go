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
// entries oldest-first. A ClickHouse-backed default is provided by NewSQLStore.
//
// Records MUST return an op's entries in append order. That ordering is load
// bearing: the orchestrator reads the last record as the current phase and
// derives completed backfill chunks from the log, so an implementation whose
// same-tick appends read back in undefined order can rewind a cursor, re-run a
// chunk, and double-count rows into an AggregatingMergeTree.
//
// Prefer NewSQLStore over a fresh implementation. To keep rebuild state in your
// own wider table (extra audit columns, say), point it at that table and call
// UseExistingTable rather than reimplementing this interface.
type StateStore interface {
	Ensure(ctx context.Context) error
	Append(ctx context.Context, r Record) error
	Records(ctx context.Context, opID string) ([]Record, error)
	SpecHashSeen(ctx context.Context, specHash string) (bool, error)
}

const DefaultTable = "_chtool_ops"

// RequiredColumns are the columns SQLStore reads and writes. A caller-owned
// table must provide them; any further columns are ignored (see
// SQLStore.UseExistingTable).
var RequiredColumns = []string{"ts", "seq", "op_id", "spec_hash", "phase", "status", "cursor", "detail"}

type SQLStore struct {
	conn     Conn
	table    string
	mu       sync.Mutex
	ensured  bool
	external bool          // table is caller-owned; never run DDL
	seq      atomic.Uint64 // tiebreaks appends that share a ts within one process
}

// NewSQLStore returns a store writing to table (may be db-qualified, e.g.
// "analytics._chtool_ops"). An empty table uses DefaultTable.
func NewSQLStore(conn Conn, table string) *SQLStore {
	if table == "" {
		table = DefaultTable
	}
	return &SQLStore{conn: conn, table: table}
}

// UseExistingTable declares the state table caller-owned: Ensure becomes a no-op
// and the store never runs DDL. Returns the store for chaining.
//
// Use it to keep rebuild state in a wider table you manage — e.g. one that also
// carries your own operator audit columns. SQLStore already works against such a
// superset: it names columns explicitly in both its INSERT and its SELECT, so
// extra columns are untouched (they must be nullable or have defaults, since
// SQLStore does not write them). Without this option the DDL is order-dependent:
// whichever of Ensure or your own migration runs first wins, and if Ensure wins
// it creates the narrow table and your audit inserts then fail.
//
// Reimplementing StateStore instead is a trap worth naming: a copy that drops
// the seq tiebreaker lets same-millisecond appends read back in undefined order,
// so the orchestrator can resolve the wrong latest record, rewind a backfill
// cursor, re-run a chunk, and double-count rows into an AggregatingMergeTree —
// silent sum inflation. Point this store at your table instead.
//
// The table must provide RequiredColumns with the same types and ordering
// semantics as the DDL in Ensure:
//
//	ts        DateTime64(3) DEFAULT now64(3)  -- server-stamped; do not bind a client time
//	seq       UInt64                          -- monotonic tiebreaker within a process
//	op_id     String
//	spec_hash String
//	phase     String
//	status    String
//	cursor    String
//	detail    String
//
// ordered by (ts, seq) so records read back in append order.
func (s *SQLStore) UseExistingTable() *SQLStore {
	s.external = true
	return s
}

// Ensure creates the state table if absent, unless the table is caller-owned
// (UseExistingTable), in which case it is a no-op. The DDL runs at most once per
// store after it first succeeds, keeping it off the per-append hot path.
func (s *SQLStore) Ensure(ctx context.Context) error {
	if s.external {
		return nil
	}
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
