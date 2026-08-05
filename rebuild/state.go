package rebuild

import (
	"context"
	"fmt"
	"strconv"
	"strings"
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
// One rebuild is expected to be driven by one orchestrator at a time, so this
// contract is about a single writer's appends. Two processes racing on the same
// op are not ordered against each other by SQLStore (see UseExistingTable), and
// are not a supported way to run a rebuild.
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
//	seq       UInt64                          -- per-store tiebreaker, see below
//	op_id     String
//	spec_hash String
//	phase     String
//	status    String
//	cursor    String
//	detail    String
//
// ordered by (ts, seq), which reads back in append order for one store's
// appends. seq counts per SQLStore, so it does not order two stores against
// each other; what separates a resume from the run before it is the later ts,
// not seq. Driving one rebuild from two orchestrators at once is unsupported
// for this reason among others.
func (s *SQLStore) UseExistingTable() *SQLStore {
	s.external = true
	return s
}

// Ensure creates the state table if absent. When the table is caller-owned
// (UseExistingTable) it runs no DDL and instead verifies the table satisfies the
// contract, so a missing table or a missing column is a clear error here rather
// than a raw driver error from an INSERT partway through a rebuild.
//
// Either way the work happens at most once per store after it first succeeds,
// keeping it off the per-append hot path.
func (s *SQLStore) Ensure(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ensured {
		return nil
	}
	if s.external {
		if err := s.verifyTable(ctx); err != nil {
			return err
		}
		s.ensured = true
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

// verifyTable checks a caller-owned table satisfies the UseExistingTable
// contract. It runs once per store, on the first Ensure.
//
// The ts default gets its own check because getting it wrong fails silently
// rather than loudly: Append omits ts so the server stamps it, so a column with
// no default takes the zero value on every row. Every record then shares one
// timestamp, ordering collapses onto seq alone, and seq restarts per store — the
// exact ordering bug the tiebreaker exists to prevent, reintroduced by a table
// that looks fine.
func (s *SQLStore) verifyTable(ctx context.Context) error {
	db, table := splitTable(s.table)

	query := "SELECT name, type, default_expression FROM system.columns WHERE database = currentDatabase() AND table = ?"
	args := []any{table}
	if db != "" {
		query = "SELECT name, type, default_expression FROM system.columns WHERE database = ? AND table = ?"
		args = []any{db, table}
	}
	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("verify state table %s: %w", s.table, err)
	}
	defer rows.Close()

	types, defaults := map[string]string{}, map[string]string{}
	for rows.Next() {
		var name, typ, def string
		if err := rows.Scan(&name, &typ, &def); err != nil {
			return fmt.Errorf("verify state table %s: %w", s.table, err)
		}
		types[name], defaults[name] = typ, def
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("verify state table %s: %w", s.table, err)
	}

	if len(types) == 0 {
		return fmt.Errorf("state table %s does not exist: UseExistingTable says you create it, "+
			"and it must provide %s — see the UseExistingTable docs for the DDL",
			s.table, strings.Join(RequiredColumns, ", "))
	}
	var missing []string
	for _, c := range RequiredColumns {
		if _, ok := types[c]; !ok {
			missing = append(missing, c)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("state table %s is missing column(s) %s: it must provide %s — "+
			"see the UseExistingTable docs for the DDL",
			s.table, strings.Join(missing, ", "), strings.Join(RequiredColumns, ", "))
	}

	// seq must sort numerically. A String seq is the trap here: it compares
	// lexicographically, so the tenth append sorts before the second.
	if !strings.Contains(types["seq"], "Int") {
		return fmt.Errorf("state table %s has seq of type %s, want UInt64: "+
			"seq breaks ties on ts, and a non-integer seq sorts lexicographically ('10' before '2')",
			s.table, types["seq"])
	}

	// ts must be at least millisecond precision. Coarser and same-millisecond
	// appends collapse onto one tick, leaving only seq — which restarts per store.
	if prec, ok := dateTime64Precision(types["ts"]); !ok || prec < 3 {
		return fmt.Errorf("state table %s has ts of type %s, want DateTime64(3) or finer: "+
			"records are ordered by (ts, seq), so a coarser ts loses the ordering seq only tiebreaks",
			s.table, types["ts"])
	}
	// The default has to be now64 at millisecond scale or finer. Both now() and
	// now64(0) yield whole seconds even in a DateTime64(3) column, silently
	// reintroducing exactly the collapse the precision check above prevents.
	if scale, ok := now64Scale(defaults["ts"]); !ok || scale < 3 {
		got := defaults["ts"]
		if got == "" {
			got = "none"
		}
		return fmt.Errorf("state table %s needs ts DEFAULT now64(3) or finer, found %s: "+
			"appends omit ts so the server stamps it — with no default every record shares the zero "+
			"timestamp, and with a whole-second default (now() or now64(0)) they share one tick, so "+
			"resume can read the wrong latest record",
			s.table, got)
	}
	return nil
}

// now64Scale returns the scale of a now64(...) default expression. A bare
// now64() is scale 3, which is the function's own default. Anything that is not
// a now64 call reports !ok — including now(), which is whole seconds.
func now64Scale(expr string) (scale int, ok bool) {
	rest, found := strings.CutPrefix(strings.ToLower(strings.TrimSpace(expr)), "now64")
	if !found {
		return 0, false
	}
	rest = strings.TrimSpace(rest)
	if !strings.HasPrefix(rest, "(") || !strings.HasSuffix(rest, ")") {
		return 0, false
	}
	args := strings.TrimSpace(rest[1 : len(rest)-1])
	if args == "" {
		return 3, true // now64() is millisecond scale
	}
	first, _, _ := strings.Cut(args, ",")
	n, err := strconv.Atoi(strings.TrimSpace(first))
	if err != nil {
		return 0, false
	}
	return n, true
}

// dateTime64Precision extracts N from a DateTime64(N[, tz]) type.
func dateTime64Precision(typ string) (int, bool) {
	rest, ok := strings.CutPrefix(strings.TrimSpace(typ), "DateTime64(")
	if !ok {
		return 0, false
	}
	digits, _, _ := strings.Cut(rest, ",")
	digits = strings.TrimSuffix(strings.TrimSpace(digits), ")")
	n, err := strconv.Atoi(strings.TrimSpace(digits))
	if err != nil {
		return 0, false
	}
	return n, true
}

// splitTable splits a possibly db-qualified, possibly backtick-quoted table name
// into its database and table parts. An empty database means the connection's
// current database.
func splitTable(name string) (db, table string) {
	unquote := func(s string) string { return strings.Trim(strings.TrimSpace(s), "`") }
	if i := strings.LastIndex(name, "."); i >= 0 {
		return unquote(name[:i]), unquote(name[i+1:])
	}
	return "", unquote(name)
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
