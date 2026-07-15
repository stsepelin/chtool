package rebuild

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// scriptRow is one result row's column values in Scan order.
type scriptRow []any

// fakeStore is an in-memory StateStore for driving orchestrator logic tests.
type fakeStore struct {
	recs      []Record
	appendErr error
}

func (s *fakeStore) Ensure(context.Context) error { return nil }
func (s *fakeStore) Append(_ context.Context, r Record) error {
	if s.appendErr != nil {
		return s.appendErr
	}
	s.recs = append(s.recs, r)
	return nil
}
func (s *fakeStore) Records(_ context.Context, opID string) ([]Record, error) {
	var out []Record
	for _, r := range s.recs {
		if r.OpID == "" || r.OpID == opID {
			out = append(out, r)
		}
	}
	return out, nil
}
func (s *fakeStore) SpecHashSeen(_ context.Context, h string) (bool, error) {
	for _, r := range s.recs {
		if r.SpecHash == h {
			return true, nil
		}
	}
	return false, nil
}

// fakeConn is a programmable driver.Conn. Exec records statements; Query/QueryRow
// route on query substrings to canned rows so full orchestrator flows can run
// without a server.
type fakeConn struct {
	driver.Conn
	execs         []string
	execErr       error
	mvRaw         string      // create_table_query returned by FetchMV
	valMismatch   bool        // make the *_v2 validation value differ
	stateRows     []scriptRow // rows returned for a state-store Records query
	lockedSetting string      // a READONLY backfill setting name, if any
	hashSeenCount uint64      // count returned for SpecHashSeen
}

func (c *fakeConn) Exec(_ context.Context, query string, _ ...any) error {
	c.execs = append(c.execs, query)
	return c.execErr
}

func (c *fakeConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	rows, err := c.route(query)
	if err != nil {
		return nil, err
	}
	return &fakeRows{rows: rows, i: -1}, nil
}

func (c *fakeConn) QueryRow(_ context.Context, query string, _ ...any) driver.Row {
	rows, err := c.route(query)
	if err != nil {
		return &fakeRow{err: err}
	}
	if len(rows) == 0 {
		return &fakeRow{err: fmt.Errorf("no rows")}
	}
	return &fakeRow{vals: rows[0]}
}

func (c *fakeConn) route(q string) ([]scriptRow, error) {
	mv := c.mvRaw
	if mv == "" {
		mv = sampleMV
	}
	switch {
	case strings.Contains(q, "op_id, spec_hash"): // state-store Records
		return c.stateRows, nil
	case strings.Contains(q, "spec_hash = ?"): // state-store SpecHashSeen count
		return []scriptRow{{c.hashSeenCount}}, nil
	case strings.Contains(q, "system.tables") && strings.Contains(q, "create_table_query"):
		return []scriptRow{{mv}}, nil
	case strings.Contains(q, "asynchronous_metrics"):
		return []scriptRow{{uint64(16 << 30)}}, nil // 16 GiB RAM
	case strings.Contains(q, "system.settings"):
		if c.lockedSetting != "" {
			return []scriptRow{{c.lockedSetting}}, nil
		}
		return nil, nil // nothing READONLY
	case strings.Contains(q, "system.parts"):
		return []scriptRow{{uint64(123456), "1.20 GiB"}}, nil
	case strings.Contains(q, "version()"):
		return []scriptRow{{"24.3.1"}}, nil
	case strings.Contains(q, "DISTINCT"):
		return []scriptRow{{"2026-07-14"}, {"2026-07-13"}}, nil
	case strings.Contains(q, "count()"):
		return []scriptRow{{uint64(1000)}}, nil
	case strings.Contains(q, "toString("):
		v := "42"
		if c.valMismatch && strings.Contains(q, "_v2") {
			v = "43"
		}
		return []scriptRow{{v}}, nil
	}
	return nil, fmt.Errorf("fakeConn: unrouted query: %s", q)
}

// fakeRows implements driver.Rows over a fixed row set (unused methods come from
// the embedded nil interface and are never called by chtool).
type fakeRows struct {
	driver.Rows
	rows []scriptRow
	i    int
	err  error
}

func (r *fakeRows) Next() bool          { r.i++; return r.i < len(r.rows) }
func (r *fakeRows) Scan(d ...any) error { return assign(d, r.rows[r.i]) }
func (r *fakeRows) Close() error        { return nil }
func (r *fakeRows) Err() error          { return r.err }

type fakeRow struct {
	driver.Row
	vals scriptRow
	err  error
}

func (r *fakeRow) Scan(d ...any) error {
	if r.err != nil {
		return r.err
	}
	return assign(d, r.vals)
}
func (r *fakeRow) Err() error { return r.err }

func assign(dest []any, vals scriptRow) error {
	if len(dest) != len(vals) {
		return fmt.Errorf("scan mismatch: %d dest vs %d cols", len(dest), len(vals))
	}
	for i, d := range dest {
		switch p := d.(type) {
		case *string:
			*p = vals[i].(string)
		case *uint64:
			*p = vals[i].(uint64)
		default:
			return fmt.Errorf("assign: unsupported dest %T", d)
		}
	}
	return nil
}
