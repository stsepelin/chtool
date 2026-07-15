package schema

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type tableRow struct{ name, engine, ddl string }

type fakeConn struct {
	driver.Conn
	rows []tableRow
}

func (c *fakeConn) Query(_ context.Context, _ string, _ ...any) (driver.Rows, error) {
	return &fakeRows{rows: c.rows, i: -1}, nil
}

type fakeRows struct {
	driver.Rows
	rows []tableRow
	i    int
}

func (r *fakeRows) Next() bool { r.i++; return r.i < len(r.rows) }
func (r *fakeRows) Scan(dest ...any) error {
	row := r.rows[r.i]
	*dest[0].(*string) = row.name
	*dest[1].(*string) = row.engine
	*dest[2].(*string) = row.ddl
	return nil
}
func (r *fakeRows) Close() error { return nil }
func (r *fakeRows) Err() error   { return nil }

func TestDumpSortsTablesThenMVsAndExcludes(t *testing.T) {
	conn := &fakeConn{rows: []tableRow{
		{"z_table", "MergeTree", "CREATE TABLE d.z_table (x UInt8) ENGINE = MergeTree ORDER BY x"},
		{"a_mv", "MaterializedView", "CREATE MATERIALIZED VIEW d.a_mv TO d.z_table AS SELECT x FROM d.src"},
		{"a_table", "MergeTree", "CREATE TABLE d.a_table (x UInt8) ENGINE = MergeTree ORDER BY x"},
		{"skip_me", "MergeTree", "CREATE TABLE d.skip_me (x UInt8) ENGINE = MergeTree ORDER BY x"},
	}}
	objs, err := Dump(context.Background(), conn, "d", "skip_me")
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 3 {
		t.Fatalf("expected 3 objects after exclude, got %d", len(objs))
	}
	// Tables first (sorted), then MVs.
	if objs[0].Name != "a_table" || objs[1].Name != "z_table" || !objs[2].IsMV || objs[2].Name != "a_mv" {
		t.Fatalf("sort order wrong: %+v", objs)
	}
}

func TestIssueString(t *testing.T) {
	i := Issue{File: "000001_x.up.sql", Msg: "contains POPULATE"}
	if got := i.String(); got != "000001_x.up.sql: contains POPULATE" {
		t.Fatalf("Issue.String = %q", got)
	}
}

func TestNormalizePreservesMVSelectTail(t *testing.T) {
	in := "CREATE MATERIALIZED VIEW d.v TO d.t\n(x UInt8)  AS   SELECT   x,   y\nFROM   d.src   GROUP BY x"
	out := Normalize(in)
	if !contains(out, "AS SELECT x, y FROM d.src GROUP BY x") {
		t.Fatalf("MV tail not whitespace-collapsed as expected: %q", out)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
