package rebuild

import (
	"context"
	"strings"
	"testing"
)

// UseExistingTable must suppress all DDL: the embedder owns the table, so
// whichever of Ensure or their migration runs first must no longer matter.
func TestSQLStoreUseExistingTableRunsNoDDL(t *testing.T) {
	conn := &fakeConn{}
	s := NewSQLStore(conn, "audit.ops").UseExistingTable()
	ctx := context.Background()

	if err := s.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(ctx, Record{OpID: "x", Phase: phaseCreated}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Records(ctx, "x"); err != nil {
		t.Fatal(err)
	}
	if n := countContains(conn.execs, "CREATE TABLE"); n != 0 {
		t.Fatalf("caller-owned table must never get DDL, got %d CREATE statements: %v", n, conn.execs)
	}
	// It must still write, and still send the seq tiebreaker.
	if n := countContains(conn.execs, "INSERT INTO audit.ops"); n != 1 {
		t.Fatalf("expected one INSERT into the caller's table, got %v", conn.execs)
	}
	if !strings.Contains(conn.execs[0], "seq") {
		t.Fatalf("INSERT must carry the seq tiebreaker: %s", conn.execs[0])
	}
}

// A caller-owned table is verified once, up front, so a wrong or absent table
// is named here rather than surfacing as a raw INSERT error mid-rebuild.
func TestUseExistingTableVerification(t *testing.T) {
	valid := validStateColumns()
	without := func(col string) []scriptRow {
		var out []scriptRow
		for _, r := range valid {
			if r[0] != col {
				out = append(out, r)
			}
		}
		return out
	}
	replace := func(col string, row scriptRow) []scriptRow {
		out := append([]scriptRow(nil), valid...)
		for i := range out {
			if out[i][0] == col {
				out[i] = row
			}
		}
		return out
	}

	cases := []struct {
		name    string
		columns []scriptRow
		wantErr []string // substrings the message must carry
	}{
		{
			name:    "table does not exist",
			columns: []scriptRow{},
			wantErr: []string{"audit.ops", "does not exist", "UseExistingTable"},
		},
		{
			name:    "missing the seq tiebreaker",
			columns: without("seq"),
			wantErr: []string{"audit.ops", "seq", "missing column"},
		},
		{
			name:    "missing a payload column",
			columns: without("spec_hash"),
			wantErr: []string{"spec_hash", "missing column"},
		},
		{
			name:    "ts is not server-stamped",
			columns: replace("ts", scriptRow{"ts", "DateTime64(3)", ""}),
			wantErr: []string{"DEFAULT now64(3)", "found none"},
		},
		{
			name:    "ts is too coarse to order by",
			columns: replace("ts", scriptRow{"ts", "DateTime", "now64(3)"}),
			wantErr: []string{"DateTime64(3) or finer", "ordered by"},
		},
		{
			// now() is second-precision even in a DateTime64(3) column, so it
			// silently collapses same-second appends onto one tick.
			name:    "ts defaults to now() rather than now64()",
			columns: replace("ts", scriptRow{"ts", "DateTime64(3)", "now()"}),
			wantErr: []string{"DEFAULT now64(3)", "whole-second"},
		},
		{
			name:    "ts precision is coarser than milliseconds",
			columns: replace("ts", scriptRow{"ts", "DateTime64(0)", "now64(0)"}),
			wantErr: []string{"DateTime64(3) or finer"},
		},
		{
			// now64(0) is whole seconds too, in a column that looks right.
			name:    "ts default is now64 at second scale",
			columns: replace("ts", scriptRow{"ts", "DateTime64(3)", "now64(0)"}),
			wantErr: []string{"now64(3) or finer", "now64(0)"},
		},
		{
			// A String seq sorts lexicographically: the tenth append would come
			// back before the second.
			name:    "seq cannot sort numerically",
			columns: replace("seq", scriptRow{"seq", "String", ""}),
			wantErr: []string{"seq of type String", "UInt64", "lexicographically"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := NewSQLStore(&fakeConn{columns: c.columns}, "audit.ops").UseExistingTable()
			err := s.Ensure(context.Background())
			if err == nil {
				t.Fatal("expected verification to fail")
			}
			for _, want := range c.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error should mention %q, got: %v", want, err)
				}
			}
		})
	}
}

// A conforming table passes, and the check is cached rather than repeated on
// every append.
func TestUseExistingTableVerifiesOnceAndCaches(t *testing.T) {
	conn := &fakeConn{}
	s := NewSQLStore(conn, "audit.ops").UseExistingTable()
	ctx := context.Background()

	for range 3 {
		if err := s.Append(ctx, Record{OpID: "x", Phase: phaseCreated}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Records(ctx, "x"); err != nil {
		t.Fatal(err)
	}
	if conn.queries != 1 {
		t.Fatalf("verification should run once, ran %d times", conn.queries)
	}
	if countContains(conn.execs, "CREATE TABLE") != 0 {
		t.Fatalf("a caller-owned table must never get DDL: %v", conn.execs)
	}
}

// A bare (unqualified) table is looked up in the connection's current database.
func TestVerifyUsesCurrentDatabaseForBareTable(t *testing.T) {
	conn := &fakeConn{}
	if err := NewSQLStore(conn, "ops").UseExistingTable().Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(conn.lastQuery, "currentDatabase()") {
		t.Fatalf("a bare table should resolve via currentDatabase(), got: %s", conn.lastQuery)
	}
}

// A table that exceeds the contract must not be rejected: finer ts precision
// and a wider integer seq both order correctly.
func TestUseExistingTableAcceptsStricterTypes(t *testing.T) {
	cases := map[string]scriptRow{
		"finer ts precision":      {"ts", "DateTime64(9)", "now64(9)"},
		"ts with a timezone":      {"ts", "DateTime64(3, 'UTC')", "now64(3)"},
		"default with a timezone": {"ts", "DateTime64(3)", "now64(3, 'UTC')"},
		"bare now64() is scale 3": {"ts", "DateTime64(3)", "now64()"},
		"wider integer seq":       {"seq", "Int64", ""},
	}
	for name, override := range cases {
		t.Run(name, func(t *testing.T) {
			cols := append([]scriptRow(nil), validStateColumns()...)
			for i := range cols {
				if cols[i][0] == override[0] {
					cols[i] = override
				}
			}
			s := NewSQLStore(&fakeConn{columns: cols}, "audit.ops").UseExistingTable()
			if err := s.Ensure(context.Background()); err != nil {
				t.Fatalf("a table stricter than the contract should pass: %v", err)
			}
		})
	}
}

func TestDateTime64Precision(t *testing.T) {
	cases := []struct {
		typ  string
		prec int
		ok   bool
	}{
		{"DateTime64(3)", 3, true},
		{"DateTime64(9)", 9, true},
		{"DateTime64(0)", 0, true},
		{"DateTime64(3, 'UTC')", 3, true},
		{"DateTime", 0, false},
		{"String", 0, false},
		{"DateTime64()", 0, false},
	}
	for _, c := range cases {
		prec, ok := dateTime64Precision(c.typ)
		if ok != c.ok || (ok && prec != c.prec) {
			t.Errorf("dateTime64Precision(%q) = (%d,%v), want (%d,%v)", c.typ, prec, ok, c.prec, c.ok)
		}
	}
}

func TestNow64Scale(t *testing.T) {
	cases := []struct {
		expr  string
		scale int
		ok    bool
	}{
		{"now64(3)", 3, true},
		{"now64(9)", 9, true},
		{"now64(0)", 0, true}, // parses, but the caller rejects scale < 3
		{"now64()", 3, true},  // the function's own default
		{"now64(3, 'UTC')", 3, true},
		{"NOW64(3)", 3, true},
		{" now64(3) ", 3, true},
		{"now()", 0, false},
		{"", 0, false},
		{"toDateTime64(now(), 3)", 0, false},
		{"now64", 0, false},
	}
	for _, c := range cases {
		scale, ok := now64Scale(c.expr)
		if ok != c.ok || (ok && scale != c.scale) {
			t.Errorf("now64Scale(%q) = (%d,%v), want (%d,%v)", c.expr, scale, ok, c.scale, c.ok)
		}
	}
}

func TestSplitTable(t *testing.T) {
	cases := map[string][2]string{
		"ops":                {"", "ops"},
		"analytics.ops":      {"analytics", "ops"},
		"`analytics`.`ops`":  {"analytics", "ops"},
		" analytics . ops  ": {"analytics", "ops"},
	}
	for in, want := range cases {
		db, table := splitTable(in)
		if db != want[0] || table != want[1] {
			t.Errorf("splitTable(%q) = (%q,%q), want (%q,%q)", in, db, table, want[0], want[1])
		}
	}
}

// The default (unowned) store still creates its table.
func TestSQLStoreDefaultStillEnsures(t *testing.T) {
	conn := &fakeConn{}
	if err := NewSQLStore(conn, "ops").Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	if countContains(conn.execs, "CREATE TABLE IF NOT EXISTS") != 1 {
		t.Fatalf("default store should create its table, got %v", conn.execs)
	}
}

// RequiredColumns must stay in sync with what Append writes and Records reads —
// it is the contract a caller-owned superset table has to satisfy.
func TestRequiredColumnsMatchQueries(t *testing.T) {
	conn := &fakeConn{}
	s := NewSQLStore(conn, "ops").UseExistingTable()
	if err := s.Append(context.Background(), Record{OpID: "x"}); err != nil {
		t.Fatal(err)
	}
	insert := conn.execs[0]
	for _, c := range RequiredColumns {
		if c == "ts" {
			continue // server-stamped via DEFAULT, deliberately not in the INSERT
		}
		if !strings.Contains(insert, c) {
			t.Errorf("RequiredColumns lists %q but the INSERT does not write it: %s", c, insert)
		}
	}
}

// BUG 1 regression: Append must not bind ts, and the DDL must server-stamp it
// via DEFAULT now64(3). Binding a Go time.Now() truncates to whole seconds and
// collapses distinct phase records onto one tick, so the orchestrator's
// "latest phase = records[len-1]" could read a non-terminal phase after a resume.
func TestSQLStoreAppendServerStampsTs(t *testing.T) {
	conn := &fakeConn{}
	s := NewSQLStore(conn, "ops")
	if err := s.Append(context.Background(), Record{OpID: "x", Phase: phaseValidated}); err != nil {
		t.Fatal(err)
	}

	var ddl, insert string
	for _, q := range conn.execs {
		switch {
		case strings.Contains(q, "CREATE TABLE IF NOT EXISTS"):
			ddl = q
		case strings.HasPrefix(strings.TrimSpace(q), "INSERT"):
			insert = q
		}
	}
	if !strings.Contains(ddl, "DEFAULT now64(3)") {
		t.Fatalf("ts must be server-stamped via DEFAULT now64(3):\n%s", ddl)
	}
	cols := insert[strings.Index(insert, "(")+1 : strings.Index(insert, ")")]
	if strings.Contains(cols, "ts") {
		t.Fatalf("INSERT must not bind ts (server-stamps it): %s", insert)
	}
	if !strings.Contains(cols, "seq") {
		t.Fatalf("INSERT should still carry the seq tiebreaker: %s", insert)
	}
}

func TestSQLStoreRoundTrip(t *testing.T) {
	conn := &fakeConn{
		stateRows: []scriptRow{
			{"rebuild:x", "HASH", phaseCreated, "done", "", ""},
			{"rebuild:x", "HASH", phaseBackfill, "done", "c1", "~10 rows"},
		},
		hashSeenCount: 2,
	}
	s := NewSQLStore(conn, "")
	if s.table != DefaultTable {
		t.Fatalf("empty table should default to %s, got %s", DefaultTable, s.table)
	}

	if err := s.Append(context.Background(), Record{OpID: "rebuild:x", Phase: phaseCreated}); err != nil {
		t.Fatal(err)
	}
	// Ensure runs at most once: Append + Records + SpecHashSeen share one CREATE.
	if got := countContains(conn.execs, "CREATE TABLE IF NOT EXISTS"); got != 1 {
		t.Fatalf("Ensure DDL should run once, ran %d times", got)
	}

	recs, err := s.Records(context.Background(), "rebuild:x")
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 || recs[1].Cursor != "c1" {
		t.Fatalf("Records round-trip wrong: %+v", recs)
	}

	seen, err := s.SpecHashSeen(context.Background(), "HASH")
	if err != nil {
		t.Fatal(err)
	}
	if !seen {
		t.Error("SpecHashSeen should report a known hash as seen")
	}
}

func TestSQLStoreNamedTable(t *testing.T) {
	s := NewSQLStore(&fakeConn{}, "analytics._ops")
	if s.table != "analytics._ops" {
		t.Fatalf("explicit table not honored: %s", s.table)
	}
}
