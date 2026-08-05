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
