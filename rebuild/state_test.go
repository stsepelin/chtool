package rebuild

import (
	"context"
	"strings"
	"testing"
)

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
