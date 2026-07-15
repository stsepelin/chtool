package rebuild

import (
	"context"
	"testing"
)

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
