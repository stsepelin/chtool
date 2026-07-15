//go:build integration

// Integration tests run against a real ClickHouse:
//
//	go test -tags integration ./rebuild/
//
// Server from CHTOOL_TEST_DSN (default clickhouse://localhost:9000/default).
// The main test drives a full online rebuild (create → arm → lag-drain →
// backfill → validate → cutover) end-to-end and checks the aggregate survives.
package rebuild

import (
	"context"
	"net/url"
	"os"
	"testing"
	"time"

	chtool "github.com/stsepelin/chtool"
)

func baseDSN() string {
	if d := os.Getenv("CHTOOL_TEST_DSN"); d != "" {
		return d
	}
	return "clickhouse://localhost:9000/default"
}

// scratchConn creates a fresh database and returns a connection whose DEFAULT
// database is that scratch DB — the orchestrator runs the operator's new_ddl
// verbatim (only the table name is retargeted, not the database), so it must
// land in the target database.
func scratchConn(t *testing.T, db string) (Conn, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := chtool.Open(ctx, baseDSN())
	if err != nil {
		t.Skipf("no ClickHouse at %s: %v", baseDSN(), err)
	}
	for _, q := range []string{"DROP DATABASE IF EXISTS " + db, "CREATE DATABASE " + db} {
		if err := admin.Exec(ctx, q); err != nil {
			admin.Close()
			t.Fatalf("%s: %v", q, err)
		}
	}
	u, err := url.Parse(baseDSN())
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	u.Path = "/" + db
	conn, err := chtool.Open(ctx, u.String())
	if err != nil {
		admin.Close()
		t.Fatalf("open scratch conn: %v", err)
	}
	return conn, func() {
		conn.Close()
		_ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db)
		admin.Close()
	}
}

func scalarU64(t *testing.T, conn Conn, q string) uint64 {
	t.Helper()
	var v uint64
	if err := conn.QueryRow(context.Background(), q).Scan(&v); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return v
}

func TestIntegrationStateStore(t *testing.T) {
	const db = "chtool_it_state"
	conn, cleanup := scratchConn(t, db)
	defer cleanup()
	ctx := context.Background()

	store := NewSQLStore(conn, db+"._chtool_ops")
	if err := store.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	recs := []Record{
		{OpID: "rebuild:x", SpecHash: "H1", Phase: phaseCreated, Status: "done"},
		{OpID: "rebuild:x", SpecHash: "H1", Phase: phaseBackfill, Status: "done", Cursor: "2026-07-15|mv|N1|b0", Detail: "~5 rows"},
		{OpID: "rebuild:y", SpecHash: "H2", Phase: phaseCreated, Status: "done"},
	}
	for _, r := range recs {
		if err := store.Append(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	got, err := store.Records(ctx, "rebuild:x")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[1].Cursor != "2026-07-15|mv|N1|b0" {
		t.Fatalf("Records(x) wrong: %+v", got)
	}
	if seen, _ := store.SpecHashSeen(ctx, "H1"); !seen {
		t.Error("H1 should be seen")
	}
	if seen, _ := store.SpecHashSeen(ctx, "nope"); seen {
		t.Error("unknown hash should not be seen")
	}
}

func TestIntegrationFullRebuildAndCutover(t *testing.T) {
	const db = "chtool_it_rebuild"
	conn, cleanup := scratchConn(t, db)
	defer cleanup()
	ctx := context.Background()

	setup := []string{
		"CREATE TABLE " + db + ".events (created_at DateTime, date Date, country String, hits UInt64) ENGINE = MergeTree ORDER BY created_at",
		"CREATE TABLE " + db + ".events_daily (date Date, country String, hits SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY (date, country)",
		"CREATE MATERIALIZED VIEW " + db + ".events_daily_mv TO " + db + ".events_daily (date Date, country String, hits UInt64) AS SELECT date, country, sum(hits) AS hits FROM " + db + ".events GROUP BY date, country",
	}
	for _, s := range setup {
		if err := conn.Exec(ctx, s); err != nil {
			t.Fatalf("setup %s: %v", s, err)
		}
	}

	// Historical (pre-boundary) data: the original MV populates events_daily.
	if err := conn.Exec(ctx,
		"INSERT INTO "+db+".events SELECT now() - toIntervalHour(1), today(), ['US','DE','FR'][(number%3)+1], number%7 FROM numbers(3000)"); err != nil {
		t.Fatalf("seed events: %v", err)
	}
	want := scalarU64(t, conn, "SELECT sum(hits) FROM "+db+".events_daily")
	if want == 0 {
		t.Fatal("seed produced no aggregated hits")
	}

	// Rebuild spec: same columns, ORDER BY reordered (country, date).
	spec := &Spec{
		Name:           "it-reorder",
		TargetTable:    "events_daily",
		BoundaryColumn: "created_at",
		ChunkColumn:    "date",
		MVs:            []string{"events_daily_mv"},
		Validations:    []string{"sum(hits)"},
	}
	spec.SetNewDDL("CREATE TABLE events_daily (date Date, country String, hits SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY (country, date)")

	store := NewSQLStore(conn, db+"._chtool_ops")
	if err := store.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	o := &Orchestrator{
		Conn: conn, DB: db, Spec: spec, Store: store,
		Log: func(f string, a ...any) { t.Logf(f, a...) },
	}

	if err := o.Run(ctx, Options{BoundaryOffset: 2 * time.Second, LagPoll: 500 * time.Millisecond}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// v2 must exist and its aggregate must match the original exactly.
	gotV2 := scalarU64(t, conn, "SELECT sum(hits) FROM "+db+".events_daily_v2")
	if gotV2 != want {
		t.Fatalf("v2 aggregate %d != original %d", gotV2, want)
	}
	last, _ := store.Records(ctx, spec.OpID())
	if last[len(last)-1].Phase != phaseValidated {
		t.Fatalf("expected validated phase, got %q", last[len(last)-1].Phase)
	}

	// Cutover swaps v2 into place.
	if err := Cutover(ctx, o, time.Now().UTC()); err != nil {
		t.Fatalf("Cutover: %v", err)
	}
	if got := scalarU64(t, conn, "SELECT sum(hits) FROM "+db+".events_daily"); got != want {
		t.Fatalf("post-cutover aggregate %d != %d", got, want)
	}
	// The new ORDER BY key must be live on the swapped-in table.
	var sortKey string
	if err := conn.QueryRow(ctx,
		"SELECT sorting_key FROM system.tables WHERE database = ? AND name = 'events_daily'", db).Scan(&sortKey); err != nil {
		t.Fatal(err)
	}
	if sortKey != "country, date" {
		t.Fatalf("expected reordered sorting_key 'country, date', got %q", sortKey)
	}
	// v2 name is gone; a dated backup remains.
	if n := scalarU64(t, conn,
		"SELECT count() FROM system.tables WHERE database = '"+db+"' AND name = 'events_daily_v2'"); n != 0 {
		t.Fatal("events_daily_v2 should no longer exist after cutover")
	}
}

func TestIntegrationAbortLeavesNoV2(t *testing.T) {
	const db = "chtool_it_abort"
	conn, cleanup := scratchConn(t, db)
	defer cleanup()
	ctx := context.Background()

	setup := []string{
		"CREATE TABLE " + db + ".events (created_at DateTime, date Date, country String, hits UInt64) ENGINE = MergeTree ORDER BY created_at",
		"CREATE TABLE " + db + ".events_daily (date Date, country String, hits SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY (date, country)",
		"CREATE MATERIALIZED VIEW " + db + ".events_daily_mv TO " + db + ".events_daily (date Date, country String, hits UInt64) AS SELECT date, country, sum(hits) AS hits FROM " + db + ".events GROUP BY date, country",
	}
	for _, s := range setup {
		if err := conn.Exec(ctx, s); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}
	spec := &Spec{
		Name: "it-abort", TargetTable: "events_daily", BoundaryColumn: "created_at",
		ChunkColumn: "date", MVs: []string{"events_daily_mv"}, Validations: []string{"sum(hits)"},
	}
	spec.SetNewDDL("CREATE TABLE events_daily (date Date, country String, hits SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY (country, date)")
	o := &Orchestrator{Conn: conn, DB: db, Spec: spec, Store: NewSQLStore(conn, db+"._chtool_ops")}

	// Run creates v2 + arms the v2 MV, then blocks waiting for the far-future
	// boundary. Cancel it mid-wait: the v2 objects now exist and can be aborted.
	runCtx, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
	defer cancel()
	if err := o.Run(runCtx, Options{BoundaryOffset: 30 * time.Second, LagPoll: 100 * time.Millisecond}); err == nil {
		t.Skip("Run completed before the boundary wait; skipping abort assertion")
	}
	// v2 table should have been created before the wait.
	if err := Abort(ctx, o); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	for _, name := range []string{"events_daily_v2", "events_daily_mv_v2"} {
		if n := scalarU64(t, conn, "SELECT count() FROM system.tables WHERE database = '"+db+"' AND name = '"+name+"'"); n != 0 {
			t.Errorf("%s should not exist after abort", name)
		}
	}
	// The live pipeline must be intact: inserts still flow through the original MV.
	if err := conn.Exec(ctx, "INSERT INTO "+db+".events VALUES (now(), today(), 'US', 5)"); err != nil {
		t.Fatalf("live insert after abort failed — pipeline broken: %v", err)
	}
}
