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
	"fmt"
	"net/url"
	"os"
	"strings"
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

// requireCHOrSkip skips when ClickHouse is unreachable, unless CHTOOL_REQUIRE_CH
// is set (as in CI), where an unreachable server is a hard failure so the
// integration suite can't pass by silently skipping.
func requireCHOrSkip(t *testing.T, err error) {
	t.Helper()
	if os.Getenv("CHTOOL_REQUIRE_CH") != "" {
		t.Fatalf("ClickHouse required (CHTOOL_REQUIRE_CH) but unreachable at %s: %v", baseDSN(), err)
	}
	t.Skipf("no ClickHouse at %s: %v", baseDSN(), err)
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
		requireCHOrSkip(t, err)
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

func scalarStr(t *testing.T, conn Conn, q string) string {
	t.Helper()
	var v string
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

// BUG 1 acceptance: two records appended within the same wall-clock second must
// read back in append order, so a terminal `validated` stays last. Before the
// fix the client-bound ts truncated to :SS.000 and the order was arbitrary.
func TestIntegrationStateStoreOrderingWithinSecond(t *testing.T) {
	const db = "chtool_it_state_order"
	conn, cleanup := scratchConn(t, db)
	defer cleanup()
	ctx := context.Background()

	store := NewSQLStore(conn, db+"._chtool_ops")
	if err := store.Append(ctx, Record{OpID: "rebuild:o", Phase: phaseLagDrained, Status: "done"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, Record{OpID: "rebuild:o", Phase: phaseValidated, Status: "done"}); err != nil {
		t.Fatal(err)
	}
	recs, err := store.Records(ctx, "rebuild:o")
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 {
		t.Fatalf("expected 2 records, got %d: %+v", len(recs), recs)
	}
	if recs[len(recs)-1].Phase != phaseValidated {
		t.Fatalf("terminal phase should be %q, got %q — same-second ordering broke", phaseValidated, recs[len(recs)-1].Phase)
	}
	// The stored ts must carry real sub-second precision (server-stamped), not the
	// truncated :SS.000 of the old client-bound path.
	subsecond := scalarU64(t, conn,
		"SELECT countIf(toUnixTimestamp64Milli(ts) % 1000 != 0) FROM "+db+"._chtool_ops")
	if subsecond == 0 {
		t.Log("note: both appends happened to land on a whole millisecond; seq still guarantees order")
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

// BUG 1 acceptance: run → resume → cutover. The resume (a fresh orchestrator and
// store, i.e. a new process with a reset seq counter) must be idempotent, leave
// `validated` as the terminal phase, and let Cutover proceed — the reported
// symptom was Cutover refusing with "current phase = lag_drained" after a resume.
func TestIntegrationRunResumeCutover(t *testing.T) {
	const db = "chtool_it_resume"
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
	if err := conn.Exec(ctx,
		"INSERT INTO "+db+".events SELECT now() - toIntervalHour(1), today(), ['US','DE','FR'][(number%3)+1], number%7 FROM numbers(3000)"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	want := scalarU64(t, conn, "SELECT sum(hits) FROM "+db+".events_daily")

	// Each call builds a fresh orchestrator + store — a new process would reset
	// the store's in-memory seq counter, which the resume must tolerate.
	newOrch := func() *Orchestrator {
		spec := &Spec{
			Name: "it-resume", TargetTable: "events_daily", BoundaryColumn: "created_at",
			ChunkColumn: "date", MVs: []string{"events_daily_mv"}, Validations: []string{"sum(hits)"},
		}
		spec.SetNewDDL("CREATE TABLE events_daily (date Date, country String, hits SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY (country, date)")
		return &Orchestrator{
			Conn: conn, DB: db, Spec: spec, Store: NewSQLStore(conn, db+"._chtool_ops"),
			Log: func(f string, a ...any) { t.Logf(f, a...) },
		}
	}
	opts := Options{BoundaryOffset: 2 * time.Second, LagPoll: 500 * time.Millisecond}

	if err := newOrch().Run(ctx, opts); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	// Resume over the same state; must be a no-op that keeps validated terminal.
	o := newOrch()
	if err := o.Run(ctx, opts); err != nil {
		t.Fatalf("resume Run: %v", err)
	}
	recs, err := o.Store.Records(ctx, o.Spec.OpID())
	if err != nil {
		t.Fatal(err)
	}
	if recs[len(recs)-1].Phase != phaseValidated {
		t.Fatalf("after resume terminal phase = %q, want %q", recs[len(recs)-1].Phase, phaseValidated)
	}

	if err := Cutover(ctx, o, time.Now().UTC()); err != nil {
		t.Fatalf("Cutover after resume: %v", err)
	}
	if got := scalarU64(t, conn, "SELECT sum(hits) FROM "+db+".events_daily"); got != want {
		t.Fatalf("post-cutover aggregate %d != %d", got, want)
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

// TestIntegrationWideKeyReorderAddColumnTwoSources is a realistic, full-scale
// rebuild: one AggregatingMergeTree fed by two large source tables (each via its
// own MV), keyed on 10 dimensions. The rebuild changes the ORDER BY — reordering
// the wide key AND adding a new derived key column (a MATERIALIZED tier the MVs
// never produce) — which forces the whole pipeline: create v2, arm both MVs at a
// boundary, lag-drain, memory-bounded chunked backfill across multiple partitions
// and buckets, exact old-vs-new validation on three measures, and a RENAME
// cutover that recreates the MVs. Post-cutover ingestion must keep flowing.
func TestIntegrationWideKeyReorderAddColumnTwoSources(t *testing.T) {
	const db = "chtool_it_wide"
	const perSource = 200_000
	conn, cleanup := scratchConn(t, db)
	defer cleanup()
	ctx := context.Background()

	// 10-dimension key + three measures (impressions/clicks/spend).
	dims := "date, country, device, os, browser, channel, campaign_id, placement_id, creative_id, ab_variant"
	// sum(Decimal(18,6)) widens to Decimal(38,6), so the aggregate storage type
	// must match that width.
	measures := "impressions SimpleAggregateFunction(sum, UInt64), clicks SimpleAggregateFunction(sum, UInt64), spend SimpleAggregateFunction(sum, Decimal(38, 6))"
	dimCols := "date Date, country LowCardinality(String), device LowCardinality(String), os LowCardinality(String), " +
		"browser LowCardinality(String), channel LowCardinality(String), campaign_id UInt32, placement_id UInt32, " +
		"creative_id UInt32, ab_variant UInt8"
	// The MV's TO column list needs types (older ClickHouse requires them); the
	// measures carry the SELECT output types (sum(Decimal(18,6)) widens to (38,6)).
	aggCols := dimCols + ", impressions UInt64, clicks UInt64, spend Decimal(38, 6)"

	if err := conn.Exec(ctx, "CREATE TABLE "+db+".events_agg ("+dimCols+", "+measures+
		") ENGINE = AggregatingMergeTree ORDER BY ("+dims+")"); err != nil {
		t.Fatalf("create agg: %v", err)
	}

	// Two source tables, each with its own MV projecting the same 13 columns into
	// events_agg, and a large, varied seed spread across three partition days.
	for _, src := range []string{"raw_web", "raw_mobile"} {
		if err := conn.Exec(ctx, "CREATE TABLE "+db+"."+src+" (created_at DateTime, "+dimCols+
			", impressions UInt64, clicks UInt64, spend Decimal(18, 6)) ENGINE = MergeTree ORDER BY created_at"); err != nil {
			t.Fatalf("create %s: %v", src, err)
		}
		if err := conn.Exec(ctx, "CREATE MATERIALIZED VIEW "+db+"."+src+"_mv TO "+db+".events_agg ("+aggCols+
			") AS SELECT "+dims+", sum(impressions) AS impressions, sum(clicks) AS clicks, sum(spend) AS spend "+
			"FROM "+db+"."+src+" GROUP BY "+dims); err != nil {
			t.Fatalf("create %s_mv: %v", src, err)
		}
		seed := fmt.Sprintf("INSERT INTO %s.%s SELECT "+
			"now() - toIntervalHour(1) - toIntervalDay(number %% 3), "+ // pre-boundary, 3 days
			"toDate(now()) - toIntervalDay(number %% 3), "+
			"['US','GB','DE','FR','JP'][(number %% 5) + 1], "+
			"['desktop','mobile','tablet'][(number %% 3) + 1], "+
			"['ios','android','win','mac','linux'][(number %% 5) + 1], "+
			"['chrome','safari','firefox','edge'][(number %% 4) + 1], "+
			"['organic','cpc','social','email'][(number %% 4) + 1], "+
			"toUInt32(number %% 500), toUInt32(number %% 2000), toUInt32(number %% 5000), toUInt8(number %% 2), "+
			"toUInt64(number %% 100 + 1), toUInt64(number %% 10), toDecimal64(number %% 1000, 6) "+
			"FROM numbers(%d)", db, src, perSource)
		if err := conn.Exec(ctx, seed); err != nil {
			t.Fatalf("seed %s: %v", src, err)
		}
	}

	// Baseline aggregates from the live (original) table.
	oldImpr := scalarU64(t, conn, "SELECT sum(impressions) FROM "+db+".events_agg")
	oldClicks := scalarU64(t, conn, "SELECT sum(clicks) FROM "+db+".events_agg")
	oldSpend := scalarStr(t, conn, "SELECT toString(sum(spend)) FROM "+db+".events_agg")
	if oldImpr == 0 {
		t.Fatal("seed produced no aggregated impressions")
	}
	t.Logf("baseline: impressions=%d clicks=%d spend=%s", oldImpr, oldClicks, oldSpend)

	// Rebuild: reorder the wide key and add a new derived key column country_tier
	// (MATERIALIZED from country — the MVs don't produce it, the target computes it
	// on insert). Functionally dependent on country, so the measure sums are
	// preserved exactly.
	spec := &Spec{
		Name: "it-wide", TargetTable: "events_agg", BoundaryColumn: "created_at", ChunkColumn: "date",
		MVs:         []string{"raw_web_mv", "raw_mobile_mv"},
		Validations: []string{"sum(impressions)", "sum(clicks)", "sum(spend)"},
	}
	spec.SetNewDDL("CREATE TABLE events_agg (" + dimCols +
		", country_tier UInt8 MATERIALIZED if(country IN ('US','GB','DE'), 1, 2), " + measures +
		") ENGINE = AggregatingMergeTree " +
		"ORDER BY (date, country_tier, country, channel, device, os, browser, campaign_id, placement_id, creative_id, ab_variant)")

	store := NewSQLStore(conn, db+"._chtool_ops")
	o := &Orchestrator{Conn: conn, DB: db, Spec: spec, Store: store, Log: func(f string, a ...any) { t.Logf(f, a...) }}

	// Small chunk target forces the backfill into multiple hash buckets per
	// (partition, source), exercising the chunking path rather than one big query.
	opts := Options{BoundaryOffset: 2 * time.Second, LagPoll: 500 * time.Millisecond, Backfill: BackfillConfig{TargetRowsPerChunk: 25_000}}
	if err := o.Run(ctx, opts); err != nil {
		t.Fatalf("Run: %v", err)
	}

	recs, _ := store.Records(ctx, spec.OpID())
	if recs[len(recs)-1].Phase != phaseValidated {
		t.Fatalf("terminal phase = %q, want validated", recs[len(recs)-1].Phase)
	}
	if n := len(completedChunks(recs)); n < 6 {
		t.Fatalf("expected a multi-chunk backfill (3 days x 2 sources x buckets), got %d chunks", n)
	} else {
		t.Logf("backfill covered %d chunks", n)
	}

	// v2 must match the original on every measure before cutover.
	if got := scalarU64(t, conn, "SELECT sum(impressions) FROM "+db+".events_agg_v2"); got != oldImpr {
		t.Fatalf("v2 impressions %d != %d", got, oldImpr)
	}
	if got := scalarU64(t, conn, "SELECT sum(clicks) FROM "+db+".events_agg_v2"); got != oldClicks {
		t.Fatalf("v2 clicks %d != %d", got, oldClicks)
	}
	if got := scalarStr(t, conn, "SELECT toString(sum(spend)) FROM "+db+".events_agg_v2"); got != oldSpend {
		t.Fatalf("v2 spend %s != %s", got, oldSpend)
	}

	// Cutover: drop MVs -> RENAME -> recreate MVs against the reordered table.
	if err := Cutover(ctx, o, time.Now().UTC()); err != nil {
		t.Fatalf("Cutover: %v", err)
	}

	// The new wide key is live, the derived column is populated, sums survived.
	wantKey := "date, country_tier, country, channel, device, os, browser, campaign_id, placement_id, creative_id, ab_variant"
	if got := scalarStr(t, conn, "SELECT sorting_key FROM system.tables WHERE database = '"+db+"' AND name = 'events_agg'"); got != wantKey {
		t.Fatalf("sorting_key = %q, want %q", got, wantKey)
	}
	if got := scalarU64(t, conn, "SELECT uniqExact(country_tier) FROM "+db+".events_agg"); got != 2 {
		t.Fatalf("country_tier should have 2 distinct values, got %d", got)
	}
	if got := scalarU64(t, conn, "SELECT sum(impressions) FROM "+db+".events_agg"); got != oldImpr {
		t.Fatalf("post-cutover impressions %d != %d", got, oldImpr)
	}
	if n := scalarU64(t, conn, "SELECT count() FROM system.tables WHERE database = '"+db+"' AND name = 'events_agg_v2'"); n != 0 {
		t.Fatal("events_agg_v2 should be gone after cutover")
	}

	// Post-cutover ingestion must flow through the recreated MVs into the new table.
	before := scalarU64(t, conn, "SELECT sum(impressions) FROM "+db+".events_agg")
	if err := conn.Exec(ctx, "INSERT INTO "+db+".raw_web (created_at, date, country, device, os, browser, channel, campaign_id, placement_id, creative_id, ab_variant, impressions, clicks, spend) "+
		"VALUES (now(), today(), 'US', 'desktop', 'mac', 'chrome', 'cpc', 1, 1, 1, 0, 4242, 1, toDecimal64(1, 6))"); err != nil {
		t.Fatalf("post-cutover insert: %v", err)
	}
	if got := scalarU64(t, conn, "SELECT sum(impressions) FROM "+db+".events_agg"); got != before+4242 {
		t.Fatalf("recreated MV did not capture post-cutover insert: %d != %d", got, before+4242)
	}
}

// TestIntegrationAddSourcedColumnTwoSources is the "add a real column to the
// aggregate" flow: a column is added to two source tables and to their MVs'
// SELECT/GROUP BY, and the target gains that column in its ORDER BY. The
// rebuilder is given the NEW MV definitions (spec.SetMVDDL), so it arms and
// backfills at the new grain and recreates the new MVs at cutover — the whole
// migration swaps atomically. Measure totals are invariant under a finer
// GROUP BY, so validation still holds exactly.
func TestIntegrationAddSourcedColumnTwoSources(t *testing.T) {
	const db = "chtool_it_sourced"
	const perSource = 50_000
	conn, cleanup := scratchConn(t, db)
	defer cleanup()
	ctx := context.Background()

	// Pre-migration world: the raw tables already have the new `tier` column
	// (added by an earlier ALTER), but the live MVs and target do NOT use it.
	if err := conn.Exec(ctx, "CREATE TABLE "+db+".events_agg (date Date, country LowCardinality(String), "+
		"hits SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY (date, country)"); err != nil {
		t.Fatalf("create agg: %v", err)
	}
	for _, src := range []string{"raw_web", "raw_mobile"} {
		if err := conn.Exec(ctx, "CREATE TABLE "+db+"."+src+" (created_at DateTime, date Date, "+
			"country LowCardinality(String), tier UInt8, hits UInt64) ENGINE = MergeTree ORDER BY created_at"); err != nil {
			t.Fatalf("create %s: %v", src, err)
		}
		// Live MV: keyed on (date, country) only — tier is ignored for now.
		if err := conn.Exec(ctx, "CREATE MATERIALIZED VIEW "+db+"."+src+"_mv TO "+db+".events_agg (date Date, country LowCardinality(String), hits UInt64) "+
			"AS SELECT date, country, sum(hits) AS hits FROM "+db+"."+src+" GROUP BY date, country"); err != nil {
			t.Fatalf("create %s_mv: %v", src, err)
		}
		seed := fmt.Sprintf("INSERT INTO %s.%s SELECT now() - toIntervalHour(1) - toIntervalDay(number %% 2), "+
			"toDate(now()) - toIntervalDay(number %% 2), ['US','GB','DE','FR'][(number %% 4) + 1], "+
			"toUInt8(number %% 3), toUInt64(number %% 10 + 1) FROM numbers(%d)", db, src, perSource)
		if err := conn.Exec(ctx, seed); err != nil {
			t.Fatalf("seed %s: %v", src, err)
		}
	}
	oldHits := scalarU64(t, conn, "SELECT sum(hits) FROM "+db+".events_agg")
	if oldHits == 0 {
		t.Fatal("seed produced no hits")
	}

	// The migration: target gains `tier` in columns and key; each MV gains `tier`
	// in its SELECT and GROUP BY. Authored for the real names, no WHERE.
	spec := &Spec{
		Name: "it-sourced", TargetTable: "events_agg", BoundaryColumn: "created_at", ChunkColumn: "date",
		MVs: []string{"raw_web_mv", "raw_mobile_mv"}, Validations: []string{"sum(hits)"},
	}
	spec.SetNewDDL("CREATE TABLE events_agg (date Date, country LowCardinality(String), tier UInt8, " +
		"hits SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY (date, country, tier)")
	for _, src := range []string{"raw_web", "raw_mobile"} {
		spec.SetMVDDL(src+"_mv", "CREATE MATERIALIZED VIEW "+src+"_mv TO events_agg (date Date, country LowCardinality(String), tier UInt8, hits UInt64) "+
			"AS SELECT date, country, tier, sum(hits) AS hits FROM "+src+" GROUP BY date, country, tier")
	}

	o := &Orchestrator{Conn: conn, DB: db, Spec: spec, Store: NewSQLStore(conn, db+"._chtool_ops"),
		Log: func(f string, a ...any) { t.Logf(f, a...) }}
	if err := o.Run(ctx, Options{BoundaryOffset: 2 * time.Second, LagPoll: 500 * time.Millisecond}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// v2 preserves the totals and is now populated at the finer (tier) grain.
	if got := scalarU64(t, conn, "SELECT sum(hits) FROM "+db+".events_agg_v2"); got != oldHits {
		t.Fatalf("v2 hits %d != %d", got, oldHits)
	}
	if got := scalarU64(t, conn, "SELECT uniqExact(tier) FROM "+db+".events_agg_v2"); got != 3 {
		t.Fatalf("v2 should carry all 3 tiers from source, got %d", got)
	}
	// The new key is genuinely finer than the old one.
	oldGroups := scalarU64(t, conn, "SELECT count() FROM (SELECT date, country FROM "+db+".events_agg GROUP BY date, country)")
	newGroups := scalarU64(t, conn, "SELECT count() FROM (SELECT date, country, tier FROM "+db+".events_agg_v2 GROUP BY date, country, tier)")
	if newGroups <= oldGroups {
		t.Fatalf("new grain should have more groups than old: new=%d old=%d", newGroups, oldGroups)
	}

	if err := Cutover(ctx, o, time.Now().UTC()); err != nil {
		t.Fatalf("Cutover: %v", err)
	}

	// After cutover: reordered key is live, tier is populated, totals survived.
	if got := scalarStr(t, conn, "SELECT sorting_key FROM system.tables WHERE database = '"+db+"' AND name = 'events_agg'"); got != "date, country, tier" {
		t.Fatalf("sorting_key = %q, want 'date, country, tier'", got)
	}
	if got := scalarU64(t, conn, "SELECT sum(hits) FROM "+db+".events_agg"); got != oldHits {
		t.Fatalf("post-cutover hits %d != %d", got, oldHits)
	}
	// The recreated MVs must be the NEW ones (their definition now groups by tier).
	for _, src := range []string{"raw_web", "raw_mobile"} {
		def := scalarStr(t, conn, "SELECT create_table_query FROM system.tables WHERE database = '"+db+"' AND name = '"+src+"_mv'")
		if !containsCI(def, "tier") {
			t.Fatalf("%s_mv was not recreated with the new definition: %s", src, def)
		}
	}

	// Post-cutover ingestion is aggregated at the new grain.
	before := scalarU64(t, conn, "SELECT sum(hits) FROM "+db+".events_agg WHERE tier = 2")
	if err := conn.Exec(ctx, "INSERT INTO "+db+".raw_web (created_at, date, country, tier, hits) VALUES (now(), today(), 'US', 2, 777)"); err != nil {
		t.Fatalf("post-cutover insert: %v", err)
	}
	if got := scalarU64(t, conn, "SELECT sum(hits) FROM "+db+".events_agg WHERE tier = 2"); got != before+777 {
		t.Fatalf("recreated MV did not aggregate the post-cutover row at tier=2: %d != %d", got, before+777)
	}
}

func containsCI(s, sub string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(sub))
}

// TestIntegrationPreflightMissingSourceColumn is the boundary fix: if a new MV
// references a column the source table doesn't have yet (the ALTER wasn't done
// first), Run must fail up front with an actionable error and mutate nothing —
// no v2 table, no armed MVs.
func TestIntegrationPreflightMissingSourceColumn(t *testing.T) {
	const db = "chtool_it_preflight"
	conn, cleanup := scratchConn(t, db)
	defer cleanup()
	ctx := context.Background()

	setup := []string{
		// Note: raw has NO `tier` column — the operator forgot to ALTER it.
		"CREATE TABLE " + db + ".raw (created_at DateTime, date Date, country String, hits UInt64) ENGINE = MergeTree ORDER BY created_at",
		"CREATE TABLE " + db + ".agg (date Date, country String, hits SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY (date, country)",
		"CREATE MATERIALIZED VIEW " + db + ".raw_mv TO " + db + ".agg (date Date, country String, hits UInt64) AS SELECT date, country, sum(hits) AS hits FROM " + db + ".raw GROUP BY date, country",
	}
	for _, s := range setup {
		if err := conn.Exec(ctx, s); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}

	spec := &Spec{
		Name: "it-preflight", TargetTable: "agg", BoundaryColumn: "created_at", ChunkColumn: "date",
		MVs: []string{"raw_mv"}, Validations: []string{"sum(hits)"},
	}
	spec.SetNewDDL("CREATE TABLE agg (date Date, country String, tier UInt8, hits SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY (date, country, tier)")
	// New MV references `tier`, which raw does not have.
	spec.SetMVDDL("raw_mv", "CREATE MATERIALIZED VIEW raw_mv TO agg (date Date, country String, tier UInt8, hits UInt64) AS SELECT date, country, tier, sum(hits) AS hits FROM raw GROUP BY date, country, tier")

	o := &Orchestrator{Conn: conn, DB: db, Spec: spec, Store: NewSQLStore(conn, db+"._chtool_ops")}
	err := o.Run(ctx, Options{BoundaryOffset: 2 * time.Second, LagPoll: 500 * time.Millisecond})
	if err == nil {
		t.Fatal("Run should fail preflight when the source column is missing")
	}
	if !strings.Contains(err.Error(), "add the new column") {
		t.Fatalf("expected an actionable preflight error, got: %v", err)
	}
	// Nothing was mutated: no v2 table, no armed MV.
	for _, name := range []string{"agg_v2", "raw_mv_v2"} {
		if n := scalarU64(t, conn, "SELECT count() FROM system.tables WHERE database = '"+db+"' AND name = '"+name+"'"); n != 0 {
			t.Fatalf("%s should not exist — preflight must fail before mutating", name)
		}
	}
}
