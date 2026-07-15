package rebuild

import (
	"strings"
	"testing"
	"time"
)

const sampleMV = `CREATE MATERIALIZED VIEW default.daily_views_mv TO default.daily_overview ` +
	`(user_id String, spent_budget Decimal(38, 6), views UInt64, ids Array(UInt64)) ` +
	`AS SELECT user_id, sum(spent_budget) AS spent_budget, count() AS views, ids ` +
	`FROM default.views GROUP BY user_id, ids`

func TestParseMV(t *testing.T) {
	m, err := parseMV(sampleMV)
	if err != nil {
		t.Fatalf("parseMV: %v", err)
	}
	if m.Name != "daily_views_mv" || m.Target != "default.daily_overview" || m.SourceName() != "views" {
		t.Fatalf("parsed wrong: %+v", m)
	}
	if got := strings.Join(m.ColumnNames(), ","); got != "user_id,spent_budget,views,ids" {
		t.Errorf("ColumnNames = %q", got)
	}
}

func TestParseMVRejectsWhere(t *testing.T) {
	if _, err := parseMV(strings.Replace(sampleMV, "FROM default.views", "FROM default.views WHERE x > 0", 1)); err == nil {
		t.Fatal("expected rejection of MV with existing WHERE")
	}
}

func TestSplitTopLevelIgnoresNestedCommas(t *testing.T) {
	if parts := splitTopLevel("a UInt64, b Decimal(38, 6), c SimpleAggregateFunction(sum, UInt64)"); len(parts) != 3 {
		t.Fatalf("expected 3 columns, got %d: %v", len(parts), parts)
	}
}

// TestBackfillIsExactComplementOfMV is the core correctness property: the v2 MV
// WHERE and the backfill WHERE partition events at T with no overlap or gap.
func TestBackfillIsExactComplementOfMV(t *testing.T) {
	m, _ := parseMV(sampleMV)
	tt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	v2 := m.V2CreateSQL("default", "daily_views_mv_v2", "default.daily_overview_v2", "created_at", tt)
	bf := m.BackfillSQL("default.daily_overview_v2", "created_at", tt, "date = '2026-07-14'", " SETTINGS max_memory_usage = 100")

	if !strings.Contains(v2, "WHERE created_at >= '2026-07-14 12:00:00'") {
		t.Errorf("v2 MV boundary wrong: %s", v2)
	}
	if !strings.Contains(bf, "WHERE created_at < '2026-07-14 12:00:00'") {
		t.Errorf("backfill boundary wrong: %s", bf)
	}
	if !strings.Contains(bf, "(`user_id`, `spent_budget`, `views`, `ids`)") {
		t.Errorf("backfill missing target column list: %s", bf)
	}
	if !strings.Contains(bf, "SETTINGS max_memory_usage = 100") {
		t.Errorf("backfill missing settings clause: %s", bf)
	}
}
