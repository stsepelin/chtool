package rebuild

import (
	"strings"
	"testing"
	"time"
)

func TestEsc(t *testing.T) {
	cases := map[string]string{
		"plain":      "plain",
		"o'brien":    `o\'brien`,
		`back\slash`: `back\\slash`,
		`'; DROP`:    `\'; DROP`,
	}
	for in, want := range cases {
		if got := esc(in); got != want {
			t.Errorf("esc(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCountSQLEscapesChunkValue(t *testing.T) {
	m, _ := parseMV(sampleMV)
	q := m.CountSQL("created_at", "name", "o'brien", time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC))
	if strings.Contains(q, "'o'brien'") {
		t.Fatalf("unescaped quote breaks the literal: %s", q)
	}
	if !strings.Contains(q, `name = 'o\'brien'`) {
		t.Fatalf("expected escaped chunk value: %s", q)
	}
}

func TestParseMVRejectsNestedFrom(t *testing.T) {
	mv := `CREATE MATERIALIZED VIEW d.v TO d.t (x UInt64) ` +
		`AS SELECT (SELECT max(y) FROM d.other) AS x FROM d.src GROUP BY x`
	if _, err := parseMV(mv); err == nil {
		t.Fatal("expected rejection of a projection containing a nested FROM/subquery")
	}
}

func TestBoundaryFrom(t *testing.T) {
	tsA := "2026-07-14 12:00:00"
	tsB := "2026-07-15 09:30:00"

	// Fresh: no records → new boundary.
	if _, isNew := boundaryFrom(nil); !isNew {
		t.Error("empty records should yield a new boundary")
	}

	// Resume: reads the recorded T back.
	recs := []Record{
		{Phase: phaseCreated, Status: "done"},
		{Phase: phaseDualWrite, Status: "boundary", Detail: "T=" + tsA},
	}
	got, isNew := boundaryFrom(recs)
	if isNew {
		t.Fatal("recorded T should not be treated as new")
	}
	if got.Format(tsLayout) != tsA {
		t.Fatalf("boundary = %s, want %s", got.Format(tsLayout), tsA)
	}

	// After an abort, an earlier T is ignored and a fresh boundary is chosen
	// unless a new T is recorded post-abort.
	aborted := []Record{
		{Phase: phaseDualWrite, Status: "boundary", Detail: "T=" + tsA},
		{Phase: phaseAborted, Status: "done"},
	}
	if _, isNew := boundaryFrom(aborted); !isNew {
		t.Error("a T before the last abort must not be reused")
	}
	aborted = append(aborted, Record{Phase: phaseDualWrite, Status: "boundary", Detail: "T=" + tsB})
	got, isNew = boundaryFrom(aborted)
	if isNew || got.Format(tsLayout) != tsB {
		t.Fatalf("post-abort T should be used: isNew=%v got=%s", isNew, got.Format(tsLayout))
	}
}

// Two MVs that share a source must produce distinct backfill cursors, else the
// second MV's backfill is skipped as already-done (silent data loss).
func TestBackfillCursorDistinguishesMVsSharingSource(t *testing.T) {
	a := backfillCursor("2026-07-14", "events_daily_mv", 4, 1)
	b := backfillCursor("2026-07-14", "events_hourly_mv", 4, 1)
	if a == b {
		t.Fatalf("cursors must differ per MV: %q == %q", a, b)
	}
}

func TestCompletedChunks(t *testing.T) {
	recs := []Record{
		{Phase: phaseBackfill, Status: "done", Cursor: "a"},
		{Phase: phaseBackfill, Status: "failed", Cursor: "b"},
		{Phase: phaseBackfill, Status: "done", Cursor: "c"},
		{Phase: phaseValidated, Status: "done", Cursor: "x"},
	}
	done := completedChunks(recs)
	if !done["a"] || !done["c"] || done["b"] || done["x"] {
		t.Fatalf("completedChunks wrong: %v", done)
	}
}

func TestBucketsNoOverflowNearMax(t *testing.T) {
	// rows + target - 1 would overflow uint64 here; the safe path must clamp to max.
	const maxU64 = ^uint64(0)
	if got := buckets(maxU64, 100, 256); got != 256 {
		t.Fatalf("buckets near MaxUint64 = %d, want 256", got)
	}
}

func TestDistinctSources(t *testing.T) {
	mvs := []*MV{{Source: "db.a"}, {Source: "db.b"}, {Source: "db.a"}}
	got := distinctSources(mvs)
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("distinctSources = %v", got)
	}
}

func TestSourceNameAndBucketExpr(t *testing.T) {
	m := &MV{Source: "`db`.`events`", GroupBy: "user_id, ids"}
	if m.SourceName() != "events" {
		t.Errorf("SourceName = %q", m.SourceName())
	}
	if got := m.BucketExpr(""); got != "cityHash64(user_id, ids)" {
		t.Errorf("default BucketExpr = %q", got)
	}
	if got := m.BucketExpr("xxHash64(user_id)"); got != "xxHash64(user_id)" {
		t.Errorf("override BucketExpr = %q", got)
	}
}

func TestOptionsWithDefaults(t *testing.T) {
	o := Options{}.withDefaults()
	if o.BoundaryOffset != 10*time.Minute || o.LagPoll != 60*time.Second {
		t.Fatalf("defaults wrong: %+v", o)
	}
	o = Options{BoundaryOffset: time.Minute, LagPoll: time.Second}.withDefaults()
	if o.BoundaryOffset != time.Minute || o.LagPoll != time.Second {
		t.Fatalf("explicit values overwritten: %+v", o)
	}
}

func TestBackfillConfigWithDefaults(t *testing.T) {
	c := BackfillConfig{}.withDefaults()
	if c.TargetRowsPerChunk != 50_000_000 || c.MemoryFraction != 0.3 || c.MaxBuckets != 256 || c.MaxExecutionTime != 3600 {
		t.Fatalf("backfill defaults wrong: %+v", c)
	}
}

func TestHumanFormatters(t *testing.T) {
	if got := hsize(0); got != "unknown" {
		t.Errorf("hsize(0) = %q", got)
	}
	if got := hsize(512); got != "512B" {
		t.Errorf("hsize(512) = %q", got)
	}
	if got := hsize(4 << 30); got != "4.0GiB" {
		t.Errorf("hsize(4GiB) = %q", got)
	}
	if got := short("0123456789abcdef"); got != "01234567" {
		t.Errorf("short = %q", got)
	}
	if got := short("abc"); got != "abc" {
		t.Errorf("short(short) = %q", got)
	}
	if got := truncate("a  b\tc", 100); got != "a b c" {
		t.Errorf("truncate collapse = %q", got)
	}
	if got := truncate("abcdef", 3); got != "abc…" {
		t.Errorf("truncate cut = %q", got)
	}
	if rateNote(0) != "" || !strings.Contains(rateNote(1024), "1.0KiB/s") {
		t.Errorf("rateNote wrong: %q %q", rateNote(0), rateNote(1024))
	}
	if lockedNote(nil) != "" || !strings.Contains(lockedNote([]string{"max_memory_usage"}), "max_memory_usage") {
		t.Error("lockedNote wrong")
	}
}
