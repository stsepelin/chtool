package rebuild

import (
	"context"
	"strings"
	"testing"
	"time"
)

func newOrch(store *fakeStore, conn Conn) *Orchestrator {
	s := &Spec{
		Name:           "reorder",
		TargetTable:    "events_daily",
		BoundaryColumn: "created_at",
		MVs:            []string{"events_daily_mv", "events_hourly_mv"},
		Validations:    []string{"sum(views)"},
	}
	s.SetNewDDL("CREATE TABLE events_daily (x UInt8) ENGINE = MergeTree ORDER BY x")
	return &Orchestrator{Conn: conn, DB: "analytics", Spec: s, Store: store}
}

// fastOpts keeps a full Run under a few milliseconds: T is essentially now and
// the lag-drain poll is tiny.
func fastOpts() Options {
	return Options{BoundaryOffset: time.Nanosecond, LagPoll: time.Millisecond}
}

func TestRunFullFlow(t *testing.T) {
	conn := &fakeConn{}
	store := &fakeStore{}
	o := newOrch(store, conn)
	o.Log = func(string, ...any) {} // exercise the logf branch

	if err := o.Run(context.Background(), fastOpts()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// The pipeline must reach validated.
	last := store.recs[len(store.recs)-1]
	if last.Phase != phaseValidated {
		t.Fatalf("expected validated phase, got %q", last.Phase)
	}
	// It must have created v2 (idempotent) and armed both MVs.
	joined := strings.Join(conn.execs, "\n")
	if !strings.Contains(joined, "CREATE TABLE IF NOT EXISTS events_daily_v2") {
		t.Errorf("v2 create missing: %s", joined)
	}
	if !strings.Contains(joined, "CREATE MATERIALIZED VIEW IF NOT EXISTS") {
		t.Errorf("armed MV missing: %s", joined)
	}
	if !strings.Contains(joined, "INSERT INTO analytics.events_daily_v2") {
		t.Errorf("backfill insert missing: %s", joined)
	}
}

func TestRunValidationMismatchFails(t *testing.T) {
	conn := &fakeConn{valMismatch: true}
	o := newOrch(&fakeStore{}, conn)
	err := o.Run(context.Background(), fastOpts())
	if err == nil || !strings.Contains(err.Error(), "validation") {
		t.Fatalf("expected a validation mismatch error, got %v", err)
	}
}

func TestRunResumesWithoutDuplicatingBackfill(t *testing.T) {
	conn := &fakeConn{}
	store := &fakeStore{}
	o := newOrch(store, conn)
	if err := o.Run(context.Background(), fastOpts()); err != nil {
		t.Fatal(err)
	}
	inserts := countContains(conn.execs, "INSERT INTO analytics.events_daily_v2")

	// Second run over the same state must skip every completed backfill chunk.
	conn2 := &fakeConn{}
	o2 := newOrch(store, conn2)
	if err := o2.Run(context.Background(), fastOpts()); err != nil {
		t.Fatal(err)
	}
	if got := countContains(conn2.execs, "INSERT INTO analytics.events_daily_v2"); got != 0 {
		t.Fatalf("resume re-ran %d backfills (of %d); completed chunks must be skipped", got, inserts)
	}
}

func TestCutoverHappyPath(t *testing.T) {
	conn := &fakeConn{}
	store := &fakeStore{recs: []Record{{Phase: phaseValidated, Status: "done"}}}
	o := newOrch(store, conn)
	if err := Cutover(context.Background(), o, time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("Cutover: %v", err)
	}
	joined := strings.Join(conn.execs, "\n")
	if !strings.Contains(joined, "RENAME TABLE analytics.events_daily TO analytics.events_daily_backup_20260715") {
		t.Errorf("rename to backup missing: %s", joined)
	}
	if store.recs[len(store.recs)-1].Phase != phaseCutover {
		t.Errorf("cutover phase not recorded: %+v", store.recs)
	}
}

func TestPlanRuns(t *testing.T) {
	conn := &fakeConn{}
	o := newOrch(&fakeStore{}, conn)
	var logs []string
	o.Log = func(f string, a ...any) { logs = append(logs, f) }
	if err := Plan(context.Background(), o, false); err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(logs) == 0 {
		t.Error("Plan should log its sequence")
	}
}

func TestPlanRefusesVersionMismatch(t *testing.T) {
	o := newOrch(&fakeStore{}, &fakeConn{})
	o.Spec.RehearsalVer = "23.8.1" // fake reports 24.3.1
	if err := Plan(context.Background(), o, false); err == nil {
		t.Fatal("Plan must refuse a server-version mismatch without force")
	}
	if err := Plan(context.Background(), o, true); err != nil {
		t.Fatalf("Plan with force-version should proceed: %v", err)
	}
}

func TestStatusReports(t *testing.T) {
	var logs []string
	log := func(f string, a ...any) { logs = append(logs, f) }

	o := newOrch(&fakeStore{}, &fakeConn{})
	o.Log = log
	if err := Status(context.Background(), o); err != nil {
		t.Fatal(err)
	}

	o2 := newOrch(&fakeStore{recs: []Record{{Phase: phaseBackfill, Status: "done", Cursor: "c"}}}, &fakeConn{})
	o2.Log = log
	if err := Status(context.Background(), o2); err != nil {
		t.Fatal(err)
	}
	if len(logs) < 2 {
		t.Error("Status should log for both empty and non-empty state")
	}
}

func TestAbortHappyPath(t *testing.T) {
	conn := &fakeConn{}
	store := &fakeStore{}
	o := newOrch(store, conn)
	if err := Abort(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(conn.execs, "\n")
	for _, want := range []string{
		"DROP VIEW IF EXISTS analytics.events_daily_mv_v2",
		"DROP VIEW IF EXISTS analytics.events_hourly_mv_v2",
		"DROP TABLE IF EXISTS analytics.events_daily_v2",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("abort did not issue %q; got:\n%s", want, joined)
		}
	}
	tableIdx := indexOfContains(conn.execs, "DROP TABLE IF EXISTS")
	for _, mv := range []string{"events_daily_mv_v2", "events_hourly_mv_v2"} {
		if mvIdx := indexOfContains(conn.execs, mv); mvIdx < 0 || mvIdx > tableIdx {
			t.Errorf("v2 MV %s must be dropped before the v2 table", mv)
		}
	}
	if store.recs[len(store.recs)-1].Phase != phaseAborted {
		t.Errorf("abort should record the aborted phase: %+v", store.recs)
	}
}

func TestAbortRefusesAfterCutover(t *testing.T) {
	store := &fakeStore{recs: []Record{{Phase: phaseCutover, Status: "done"}}}
	o := newOrch(store, &fakeConn{})
	if err := Abort(context.Background(), o); err == nil {
		t.Fatal("abort must refuse once cutover has happened")
	}
}

func TestCutoverRequiresValidatedPhase(t *testing.T) {
	store := &fakeStore{recs: []Record{{Phase: phaseBackfill, Status: "done"}}}
	o := newOrch(store, &fakeConn{})
	err := Cutover(context.Background(), o, time.Now())
	if err == nil || !strings.Contains(err.Error(), "validated rebuild") {
		t.Fatalf("cutover should require a validated rebuild, got %v", err)
	}
}

func TestCutoverReconcileGuardBlocks(t *testing.T) {
	store := &fakeStore{recs: []Record{{Phase: phaseValidated, Status: "done"}}}
	o := newOrch(store, &fakeConn{})
	o.ReconcileGuard = func() error { return context.Canceled }
	if err := Cutover(context.Background(), o, time.Now()); err == nil {
		t.Fatal("a failing ReconcileGuard must block cutover")
	}
}

func TestRunRefusesChangedSpec(t *testing.T) {
	store := &fakeStore{recs: []Record{{OpID: "rebuild:reorder", SpecHash: "OLDHASH", Phase: phaseCreated}}}
	o := newOrch(store, &fakeConn{})
	err := o.Run(context.Background(), Options{})
	if err == nil || !strings.Contains(err.Error(), "spec changed") {
		t.Fatalf("Run must refuse a changed spec, got %v", err)
	}
}

func indexOfContains(ss []string, sub string) int {
	for i, s := range ss {
		if strings.Contains(s, sub) {
			return i
		}
	}
	return -1
}

func countContains(ss []string, sub string) int {
	n := 0
	for _, s := range ss {
		if strings.Contains(s, sub) {
			n++
		}
	}
	return n
}
