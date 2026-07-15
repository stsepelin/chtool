package rebuild

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Phase names recorded in the state store.
const (
	phaseCreated    = "created"
	phaseDualWrite  = "dual_write"
	phaseLagDrained = "lag_drained"
	phaseBackfill   = "backfill"
	phaseValidated  = "validated"
	phaseValFailed  = "validation_failed"
	phaseCutover    = "cutover"
	phaseAborted    = "aborted"
)

// Options tune a Run.
type Options struct {
	BoundaryOffset time.Duration  // T = now + this, on first run (default 10m)
	LagPoll        time.Duration  // between the two lag-drain polls (default 60s)
	Backfill       BackfillConfig // overrides the spec's backfill tuning
}

func (o Options) withDefaults() Options {
	if o.BoundaryOffset <= 0 {
		o.BoundaryOffset = 10 * time.Minute
	}
	if o.LagPoll <= 0 {
		o.LagPoll = 60 * time.Second
	}
	return o
}

// Orchestrator drives one rebuild spec against one server/database.
type Orchestrator struct {
	Conn  Conn
	DB    string
	Spec  *Spec
	Store StateStore
	// ReconcileGuard, if set, is called before cutover; a non-nil error aborts
	// the cutover. Use it to enforce a companion migration exists, etc.
	ReconcileGuard func() error
	// Log receives human-readable progress; nil discards it.
	Log func(format string, a ...any)
}

func (o *Orchestrator) logf(format string, a ...any) {
	if o.Log != nil {
		o.Log(format, a...)
	}
}

func (o *Orchestrator) exec(ctx context.Context, sql string) error {
	o.logf("  SQL: %s", truncate(sql, 160))
	return o.Conn.Exec(ctx, sql)
}

func (o *Orchestrator) v2MVName(mv string) string { return mv + "_v2" }

func (o *Orchestrator) mvs(ctx context.Context) ([]*MV, error) {
	var out []*MV
	for _, name := range o.Spec.MVs {
		mv, err := FetchMV(ctx, o.Conn, o.DB, name)
		if err != nil {
			return nil, err
		}
		out = append(out, mv)
	}
	return out, nil
}

func distinctSources(mvs []*MV) []string {
	seen := map[string]bool{}
	var out []string
	for _, m := range mvs {
		if s := m.SourceName(); !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

func (o *Orchestrator) record(ctx context.Context, phase, status, cursor, detail string) error {
	return o.Store.Append(ctx, Record{
		OpID: o.Spec.OpID(), SpecHash: o.Spec.Hash(),
		Phase: phase, Status: status, Cursor: cursor, Detail: detail,
	})
}

// Run executes create → dual-write → lag-drain → backfill → validate. It is
// resumable: completed backfill chunks are skipped and the boundary T is read
// back from the state store. Cutover is a separate, explicit command.
func (o *Orchestrator) Run(ctx context.Context, opts Options) error {
	opts = opts.withDefaults()
	recs, err := o.Store.Records(ctx, o.Spec.OpID())
	if err != nil {
		return err
	}
	if err := o.guardSpecUnchanged(recs); err != nil {
		return err
	}

	// 1. create v2 (idempotent).
	v2ddl, err := o.Spec.NewDDLForV2()
	if err != nil {
		return err
	}
	o.logf("phase: create %s", o.Spec.V2Table())
	if err := o.exec(ctx, v2ddl); err != nil {
		return fmt.Errorf("create v2 table: %w", err)
	}
	if err := o.record(ctx, phaseCreated, "done", "", ""); err != nil {
		return err
	}

	// 2. dual-write: fix the near-future boundary T (once) and arm v2 MVs.
	// Persist T before arming and treat its write as fatal: a lost T makes the
	// next Run pick a new T2 while the armed MVs still capture at T1, overlapping
	// [T1,T2) into double-counted rows.
	t, isNew := boundaryFrom(recs)
	if isNew {
		t = time.Now().UTC().Add(opts.BoundaryOffset).Truncate(time.Second)
		if err := o.record(ctx, phaseDualWrite, "boundary", "", "T="+t.UTC().Format(tsLayout)); err != nil {
			return fmt.Errorf("persist boundary T: %w", err)
		}
	}
	o.logf("phase: dual-write, boundary T = %s (armed MVs capture %s >= T)", t.UTC().Format(tsLayout), o.Spec.BoundaryColumn)
	mvs, err := o.mvs(ctx)
	if err != nil {
		return err
	}
	for _, m := range mvs {
		sql := m.V2CreateSQL(o.DB, o.v2MVName(m.Name), o.DB+"."+o.Spec.V2Table(), o.Spec.BoundaryColumn, t)
		sql = strings.Replace(sql, "CREATE MATERIALIZED VIEW", "CREATE MATERIALIZED VIEW IF NOT EXISTS", 1)
		if err := o.exec(ctx, sql); err != nil {
			return fmt.Errorf("arm v2 mv %s: %w", m.Name, err)
		}
	}

	// 3. lag-drain.
	if wait := time.Until(t); wait > 0 {
		o.logf("phase: waiting %s for wall clock to pass T", wait.Round(time.Second))
		if err := sleep(ctx, wait); err != nil {
			return err
		}
	}
	if err := o.waitLagDrained(ctx, mvs, t, opts.LagPoll); err != nil {
		return err
	}
	if err := o.record(ctx, phaseLagDrained, "done", "", ""); err != nil {
		return err
	}

	// 4. backfill.
	if err := o.backfill(ctx, mvs, t, opts, recs); err != nil {
		return err
	}

	// 5. validate.
	if err := o.validate(ctx); err != nil {
		_ = o.record(ctx, phaseValFailed, "failed", "", err.Error())
		return err
	}
	if err := o.record(ctx, phaseValidated, "done", "", ""); err != nil {
		return err
	}
	o.logf("rebuild ready: validation passed. Run Cutover to swap.")
	return nil
}

func (o *Orchestrator) guardSpecUnchanged(recs []Record) error {
	if len(recs) == 0 {
		return nil
	}
	last := recs[len(recs)-1]
	if last.SpecHash != "" && last.SpecHash != o.Spec.Hash() {
		return fmt.Errorf("spec changed since the in-progress run (hash %s→%s); abort the old run first",
			short(last.SpecHash), short(o.Spec.Hash()))
	}
	return nil
}

// boundaryFrom returns the boundary T recorded after the last abort, or
// isNew=true when a fresh boundary must be chosen.
func boundaryFrom(recs []Record) (t time.Time, isNew bool) {
	lastAbort := -1
	for i, r := range recs {
		if r.Phase == phaseAborted {
			lastAbort = i
		}
	}
	for i := lastAbort + 1; i < len(recs); i++ {
		if strings.HasPrefix(recs[i].Detail, "T=") {
			if parsed, err := time.ParseInLocation(tsLayout, strings.TrimPrefix(recs[i].Detail, "T="), time.UTC); err == nil {
				return parsed, false
			}
		}
	}
	return time.Time{}, true
}

func (o *Orchestrator) waitLagDrained(ctx context.Context, mvs []*MV, t time.Time, poll time.Duration) error {
	sources := distinctSources(mvs)
	o.logf("phase: lag-drain (sources %s, poll %s)", strings.Join(sources, ","), poll)
	first := map[string]uint64{}
	for _, s := range sources {
		n, err := o.preTCount(ctx, s, t)
		if err != nil {
			return err
		}
		first[s] = n
	}
	if err := sleep(ctx, poll); err != nil {
		return err
	}
	for _, s := range sources {
		n, err := o.preTCount(ctx, s, t)
		if err != nil {
			return err
		}
		if n != first[s] {
			return fmt.Errorf("lag not drained for %s: pre-T count moved %d→%d; late events still arriving", s, first[s], n)
		}
	}
	return nil
}

func (o *Orchestrator) preTCount(ctx context.Context, source string, t time.Time) (uint64, error) {
	return o.scalarUint(ctx, fmt.Sprintf("SELECT count() FROM %s.%s WHERE %s < '%s'",
		o.DB, source, o.Spec.BoundaryColumn, t.UTC().Format(tsLayout)))
}

func (o *Orchestrator) backfill(ctx context.Context, mvs []*MV, t time.Time, opts Options, recs []Record) error {
	cfg := o.Spec.Backfill.Merge(opts.Backfill)
	tuning, err := DeriveTuning(ctx, o.Conn, cfg)
	if err != nil {
		return fmt.Errorf("derive backfill tuning: %w", err)
	}
	cfg = tuning.cfg
	settings := tuning.SettingsClause()
	o.logf("phase: backfill — RAM=%s, external_group_by=%s, max_memory=%s, target=%d rows/chunk%s%s",
		hsize(tuning.RAMBytes), hsize(tuning.ExternalGroupByBytes), hsize(tuning.MaxMemoryBytes),
		cfg.TargetRowsPerChunk, rateNote(tuning.RateLimitBytesPerSec), lockedNote(tuning.Locked))

	values, err := o.backfillChunkValues(ctx, mvs, t)
	if err != nil {
		return err
	}
	done := completedChunks(recs)
	v2target := o.DB + "." + o.Spec.V2Table()
	chunkCol := o.Spec.chunkColumn()

	for _, val := range values {
		for _, m := range mvs {
			cnt, err := o.scalarUint(ctx, m.CountSQL(o.Spec.BoundaryColumn, chunkCol, val, t))
			if err != nil {
				return err
			}
			if cnt == 0 {
				continue
			}
			n := buckets(cnt, cfg.TargetRowsPerChunk, cfg.MaxBuckets)
			bucketExpr := m.BucketExpr(cfg.BucketKey)
			for b := range n {
				cursor := backfillCursor(val, m.Name, n, b)
				if done[cursor] {
					continue
				}
				pred := fmt.Sprintf("%s = '%s' AND %s %% %d = %d", chunkCol, esc(val), bucketExpr, n, b)
				if err := o.exec(ctx, m.BackfillSQL(v2target, o.Spec.BoundaryColumn, t, pred, settings)); err != nil {
					return fmt.Errorf("backfill %s: %w", cursor, err)
				}
				if err := o.record(ctx, phaseBackfill, "done", cursor, fmt.Sprintf("~%d rows", cnt/uint64(n))); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (o *Orchestrator) backfillChunkValues(ctx context.Context, mvs []*MV, t time.Time) ([]string, error) {
	col := o.Spec.chunkColumn()
	var parts []string
	for _, s := range distinctSources(mvs) {
		parts = append(parts, fmt.Sprintf("SELECT %s AS v FROM %s.%s WHERE %s < '%s'",
			col, o.DB, s, o.Spec.BoundaryColumn, t.UTC().Format(tsLayout)))
	}
	q := "SELECT DISTINCT toString(v) AS d FROM (" + strings.Join(parts, " UNION ALL ") + ") ORDER BY d DESC"
	rows, err := o.Conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var vals []string
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		vals = append(vals, d)
	}
	return vals, rows.Err()
}

// backfillCursor keys one backfill unit. It must include the MV name (not the
// source): distinct MVs can share a source, and a source-keyed cursor would
// collide and skip the second MV's backfill.
func backfillCursor(val, mvName string, n, b int) string {
	return fmt.Sprintf("%s|%s|N%d|b%d", val, mvName, n, b)
}

func completedChunks(recs []Record) map[string]bool {
	out := map[string]bool{}
	for _, r := range recs {
		if r.Phase == phaseBackfill && r.Status == "done" {
			out[r.Cursor] = true
		}
	}
	return out
}

func (o *Orchestrator) validate(ctx context.Context) error {
	o.logf("phase: validate (old %s vs new %s)", o.Spec.TargetTable, o.Spec.V2Table())
	for _, expr := range o.Spec.Validations {
		oldV, err := o.scalarString(ctx, fmt.Sprintf("SELECT toString(%s) FROM %s.%s", expr, o.DB, o.Spec.TargetTable))
		if err != nil {
			return err
		}
		newV, err := o.scalarString(ctx, fmt.Sprintf("SELECT toString(%s) FROM %s.%s", expr, o.DB, o.Spec.V2Table()))
		if err != nil {
			return err
		}
		if oldV != newV {
			return fmt.Errorf("validation %q mismatch: old=%s new=%s", expr, oldV, newV)
		}
		o.logf("  ✓ %s = %s", expr, oldV)
	}
	return nil
}

func (o *Orchestrator) scalarUint(ctx context.Context, q string) (uint64, error) {
	var v uint64
	err := o.Conn.QueryRow(ctx, q).Scan(&v)
	return v, err
}

func (o *Orchestrator) scalarString(ctx context.Context, q string) (string, error) {
	var v string
	err := o.Conn.QueryRow(ctx, q).Scan(&v)
	return v, err
}

func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

func truncate(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}

func short(h string) string {
	if len(h) > 8 {
		return h[:8]
	}
	return h
}

func hsize(b uint64) string {
	if b == 0 {
		return "unknown"
	}
	const u = 1024
	if b < u {
		return fmt.Sprintf("%dB", b)
	}
	div, exp := uint64(u), 0
	for n := b / u; n >= u; n /= u {
		div *= u
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func rateNote(bytesPerSec uint64) string {
	if bytesPerSec == 0 {
		return ""
	}
	return ", rate=" + hsize(bytesPerSec) + "/s"
}

func lockedNote(locked []string) string {
	if len(locked) == 0 {
		return ""
	}
	return ", READONLY(not set)=" + strings.Join(locked, ",")
}
