package rebuild

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Plan prints the rebuild sequence and runs preflight checks: a server-version
// gate (refuse if it differs from the spec's rehearsal version unless
// forceVersion), a size estimate, the server-adapted backfill tuning, and a
// FORMAT Null cost probe on one representative chunk. It mutates nothing.
func Plan(ctx context.Context, o *Orchestrator, forceVersion bool) error {
	if err := o.Spec.validate(); err != nil {
		return err
	}
	o.logf("Rebuild plan: %s (target %s → %s)", o.Spec.Name, o.Spec.TargetTable, o.Spec.V2Table())

	ver, err := o.scalarString(ctx, "SELECT version()")
	if err != nil {
		return err
	}
	if o.Spec.RehearsalVer != "" && ver != o.Spec.RehearsalVer {
		msg := fmt.Sprintf("server version %s differs from dress-rehearsal version %s", ver, o.Spec.RehearsalVer)
		if !forceVersion {
			return fmt.Errorf("%s: re-rehearse or pass force-version (MV/rename semantics have regressed across versions)", msg)
		}
		o.logf("WARNING: %s (forced)", msg)
	} else {
		o.logf("server version: %s", ver)
	}

	mvs, err := o.mvs(ctx)
	if err != nil {
		return err
	}
	if err := o.preflightMVs(ctx, mvs); err != nil {
		return err
	}
	sources := distinctSources(mvs)
	rows, bytes, err := o.sizeEstimate(ctx, sources)
	if err != nil {
		return err
	}
	o.logf("backfill sources: %s (~%d rows, %s on disk)", strings.Join(sources, ", "), rows, bytes)

	tuning, err := DeriveTuning(ctx, o.Conn, o.Spec.Backfill)
	if err != nil {
		return err
	}
	o.logf("backfill tuning: RAM=%s, external_group_by=%s, max_memory=%s, target=%d rows/chunk%s%s",
		hsize(tuning.RAMBytes), hsize(tuning.ExternalGroupByBytes), hsize(tuning.MaxMemoryBytes),
		tuning.cfg.TargetRowsPerChunk, rateNote(tuning.RateLimitBytesPerSec), lockedNote(tuning.Locked))
	if err := o.probeChunk(ctx, mvs, tuning); err != nil {
		o.logf("WARNING: chunk cost probe failed (%v) — the real run may need smaller chunks or more memory", err)
	}

	seen, err := o.Store.SpecHashSeen(ctx, o.Spec.Hash())
	if err != nil {
		return err
	}
	if seen {
		o.logf("NOTE: this spec hash already appears in the state store — Run will RESUME, not restart")
	}

	o.logf("planned sequence:")
	o.logf("  1. CREATE %s (new DDL)", o.Spec.V2Table())
	o.logf("  2. arm v2 MVs %s with boundary %s >= T", strings.Join(o.Spec.MVs, ","), o.Spec.BoundaryColumn)
	o.logf("  3. wait past T, then lag-drain on %s", strings.Join(sources, ","))
	o.logf("  4. backfill (%s < T), by %s, memory-safe hash buckets", o.Spec.BoundaryColumn, o.Spec.chunkColumn())
	o.logf("  5. validate old vs new: %s", strings.Join(o.Spec.Validations, ", "))
	o.logf("  6. (separate) cutover: drop MVs → RENAME → recreate MVs")
	return nil
}

func (o *Orchestrator) probeChunk(ctx context.Context, mvs []*MV, tuning Tuning) error {
	if len(mvs) == 0 {
		return nil
	}
	now := time.Now().UTC()
	values, err := o.backfillChunkValues(ctx, mvs, now)
	if err != nil || len(values) == 0 {
		return err
	}
	m := mvs[0]
	col := o.Spec.chunkColumn()
	cnt, err := o.scalarUint(ctx, m.CountSQL(o.Spec.BoundaryColumn, col, values[0], now))
	if err != nil {
		return err
	}
	if cnt == 0 {
		return nil
	}
	n := buckets(cnt, tuning.cfg.TargetRowsPerChunk, tuning.cfg.MaxBuckets)
	pred := fmt.Sprintf("%s = '%s' AND %s %% %d = 0", col, esc(values[0]), m.BucketExpr(tuning.cfg.BucketKey), n)
	sql := m.SelectForProbe(o.Spec.BoundaryColumn, now, pred, tuning.SettingsClause())
	start := time.Now()
	if err := o.Conn.Exec(ctx, sql); err != nil {
		return err
	}
	o.logf("chunk probe: %s %s=%s split into %d bucket(s), 1 aggregated in %s (FORMAT Null, within settings)",
		m.SourceName(), col, values[0], n, time.Since(start).Round(time.Millisecond))
	return nil
}

func (o *Orchestrator) sizeEstimate(ctx context.Context, sources []string) (uint64, string, error) {
	quoted := make([]string, len(sources))
	for i, s := range sources {
		quoted[i] = "'" + esc(s) + "'"
	}
	q := fmt.Sprintf(
		"SELECT sum(rows), formatReadableSize(sum(bytes_on_disk)) FROM system.parts WHERE database = '%s' AND active AND table IN (%s)",
		o.DB, strings.Join(quoted, ","))
	var rows uint64
	var size string
	if err := o.Conn.QueryRow(ctx, q).Scan(&rows, &size); err != nil {
		return 0, "", err
	}
	return rows, size, nil
}
