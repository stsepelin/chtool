package rebuild

import (
	"context"
	"fmt"
	"time"
)

// Cutover performs the swap. It is a separate, explicit step, gated on
// validation having passed and (if set) the ReconcileGuard.
//
// Order is a correctness rule: never let any MV survive the rename. Drop all
// MVs (old and v2) first, RENAME, then recreate the canonical MVs pointing at
// the renamed target.
func Cutover(ctx context.Context, o *Orchestrator, now time.Time) error {
	if o.ReconcileGuard != nil {
		if err := o.ReconcileGuard(); err != nil {
			return fmt.Errorf("reconcile guard: %w", err)
		}
	}

	recs, err := o.Store.Records(ctx, o.Spec.OpID())
	if err != nil {
		return err
	}
	if len(recs) == 0 || recs[len(recs)-1].Phase != phaseValidated {
		phase := "(none)"
		if len(recs) > 0 {
			phase = recs[len(recs)-1].Phase
		}
		return fmt.Errorf("cutover requires a validated rebuild; current phase = %q", phase)
	}

	mvs, err := o.mvs(ctx)
	if err != nil {
		return err
	}

	o.logf("OPERATOR CHECKLIST before proceeding:")
	o.logf("  • stop the writers and verify NO inserts for ~30s (a buffered queue means zero loss)")
	o.logf("  • confirm this is the intended target: %s", o.Spec.TargetTable)

	backup := o.Spec.BackupTable(now)
	for _, m := range mvs {
		if err := o.exec(ctx, fmt.Sprintf("DROP VIEW IF EXISTS %s.%s", o.DB, m.Name)); err != nil {
			return err
		}
	}
	for _, m := range mvs {
		if err := o.exec(ctx, fmt.Sprintf("DROP VIEW IF EXISTS %s.%s", o.DB, o.v2MVName(m.Name))); err != nil {
			return err
		}
	}
	if err := o.exec(ctx, fmt.Sprintf("RENAME TABLE %s.%s TO %s.%s, %s.%s TO %s.%s",
		o.DB, o.Spec.TargetTable, o.DB, backup,
		o.DB, o.Spec.V2Table(), o.DB, o.Spec.TargetTable)); err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	for _, m := range mvs {
		if err := o.exec(ctx, m.OriginalCreateSQL()); err != nil {
			return fmt.Errorf("recreate mv %s: %w", m.Name, err)
		}
	}

	_ = o.record(ctx, phaseCutover, "done", "", "backup="+backup)
	o.logf("cutover complete. Old table kept as %s.%s (drop it only after the new table survives a full reporting cycle).", o.DB, backup)
	o.logf("Resume the writers now.")
	return nil
}
