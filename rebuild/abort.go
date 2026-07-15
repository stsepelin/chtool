package rebuild

import (
	"context"
	"fmt"
)

// Status prints the current state of the rebuild (read-only).
func Status(ctx context.Context, o *Orchestrator) error {
	recs, err := o.Store.Records(ctx, o.Spec.OpID())
	if err != nil {
		return err
	}
	if len(recs) == 0 {
		o.logf("rebuild %s: not started", o.Spec.Name)
		return nil
	}
	cur := recs[len(recs)-1]
	o.logf("rebuild %s: phase=%s status=%s cursor=%s %s", o.Spec.Name, cur.Phase, cur.Status, cur.Cursor, cur.Detail)
	if cur.Status == "failed" && cur.Detail != "" {
		o.logf("  last error: %s", cur.Detail)
	}
	o.logf("  backfill chunks done: %d", len(completedChunks(recs)))
	return nil
}

// Abort tears down the in-progress rebuild BEFORE cutover: drop the v2 MVs then
// the v2 table, leaving no *_v2 objects. The live pipeline is never touched. It
// refuses once cutover has happened.
func Abort(ctx context.Context, o *Orchestrator) error {
	recs, err := o.Store.Records(ctx, o.Spec.OpID())
	if err != nil {
		return err
	}
	if len(recs) > 0 && recs[len(recs)-1].Phase == phaseCutover {
		return fmt.Errorf("cannot abort: cutover already happened; roll back via the manual reverse rename of the %s backup table", o.Spec.TargetTable)
	}

	// Drop the v2 MVs by their derived name, not via FetchMV: a parse failure
	// there would drop the v2 table while orphaning the v2 MVs, which then fail
	// every insert into the live source.
	for _, name := range o.Spec.MVs {
		if err := o.exec(ctx, fmt.Sprintf("DROP VIEW IF EXISTS %s.%s", o.DB, o.v2MVName(name))); err != nil {
			return err
		}
	}
	if err := o.exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", o.DB, o.Spec.V2Table())); err != nil {
		return err
	}
	_ = o.record(ctx, phaseAborted, "done", "", "")
	o.logf("aborted: dropped v2 MVs and %s. No *_v2 objects remain.", o.Spec.V2Table())
	return nil
}
