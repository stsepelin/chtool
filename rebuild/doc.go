// Package rebuild performs online rebuilds of ClickHouse AggregatingMergeTree
// tables — an ORDER BY change or a materialized-view re-point — while ingestion
// continues, with no data loss and no double-counting.
//
// ALTER TABLE … MODIFY ORDER BY is metadata-only and never re-sorts existing
// data, so any real key change means building a new table and backfilling it.
// Doing that on a live table without losing or double-counting in-flight events
// is fiddly; this package encodes a correct, resumable procedure:
//
//  1. create the new table (v2) from the spec's DDL;
//  2. arm copies of the feeding materialized views at a near-future boundary T
//     (they capture events with created_at >= T), derived from the live
//     SHOW CREATE so their SELECT is never hand-duplicated;
//  3. wait past T, then lag-drain (the count of pre-T rows must go quiet);
//  4. backfill history (created_at < T) — the exact complement of the MVs —
//     newest-partition first, split into memory-bounded hash-bucket chunks,
//     with query settings adapted to the server's RAM;
//  5. validate old vs new (sum() per measure must match exactly);
//  6. cut over: drop MVs, RENAME, recreate MVs pointing at the swapped table.
//
// State lives in a StateStore so a run resumes after any interruption. The
// orchestrator is engine-agnostic: it takes a clickhouse-go/v2 driver.Conn and
// a StateStore, and never assumes a particular migration tool.
package rebuild

import "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

type Conn = driver.Conn
