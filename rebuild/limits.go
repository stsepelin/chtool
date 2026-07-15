package rebuild

import (
	"context"
	"fmt"
	"strings"
)

// BackfillConfig tunes the memory-safe backfill. Zero fields fall back to
// defaults; a spec may override them.
type BackfillConfig struct {
	TargetRowsPerChunk   uint64  `yaml:"target_rows_per_chunk"`
	MemoryFraction       float64 `yaml:"memory_fraction"`
	MaxBuckets           int     `yaml:"max_buckets"`
	RateLimitBytesPerSec uint64  `yaml:"rate_limit_bytes_per_sec"`
	MaxExecutionTime     int     `yaml:"max_execution_time"`
	BucketKey            string  `yaml:"bucket_key"`
}

func (c BackfillConfig) withDefaults() BackfillConfig {
	if c.TargetRowsPerChunk == 0 {
		c.TargetRowsPerChunk = 50_000_000
	}
	if c.MemoryFraction == 0 {
		c.MemoryFraction = 0.3
	}
	if c.MaxBuckets == 0 {
		c.MaxBuckets = 256
	}
	if c.MaxExecutionTime == 0 {
		c.MaxExecutionTime = 3600
	}
	return c
}

// Merge overlays non-zero fields of override onto c.
func (c BackfillConfig) Merge(override BackfillConfig) BackfillConfig {
	if override.TargetRowsPerChunk > 0 {
		c.TargetRowsPerChunk = override.TargetRowsPerChunk
	}
	if override.MemoryFraction > 0 {
		c.MemoryFraction = override.MemoryFraction
	}
	if override.MaxBuckets > 0 {
		c.MaxBuckets = override.MaxBuckets
	}
	if override.RateLimitBytesPerSec > 0 {
		c.RateLimitBytesPerSec = override.RateLimitBytesPerSec
	}
	if override.MaxExecutionTime > 0 {
		c.MaxExecutionTime = override.MaxExecutionTime
	}
	if override.BucketKey != "" {
		c.BucketKey = override.BucketKey
	}
	return c
}

// Tuning is the resolved, server-adapted set of backfill query settings.
type Tuning struct {
	ExternalGroupByBytes uint64
	MaxMemoryBytes       uint64
	RateLimitBytesPerSec uint64
	MaxExecutionTime     int
	RAMBytes             uint64
	Locked               []string
	cfg                  BackfillConfig
}

var backfillSettings = []string{
	"max_bytes_before_external_group_by",
	"max_memory_usage",
	"max_execution_time",
	"max_execution_speed_bytes",
}

const fallbackExternalGroupBy = 4 << 30 // 4 GiB

// DeriveTuning probes the live service for its RAM and which settings are
// adjustable, then derives memory-safe values: external GROUP BY at
// MemoryFraction of RAM and max_memory_usage at 2× that (the merge stage cannot
// spill, so it may need as much RAM as stage 1).
func DeriveTuning(ctx context.Context, conn Conn, cfg BackfillConfig) (Tuning, error) {
	cfg = cfg.withDefaults()
	ram, err := probeRAM(ctx, conn)
	if err != nil {
		return Tuning{}, err
	}
	locked, err := probeReadonly(ctx, conn)
	if err != nil {
		return Tuning{}, err
	}
	var ext uint64 = fallbackExternalGroupBy
	if ram > 0 {
		ext = uint64(cfg.MemoryFraction * float64(ram))
	}
	return Tuning{
		ExternalGroupByBytes: ext,
		MaxMemoryBytes:       2 * ext,
		RateLimitBytesPerSec: cfg.RateLimitBytesPerSec,
		MaxExecutionTime:     cfg.MaxExecutionTime,
		RAMBytes:             ram,
		Locked:               locked,
		cfg:                  cfg,
	}, nil
}

// SettingsClause renders the ` SETTINGS …` clause with only the settings the
// service permits (skips any READONLY ones).
func (t Tuning) SettingsClause() string {
	locked := map[string]bool{}
	for _, n := range t.Locked {
		locked[n] = true
	}
	var parts []string
	addUint := func(name string, v uint64) {
		if !locked[name] && v > 0 {
			parts = append(parts, fmt.Sprintf("%s = %d", name, v))
		}
	}
	addUint("max_bytes_before_external_group_by", t.ExternalGroupByBytes)
	addUint("max_memory_usage", t.MaxMemoryBytes)
	addUint("max_execution_speed_bytes", t.RateLimitBytesPerSec)
	if !locked["max_execution_time"] && t.MaxExecutionTime > 0 {
		parts = append(parts, fmt.Sprintf("max_execution_time = %d", t.MaxExecutionTime))
	}
	if len(parts) == 0 {
		return ""
	}
	return " SETTINGS " + strings.Join(parts, ", ")
}

func probeRAM(ctx context.Context, conn Conn) (uint64, error) {
	const q = `SELECT toUInt64(if(cg > 0, cg, os)) FROM (
		SELECT
			maxIf(value, metric = 'CGroupMemoryTotal') AS cg,
			maxIf(value, metric = 'OSMemoryTotal')     AS os
		FROM system.asynchronous_metrics
		WHERE metric IN ('CGroupMemoryTotal', 'OSMemoryTotal'))`
	var ram uint64
	if err := conn.QueryRow(ctx, q).Scan(&ram); err != nil {
		return 0, nil // memory not reported — use the conservative fallback
	}
	return ram, nil
}

func probeReadonly(ctx context.Context, conn Conn) ([]string, error) {
	rows, err := conn.Query(ctx,
		"SELECT name FROM system.settings WHERE name IN ('"+strings.Join(backfillSettings, "','")+"') AND readonly != 0")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var locked []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		locked = append(locked, n)
	}
	return locked, rows.Err()
}

// buckets sizes the number of sub-chunks so each targets ~target rows, clamped
// to [1, max].
func buckets(rows, target uint64, maxBuckets int) int {
	if target == 0 {
		return 1
	}
	// ceil(rows/target) without the rows+target-1 overflow near MaxUint64.
	q := rows / target
	if rows%target != 0 {
		q++
	}
	return min(maxBuckets, max(1, int(q)))
}
