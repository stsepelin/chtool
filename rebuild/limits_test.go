package rebuild

import (
	"strings"
	"testing"
)

func TestBuckets(t *testing.T) {
	cases := []struct {
		rows, target uint64
		max, want    int
	}{
		{1000, 100, 256, 10},
		{50, 100, 256, 1},
		{101, 100, 256, 2},
		{1e9, 100, 256, 256},
		{0, 100, 256, 1},
	}
	for _, c := range cases {
		if got := buckets(c.rows, c.target, c.max); got != c.want {
			t.Errorf("buckets(%d,%d,%d) = %d, want %d", c.rows, c.target, c.max, got, c.want)
		}
	}
}

func TestSettingsClauseSkipsLocked(t *testing.T) {
	tn := Tuning{ExternalGroupByBytes: 4 << 30, MaxMemoryBytes: 8 << 30, MaxExecutionTime: 3600, Locked: []string{"max_memory_usage"}}
	c := tn.SettingsClause()
	if !strings.Contains(c, "max_bytes_before_external_group_by = ") {
		t.Errorf("external group by should be set: %s", c)
	}
	if strings.Contains(c, "max_memory_usage") {
		t.Errorf("READONLY max_memory_usage must be skipped: %s", c)
	}
	if !strings.Contains(c, "max_execution_time = 3600") {
		t.Errorf("execution time should be set: %s", c)
	}
}

func TestSettingsClauseEmpty(t *testing.T) {
	if c := (Tuning{}).SettingsClause(); c != "" {
		t.Errorf("all-zero tuning should produce no clause, got %q", c)
	}
}

func TestSettingsClauseRateLimit(t *testing.T) {
	if c := (Tuning{RateLimitBytesPerSec: 1000}).SettingsClause(); !strings.Contains(c, "max_execution_speed_bytes = 1000") {
		t.Errorf("rate limit should map to max_execution_speed_bytes: %s", c)
	}
}

func TestBackfillConfigMerge(t *testing.T) {
	base := BackfillConfig{TargetRowsPerChunk: 100, MemoryFraction: 0.3}
	got := base.Merge(BackfillConfig{TargetRowsPerChunk: 500, RateLimitBytesPerSec: 9})
	if got.TargetRowsPerChunk != 500 || got.MemoryFraction != 0.3 || got.RateLimitBytesPerSec != 9 {
		t.Fatalf("merge wrong: %+v", got)
	}
}
