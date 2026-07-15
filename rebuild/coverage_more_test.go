package rebuild

import (
	"context"
	"strings"
	"testing"
)

func TestSpecValidate(t *testing.T) {
	base := func() *Spec {
		s := &Spec{Name: "n", TargetTable: "t", BoundaryColumn: "created_at", MVs: []string{"mv"}}
		s.SetNewDDL("CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY x")
		return s
	}
	if err := base().validate(); err != nil {
		t.Fatalf("valid spec rejected: %v", err)
	}
	cases := map[string]func(*Spec){
		"name":         func(s *Spec) { s.Name = "" },
		"target_table": func(s *Spec) { s.TargetTable = "" },
		"boundary":     func(s *Spec) { s.BoundaryColumn = "" },
		"mvs":          func(s *Spec) { s.MVs = nil },
		"new_ddl":      func(s *Spec) { s.newDDL = "" },
	}
	for name, mutate := range cases {
		s := base()
		mutate(s)
		if err := s.validate(); err == nil {
			t.Errorf("missing %s should fail validation", name)
		}
	}
}

func TestBackfillConfigMergeAllFields(t *testing.T) {
	got := BackfillConfig{}.Merge(BackfillConfig{
		TargetRowsPerChunk:   10,
		MemoryFraction:       0.5,
		MaxBuckets:           8,
		RateLimitBytesPerSec: 99,
		MaxExecutionTime:     120,
		BucketKey:            "xxHash64(id)",
	})
	if got.TargetRowsPerChunk != 10 || got.MemoryFraction != 0.5 || got.MaxBuckets != 8 ||
		got.RateLimitBytesPerSec != 99 || got.MaxExecutionTime != 120 || got.BucketKey != "xxHash64(id)" {
		t.Fatalf("full override merge wrong: %+v", got)
	}
}

func TestBucketsZeroTarget(t *testing.T) {
	if got := buckets(1000, 0, 256); got != 1 {
		t.Fatalf("zero target should yield 1 bucket, got %d", got)
	}
}

func TestDeriveTuningRespectsLockedSetting(t *testing.T) {
	conn := &fakeConn{lockedSetting: "max_memory_usage"}
	tn, err := DeriveTuning(context.Background(), conn, BackfillConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if len(tn.Locked) != 1 || tn.Locked[0] != "max_memory_usage" {
		t.Fatalf("locked setting not surfaced: %+v", tn.Locked)
	}
	// 16 GiB RAM * 0.3 default fraction → external group by set, memory 2x.
	if tn.ExternalGroupByBytes == 0 || tn.MaxMemoryBytes != 2*tn.ExternalGroupByBytes {
		t.Fatalf("tuning math wrong: %+v", tn)
	}
	if strings.Contains(tn.SettingsClause(), "max_memory_usage") {
		t.Errorf("locked setting must be omitted from the clause: %s", tn.SettingsClause())
	}
}

func TestFetchMVNotFound(t *testing.T) {
	conn := &fakeConn{mvRaw: "not-a-materialized-view"}
	if _, err := FetchMV(context.Background(), conn, "db", "v"); err == nil {
		t.Fatal("expected parse error for an unsupported MV shape")
	}
}
