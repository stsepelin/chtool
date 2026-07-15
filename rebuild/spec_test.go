package rebuild

import (
	"strings"
	"testing"
	"time"
)

func TestNewDDLForV2RetargetsCreateNotComments(t *testing.T) {
	s := (&Spec{TargetTable: "daily_overview"}).SetNewDDL(
		"-- rebuild of daily_overview (name in a comment)\n" +
			"CREATE TABLE IF NOT EXISTS daily_overview (x UInt8) ENGINE = MergeTree ORDER BY x;")
	out, err := s.NewDDLForV2()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "CREATE TABLE IF NOT EXISTS daily_overview_v2 (") {
		t.Fatalf("CREATE TABLE not retargeted: %s", out)
	}
	if !strings.Contains(out, "-- rebuild of daily_overview (name") {
		t.Fatalf("comment should be preserved: %s", out)
	}
}

func TestNewDDLForV2ForcesIfNotExists(t *testing.T) {
	s := (&Spec{TargetTable: "daily_overview"}).SetNewDDL(
		"CREATE TABLE daily_overview (x UInt8) ENGINE = MergeTree ORDER BY x;")
	out, err := s.NewDDLForV2()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "CREATE TABLE IF NOT EXISTS daily_overview_v2 (") {
		t.Fatalf("IF NOT EXISTS should be injected so resume is idempotent: %s", out)
	}
}

func TestNewDDLForV2ErrorsWithoutCreate(t *testing.T) {
	s := (&Spec{TargetTable: "daily_overview"}).SetNewDDL("SELECT 1")
	if _, err := s.NewDDLForV2(); err == nil {
		t.Fatal("expected error when no CREATE TABLE present")
	}
}

func TestSpecMVDDLChangesHashAndReadsBack(t *testing.T) {
	s := (&Spec{Name: "n", TargetTable: "t"}).SetNewDDL("CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY x")
	before := s.Hash()
	s.SetMVDDL("mv1", "CREATE MATERIALIZED VIEW mv1 TO t (x UInt8) AS SELECT x FROM s GROUP BY x")
	if s.Hash() == before {
		t.Fatal("hash should change when a new MV definition is supplied")
	}
	if s.newMVDDL("mv1") == "" {
		t.Fatal("newMVDDL should return the supplied definition")
	}
	if s.newMVDDL("absent") != "" {
		t.Fatal("newMVDDL should be empty for an MV without a new definition")
	}
}

func TestSpecValidateRejectsUnknownNewMV(t *testing.T) {
	s := &Spec{Name: "n", TargetTable: "t", BoundaryColumn: "created_at", MVs: []string{"mv"}}
	s.SetNewDDL("CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY x")
	s.SetMVDDL("not_listed", "CREATE MATERIALIZED VIEW not_listed TO t (x UInt8) AS SELECT x FROM s GROUP BY x")
	if err := s.validate(); err == nil {
		t.Fatal("validate should reject a new_mvs entry not present in spec.mvs")
	}
}

func TestChunkColumnDefault(t *testing.T) {
	if (&Spec{}).chunkColumn() != "date" {
		t.Fatal("default chunk column should be date")
	}
	if (&Spec{ChunkColumn: "event_day"}).chunkColumn() != "event_day" {
		t.Fatal("explicit chunk column should be honored")
	}
}

func TestSpecHelpers(t *testing.T) {
	s := &Spec{Name: "x", TargetTable: "t"}
	if s.OpID() != "rebuild:x" || s.V2Table() != "t_v2" {
		t.Fatalf("helpers wrong: %s %s", s.OpID(), s.V2Table())
	}
	if got := s.BackupTable(time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)); got != "t_backup_20260714" {
		t.Fatalf("BackupTable = %s", got)
	}
}
