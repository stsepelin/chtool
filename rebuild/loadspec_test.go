package rebuild

import (
	"os"
	"path/filepath"
	"testing"
)

func writeSpecDir(t *testing.T, yaml, ddl string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "spec.yaml"), []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	if ddl != "" {
		if err := os.WriteFile(filepath.Join(dir, "new_ddl.sql"), []byte(ddl), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

const validSpecYAML = `name: events-daily-neworder
target_table: events_daily
new_ddl: new_ddl.sql
boundary_column: created_at
chunk_column: date
mvs:
  - events_daily_mv
validations:
  - sum(hits)
`

func TestLoadSpecSuccess(t *testing.T) {
	dir := writeSpecDir(t, validSpecYAML, "CREATE TABLE events_daily (x UInt8) ENGINE = AggregatingMergeTree ORDER BY x")
	s, err := LoadSpec(dir)
	if err != nil {
		t.Fatal(err)
	}
	if s.Name != "events-daily-neworder" || s.V2Table() != "events_daily_v2" || s.chunkColumn() != "date" {
		t.Fatalf("spec parsed wrong: %+v", s)
	}
	if s.Hash() == "" {
		t.Error("hash should be set after load")
	}
	if _, err := s.NewDDLForV2(); err != nil {
		t.Errorf("NewDDLForV2 after load: %v", err)
	}
}

func TestLoadSpecMissingRequiredField(t *testing.T) {
	// no mvs → validate must fail.
	dir := writeSpecDir(t, "name: x\ntarget_table: t\nnew_ddl: new_ddl.sql\nboundary_column: created_at\n",
		"CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY x")
	if _, err := LoadSpec(dir); err == nil {
		t.Fatal("expected validation error for missing mvs")
	}
}

func TestLoadSpecMissingDDLFile(t *testing.T) {
	dir := writeSpecDir(t, validSpecYAML, "")
	if _, err := LoadSpec(dir); err == nil {
		t.Fatal("expected error when the new_ddl file is absent")
	}
}

func TestSpecHashChangesWithDDL(t *testing.T) {
	a := (&Spec{Name: "n", TargetTable: "t"}).SetNewDDL("CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY x")
	h1 := a.Hash()
	a.SetNewDDL("CREATE TABLE t (x UInt16) ENGINE = MergeTree ORDER BY x")
	if a.Hash() == h1 {
		t.Fatal("hash should change when the DDL changes")
	}
}
