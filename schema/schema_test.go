package schema

import (
	"strings"
	"testing"
	"testing/fstest"
)

func TestNormalizeSharedEngineMapsToOSS(t *testing.T) {
	cloud := `CREATE TABLE d.t (date Date) ENGINE = SharedAggregatingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY date SETTINGS index_granularity = 8192`
	oss := `CREATE TABLE d.t (date Date) ENGINE = AggregatingMergeTree ORDER BY date SETTINGS index_granularity = 8192`
	if Normalize(cloud) != Normalize(oss) {
		t.Fatalf("cloud/oss differ:\n%q\n%q", Normalize(cloud), Normalize(oss))
	}
	if strings.Contains(Normalize(cloud), "Shared") {
		t.Fatalf("Shared* not stripped: %q", Normalize(cloud))
	}
}

func TestRenderParseRoundTrip(t *testing.T) {
	objs := []Object{
		{Name: "t", DDL: "CREATE TABLE t\n(\n x UInt8\n)\nENGINE = MergeTree\nORDER BY x"},
		{Name: "v", IsMV: true, DDL: "CREATE MATERIALIZED VIEW v TO t\nAS SELECT x FROM s"},
	}
	got, err := Parse(Render(objs))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Name != "t" || !got[1].IsMV || got[0].DDL != objs[0].DDL {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

func TestDiff(t *testing.T) {
	want := []Object{{Name: "a", DDL: "x"}, {Name: "b", DDL: "v1"}, {Name: "c", DDL: "z"}}
	got := []Object{{Name: "a", DDL: "x"}, {Name: "b", DDL: "v2"}, {Name: "d", DDL: "z"}}
	if r := Diff(want, got, nil); len(r) != 3 {
		t.Fatalf("expected 3 diffs, got %d: %v", len(r), r)
	}
	if r := Diff(want, got, map[string]bool{"b": true, "c": true, "d": true}); len(r) != 0 {
		t.Fatalf("ignored diffs should vanish: %v", r)
	}
}

func lintFiles(t *testing.T, cfg LintConfig, files map[string]string) []Issue {
	t.Helper()
	fsys := fstest.MapFS{}
	for n, b := range files {
		fsys[n] = &fstest.MapFile{Data: []byte(b)}
	}
	issues, err := Lint(fsys, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return issues
}

func has(issues []Issue, sub string) bool {
	for _, i := range issues {
		if strings.Contains(i.Msg, sub) {
			return true
		}
	}
	return false
}

func TestLintRules(t *testing.T) {
	issues := lintFiles(t, LintConfig{}, map[string]string{
		"000001_x.up.sql":   "ALTER TABLE a ADD COLUMN x UInt8; ALTER TABLE b ADD COLUMN y UInt8;",
		"000001_x.down.sql": "--DROP TABLE a;",
	})
	if !has(issues, "statements") {
		t.Error("expected multi-statement issue")
	}
	if !has(issues, "Empty query") {
		t.Error("expected comment-only issue")
	}
}

func TestLintGrandfather(t *testing.T) {
	issues := lintFiles(t, LintConfig{GrandfatherBelow: 20}, map[string]string{
		"000015_x.up.sql":   "ALTER TABLE a ADD COLUMN x UInt8; ALTER TABLE b ADD COLUMN y UInt8;",
		"000015_x.down.sql": "",
	})
	if has(issues, "statements") {
		t.Errorf("grandfathered file should not be flagged: %v", issues)
	}
}

func TestLintDestructiveMarker(t *testing.T) {
	without := lintFiles(t, LintConfig{}, map[string]string{"000021_d.up.sql": "ALTER TABLE a DROP COLUMN x;"})
	if !has(without, "destructive") {
		t.Error("expected destructive-marker requirement")
	}
	with := lintFiles(t, LintConfig{}, map[string]string{"000021_d.up.sql": "-- destructive: acknowledged\nALTER TABLE a DROP COLUMN x;"})
	if has(with, "destructive") {
		t.Error("marker should satisfy the rule")
	}
}
