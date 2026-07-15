package schema

import (
	"strings"
	"testing"
)

// index_granularity = 8192 in the MIDDLE of a settings list must not leave a
// dangling comma (regression for the "SETTINGS, ..." corruption).
func TestNormalizeStripsGranularityMidList(t *testing.T) {
	in := `CREATE TABLE d.t (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1`
	out := Normalize(in)
	if strings.Contains(out, ",") && strings.Contains(out, "SETTINGS ,") {
		t.Fatalf("dangling comma after SETTINGS: %q", out)
	}
	if strings.Contains(out, "index_granularity") {
		t.Fatalf("default granularity not stripped: %q", out)
	}
	if !strings.Contains(out, "ttl_only_drop_parts = 1") {
		t.Fatalf("other settings must survive: %q", out)
	}
	if strings.Contains(out, "SETTINGS,") || strings.Contains(out, ", ,") {
		t.Fatalf("malformed settings list: %q", out)
	}
}

func TestNormalizeGranularityAsLastSetting(t *testing.T) {
	in := `CREATE TABLE d.t (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192`
	out := Normalize(in)
	if strings.Contains(out, "index_granularity") {
		t.Fatalf("granularity not stripped: %q", out)
	}
	if strings.HasSuffix(strings.TrimSpace(out), ",") {
		t.Fatalf("trailing comma left: %q", out)
	}
	if !strings.Contains(out, "ttl_only_drop_parts = 1") {
		t.Fatalf("surviving setting lost: %q", out)
	}
}

func TestNormalizeGranularityOnlySettingDropsClause(t *testing.T) {
	in := `CREATE TABLE d.t (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192`
	out := Normalize(in)
	if strings.Contains(out, "SETTINGS") {
		t.Fatalf("empty SETTINGS clause should be removed: %q", out)
	}
}

// A Cloud engine that carries a semantic argument (ReplacingMergeTree's version
// column) must keep that argument after the replication-path prefix is stripped,
// so it matches the OSS engine that takes the same argument.
func TestNormalizeKeepsSemanticEngineArg(t *testing.T) {
	cloud := `CREATE TABLE d.t (x UInt8, ver UInt64) ENGINE = SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', ver) ORDER BY x`
	oss := `CREATE TABLE d.t (x UInt8, ver UInt64) ENGINE = ReplacingMergeTree(ver) ORDER BY x`
	if Normalize(cloud) != Normalize(oss) {
		t.Fatalf("cloud/oss with version arg differ:\n%q\n%q", Normalize(cloud), Normalize(oss))
	}
	if !strings.Contains(Normalize(cloud), "ReplacingMergeTree(ver)") {
		t.Fatalf("version arg not preserved: %q", Normalize(cloud))
	}
}

func TestNormalizeReplicatedNoArgs(t *testing.T) {
	cloud := `CREATE TABLE d.t (x UInt8) ENGINE = ReplicatedMergeTree('/path', '{replica}') ORDER BY x`
	if got := Normalize(cloud); !strings.Contains(got, "ENGINE = MergeTree") || strings.Contains(got, "Replicated") {
		t.Fatalf("replicated engine not reduced to OSS MergeTree: %q", got)
	}
}

func TestParseNameMarkerDefaultsToTable(t *testing.T) {
	name, kind, ok := parseNameMarker("-- name: my_table")
	if !ok || name != "my_table" || kind != "table" {
		t.Fatalf("got name=%q kind=%q ok=%v", name, kind, ok)
	}
}
