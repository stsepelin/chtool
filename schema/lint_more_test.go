package schema

import "testing"

// A semicolon inside a string literal must not be counted as a statement
// separator (regression for the naive strings.Split on ';').
func TestStatementCountIgnoresSemicolonInLiteral(t *testing.T) {
	issues := lintFiles(t, LintConfig{}, map[string]string{
		"000001_x.up.sql": "ALTER TABLE a MODIFY COLUMN s String DEFAULT 'a;b;c'",
	})
	if has(issues, "statements") {
		t.Errorf("single statement with a ';' in a literal should not trip the multi-statement rule: %v", issues)
	}
}

func TestLintFlagsOnClusterAndPopulate(t *testing.T) {
	issues := lintFiles(t, LintConfig{}, map[string]string{
		"000001_x.up.sql": "CREATE TABLE a ON CLUSTER c (x UInt8) ENGINE = MergeTree ORDER BY x",
		"000002_y.up.sql": "CREATE MATERIALIZED VIEW v TO t AS SELECT x FROM s POPULATE",
	})
	if !has(issues, "ON CLUSTER") {
		t.Error("expected ON CLUSTER flag")
	}
	if !has(issues, "POPULATE") {
		t.Error("expected POPULATE flag")
	}
}

func TestLintDetectsSequenceGapAndMissingUp(t *testing.T) {
	issues := lintFiles(t, LintConfig{}, map[string]string{
		"000001_a.up.sql":   "SELECT 1",
		"000003_c.up.sql":   "SELECT 1",
		"000004_d.down.sql": "SELECT 1", // down without up
	})
	if !has(issues, "gap in migration sequence") {
		t.Errorf("expected a gap at 000002: %v", issues)
	}
	if !has(issues, "missing .up.sql") {
		t.Errorf("expected missing up for 000004: %v", issues)
	}
}

func TestLintRejectsBadFilename(t *testing.T) {
	issues := lintFiles(t, LintConfig{}, map[string]string{
		"nonsense.sql": "SELECT 1",
	})
	if !has(issues, "does not match") {
		t.Errorf("expected filename-pattern issue: %v", issues)
	}
}
