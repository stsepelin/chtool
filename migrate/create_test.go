package migrate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCreateNumbersFromHighestExisting(t *testing.T) {
	dir := t.TempDir()
	for _, f := range []string{
		"000001_init.up.sql", "000001_init.down.sql",
		"000007_widen.up.sql", // gaps below are none of Create's business
		"notes.md",            // ignored
	} {
		if err := os.WriteFile(filepath.Join(dir, f), []byte("SELECT 1"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	path, err := Create(dir, "add_country")
	if err != nil {
		t.Fatal(err)
	}
	if got := filepath.Base(path); got != "000008_add_country.up.sql" {
		t.Fatalf("next migration = %q, want 000008_add_country.up.sql", got)
	}
}

func TestCreateStartsAtOneInEmptyDir(t *testing.T) {
	path, err := Create(t.TempDir(), "init")
	if err != nil {
		t.Fatal(err)
	}
	if got := filepath.Base(path); got != "000001_init.up.sql" {
		t.Fatalf("first migration = %q, want 000001_init.up.sql", got)
	}
}

// The name charset is what keeps a path separator or ".." out of the filename.
func TestCreateRejectsBadNames(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{
		"", "Add_Country", "add-country", "add country",
		"../escape", "sub/dir", `..\escape`, "add.country",
	} {
		if _, err := Create(dir, name); err == nil {
			t.Errorf("Create(%q) should have been rejected", name)
		}
	}
	// Nothing was written.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("a rejected name must create no files, found %d", len(entries))
	}
}

// O_EXCL: Create must never truncate a migration that already exists.
func TestCreateNeverTruncatesExisting(t *testing.T) {
	dir := t.TempDir()
	existing := filepath.Join(dir, "000001_init.up.sql")
	const body = "ALTER TABLE t ADD COLUMN x UInt8"
	if err := os.WriteFile(existing, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	// Same sequence, same name: the path Create would pick if it miscounted.
	if _, err := Create(dir, "init"); err != nil {
		t.Fatalf("Create should pick 000002 and succeed: %v", err)
	}
	got, err := os.ReadFile(existing)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != body {
		t.Fatalf("existing migration was modified: %q", got)
	}
}

func TestCreateErrorsOnMissingDir(t *testing.T) {
	if _, err := Create(filepath.Join(t.TempDir(), "nope"), "init"); err == nil {
		t.Fatal("a missing migrations dir should be an error, not a silent mkdir")
	}
}

// A scaffolded name must satisfy the pattern schema.Lint enforces.
func TestCreateOutputMatchesLintConvention(t *testing.T) {
	path, err := Create(t.TempDir(), "add_country_tier")
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Base(path)
	if !strings.HasSuffix(base, ".up.sql") || len(base) < 8 || !seqRe.MatchString(base) {
		t.Fatalf("scaffolded name %q does not match the NNNNNN_name.up.sql convention", base)
	}
}
