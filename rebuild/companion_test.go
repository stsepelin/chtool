package rebuild

import (
	"strings"
	"testing"
	"testing/fstest"
)

func companionFS() fstest.MapFS {
	return fstest.MapFS{
		"000041_earlier.up.sql":          {Data: []byte("SELECT 1")},
		"000042_reorder_events.up.sql":   {Data: []byte("SELECT 1")},
		"000042_reorder_events.down.sql": {Data: []byte("SELECT 1")},
	}
}

func TestCompanionInFSMatchesFullNameAndSeqPrefix(t *testing.T) {
	for _, companion := range []string{"000042_reorder_events", "000042"} {
		spec := &Spec{Name: "s", Companion: companion}
		if err := CompanionInFS(companionFS(), spec)(); err != nil {
			t.Errorf("companion %q should match: %v", companion, err)
		}
	}
}

func TestCompanionInFSFailsWhenAbsent(t *testing.T) {
	spec := &Spec{Name: "s", Companion: "000099_never_written"}
	err := CompanionInFS(companionFS(), spec)()
	if err == nil {
		t.Fatal("a companion absent from the FS must block cutover")
	}
	if !strings.Contains(err.Error(), "does not carry it") {
		t.Fatalf("error should say the binary lacks the migration: %v", err)
	}
}

// A .down.sql alone must not satisfy the guard: the forward migration is the
// companion.
func TestCompanionInFSIgnoresDownOnly(t *testing.T) {
	fsys := fstest.MapFS{"000042_reorder_events.down.sql": {Data: []byte("SELECT 1")}}
	if err := CompanionInFS(fsys, &Spec{Name: "s", Companion: "000042"})(); err == nil {
		t.Fatal("a down-only migration should not satisfy the guard")
	}
}

// The guard runs immediately before cutover, so a nil spec must be a refusal
// rather than a panic mid-swap.
func TestCompanionInFSFailsOnNilSpec(t *testing.T) {
	guard := CompanionInFS(companionFS(), nil)
	err := guard()
	if err == nil {
		t.Fatal("a nil spec must be refused, not accepted")
	}
	if !strings.Contains(err.Error(), "nil spec") {
		t.Fatalf("error should name the cause: %v", err)
	}
}

// Fail closed: a guard that can verify nothing must not silently pass.
func TestCompanionInFSFailsWithoutCompanion(t *testing.T) {
	if err := CompanionInFS(companionFS(), &Spec{Name: "s"})(); err == nil {
		t.Fatal("a spec with no companion_migration should error, not pass")
	}
}

// It plugs into the Orchestrator's ReconcileGuard as-is.
func TestCompanionInFSSatisfiesReconcileGuard(t *testing.T) {
	o := &Orchestrator{}
	o.ReconcileGuard = CompanionInFS(companionFS(), &Spec{Name: "s", Companion: "000042"})
	if err := o.ReconcileGuard(); err != nil {
		t.Fatalf("guard should pass: %v", err)
	}
}

func TestSeqPrefix(t *testing.T) {
	cases := map[string]string{
		"000042_reorder": "000042",
		"000042":         "", // no underscore
		"42_short":       "", // not six digits
		"abcdef_name":    "", // not digits
		"0000421_name":   "", // underscore not at index 6
	}
	for in, want := range cases {
		if got := seqPrefix(in); got != want {
			t.Errorf("seqPrefix(%q) = %q, want %q", in, got, want)
		}
	}
}
