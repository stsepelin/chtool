package rebuild

import (
	"fmt"
	"io/fs"
	"strings"
)

// CompanionInFS returns a ReconcileGuard that refuses cutover unless the spec's
// companion migration is present in fsys.
//
// Pass the same embed.FS the binary migrates from. That is the point: the
// guarantee you want is that the running binary carries the companion migration,
// which an os.Stat on a working-directory-relative path does not check — it
// asks what happens to be on disk next to wherever the process was started.
//
// A file matches when its name (with the .up.sql suffix removed) equals the
// spec's companion_migration, or when the companion is just the six-digit
// sequence prefix. Both of these match "000042_reorder_events.up.sql":
//
//	companion_migration: 000042_reorder_events
//	companion_migration: "000042"
//
// The guard fails closed: a spec with no companion_migration is an error,
// because a guard that can verify nothing should not silently pass.
func CompanionInFS(fsys fs.FS, spec *Spec) func() error {
	return func() error {
		want := strings.TrimSpace(spec.Companion)
		if want == "" {
			return fmt.Errorf("spec %q declares no companion_migration, so CompanionInFS has nothing to verify", spec.Name)
		}
		entries, err := fs.ReadDir(fsys, ".")
		if err != nil {
			return fmt.Errorf("read migrations fs: %w", err)
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name, ok := strings.CutSuffix(e.Name(), ".up.sql")
			if !ok {
				continue
			}
			if name == want || seqPrefix(name) == want {
				return nil
			}
		}
		return fmt.Errorf("companion migration %q not found in the embedded migrations — the binary does not carry it", want)
	}
}

// seqPrefix returns the leading NNNNNN_ sequence of a migration name, or "".
func seqPrefix(name string) string {
	i := strings.IndexByte(name, '_')
	if i != 6 {
		return ""
	}
	for j := range 6 {
		if name[j] < '0' || name[j] > '9' {
			return ""
		}
	}
	return name[:6]
}
