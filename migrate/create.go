package migrate

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
)

// nameRe is the slug charset schema.Lint accepts in NNNNNN_name.up.sql. Keeping
// Create and Lint on the same charset means a scaffolded file always passes the
// linter; it also makes a path separator or ".." unrepresentable in a name.
var nameRe = regexp.MustCompile(`^[a-z0-9_]+$`)

// seqRe matches any migration file so Create can find the highest sequence.
var seqRe = regexp.MustCompile(`^(\d{6})_[a-z0-9_]+\.(?:up|down)\.sql$`)

// Create scaffolds the next migration in dir and returns its path.
//
// The sequence number is one past the highest already present (000001 in an
// empty directory), so the run stays gapless the way schema.Lint requires. name
// must be a lowercase slug of [a-z0-9_] — the same charset Lint accepts, which
// also rules out path separators and "..". The file is created with O_EXCL, so
// Create never truncates an existing migration.
//
// Only the .up.sql file is written. A .down.sql is optional (absent means the
// revert is a no-op), and an empty one is worse than none: ClickHouse rejects an
// empty statement at runtime as "Empty query". Add one when you have a real
// statement to put in it.
func Create(dir, name string) (path string, err error) {
	if !nameRe.MatchString(name) {
		return "", fmt.Errorf("migration name %q must be a lowercase slug of [a-z0-9_]", name)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", fmt.Errorf("read migrations dir: %w", err)
	}

	var highest uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		m := seqRe.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		if n, err := strconv.ParseUint(m[1], 10, 64); err == nil && n > highest {
			highest = n
		}
	}

	path = filepath.Join(dir, fmt.Sprintf("%06d_%s.up.sql", highest+1, name))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return "", fmt.Errorf("create migration: %w", err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("create migration: %w", err)
	}
	return path, nil
}
