package schema

import (
	"cmp"
	"fmt"
	"io/fs"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

type LintConfig struct {
	// GrandfatherBelow exempts migrations numbered <= this from the
	// one-statement and destructive-marker rules (for a pre-existing baseline
	// that must not be rewritten). Zero enforces on all files.
	GrandfatherBelow uint
}

type Issue struct {
	File string
	Msg  string
}

func (i Issue) String() string { return fmt.Sprintf("%s: %s", i.File, i.Msg) }

var (
	fileRe         = regexp.MustCompile(`^(\d{6})_([a-z0-9_]+)\.(up|down)\.sql$`)
	onClusterRe    = regexp.MustCompile(`(?i)\bon\s+cluster\b`)
	populateRe     = regexp.MustCompile(`(?i)\bpopulate\b`)
	destructiveRe  = regexp.MustCompile(`(?i)\b(drop\s+table|drop\s+column|truncate|modify\s+column)\b`)
	ackMarkerRe    = regexp.MustCompile(`(?im)^\s*--\s*destructive:\s*acknowledged\s*$`)
	lineCommentRe  = regexp.MustCompile(`(?m)--.*$`)
	blockCommentRe = regexp.MustCompile(`(?s)/\*.*?\*/`)
)

type pair struct{ up, down bool }

// Lint checks every NNNNNN_name.{up,down}.sql file in fsys:
//
//   - filenames match the sequence pattern and form a gapless run;
//   - every sequence has an up file (down is optional — absent means no-op);
//   - no ON CLUSTER / POPULATE anywhere;
//   - a non-empty file has at least one real statement (a comment-only file is
//     rejected by ClickHouse as "Empty query" at runtime);
//   - exactly one statement per file, and destructive statements carry a
//     `-- destructive: acknowledged` marker — both only for files above
//     cfg.GrandfatherBelow.
func Lint(fsys fs.FS, cfg LintConfig) ([]Issue, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}
	seqs := map[uint]*pair{}
	var issues []Issue
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		name := e.Name()
		m := fileRe.FindStringSubmatch(name)
		if m == nil {
			issues = append(issues, Issue{name, "filename does not match NNNNNN_name.{up,down}.sql"})
			continue
		}
		seq64, _ := strconv.ParseUint(m[1], 10, 64)
		seq := uint(seq64)
		if seqs[seq] == nil {
			seqs[seq] = &pair{}
		}
		if m[3] == "up" {
			seqs[seq].up = true
		} else {
			seqs[seq].down = true
		}
		body, err := fs.ReadFile(fsys, name)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", name, err)
		}
		issues = append(issues, lintBody(name, seq, m[3], string(body), cfg)...)
	}
	issues = append(issues, checkSequence(seqs)...)
	slices.SortFunc(issues, func(a, b Issue) int {
		return cmp.Or(cmp.Compare(a.File, b.File), cmp.Compare(a.Msg, b.Msg))
	})
	return issues, nil
}

func lintBody(name string, seq uint, direction, body string, cfg LintConfig) []Issue {
	var issues []Issue
	if onClusterRe.MatchString(body) {
		issues = append(issues, Issue{name, "contains ON CLUSTER (Cloud replicates transparently)"})
	}
	if populateRe.MatchString(body) {
		issues = append(issues, Issue{name, "contains POPULATE (use an explicit backfill)"})
	}
	if strings.TrimSpace(body) != "" && statementCount(body) == 0 {
		issues = append(issues, Issue{name, `comment-only file errors at runtime as "Empty query" — delete it (absent = no-op) or add a statement`})
	}
	if seq <= cfg.GrandfatherBelow {
		return issues
	}
	if n := statementCount(body); n > 1 {
		issues = append(issues, Issue{name, fmt.Sprintf("contains %d statements; exactly one per file (DDL is non-transactional)", n)})
	}
	if direction == "up" && destructiveRe.MatchString(stripComments(body)) && !ackMarkerRe.MatchString(body) {
		issues = append(issues, Issue{name, "destructive statement requires a `-- destructive: acknowledged` marker"})
	}
	return issues
}

// statementCount counts semicolon-separated statements, ignoring semicolons
// inside string/identifier quotes.
func statementCount(body string) int {
	s := stripComments(body)
	count := 0
	var cur strings.Builder
	var quote byte
	flush := func() {
		if strings.TrimSpace(cur.String()) != "" {
			count++
		}
		cur.Reset()
	}
	for i := range len(s) {
		c := s[i]
		if quote != 0 {
			cur.WriteByte(c)
			if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			quote = c
			cur.WriteByte(c)
		case ';':
			flush()
		default:
			cur.WriteByte(c)
		}
	}
	flush()
	return count
}

func stripComments(s string) string {
	return lineCommentRe.ReplaceAllString(blockCommentRe.ReplaceAllString(s, ""), "")
}

func checkSequence(seqs map[uint]*pair) []Issue {
	if len(seqs) == 0 {
		return nil
	}
	nums := slices.Sorted(maps.Keys(seqs))
	var issues []Issue
	for _, n := range nums {
		if !seqs[n].up {
			issues = append(issues, Issue{fmt.Sprintf("%06d", n), "missing .up.sql file"})
		}
	}
	for i := nums[0]; i <= nums[len(nums)-1]; i++ {
		if seqs[i] == nil {
			issues = append(issues, Issue{fmt.Sprintf("%06d", i), "gap in migration sequence"})
		}
	}
	return issues
}
