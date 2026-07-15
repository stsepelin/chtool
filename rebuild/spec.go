package rebuild

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"gopkg.in/yaml.v3"
)

// Spec describes one rebuild. Load it from a directory (spec.yaml + the new DDL
// file) with LoadSpec, or construct it in code and call SetNewDDL.
type Spec struct {
	// Name is a stable identifier; it keys the operation's state.
	Name string `yaml:"name"`
	// TargetTable is the AggregatingMergeTree being rebuilt.
	TargetTable string `yaml:"target_table"`
	// NewDDLFile is the co-located file holding the new CREATE TABLE (authored
	// for the real target name; the rebuilder retargets it to <target>_v2).
	NewDDLFile string `yaml:"new_ddl"`
	// BoundaryColumn is the immutable, producer-set event time that partitions
	// events at T (e.g. created_at). T is formatted UTC and compared as a string
	// literal (parsed in the column's timezone); prefer a UTC DateTime column so
	// the cutover instant matches the intended wall clock.
	BoundaryColumn string `yaml:"boundary_column"`
	// ChunkColumn is the column the backfill iterates over, one value per chunk
	// (e.g. the partition/day column). Defaults to "date".
	ChunkColumn string `yaml:"chunk_column"`
	// MVs are the materialized views that feed the target.
	MVs []string `yaml:"mvs"`
	// Validations are aggregate expressions compared old-vs-new (e.g.
	// "sum(views)"); a mismatch fails the rebuild.
	Validations []string `yaml:"validations"`
	// Companion is optional metadata (e.g. a migration name) for the caller's
	// reconcile check; the rebuilder does not interpret it.
	Companion string `yaml:"companion_migration"`
	// RehearsalVer, if set, is the server version this spec was rehearsed
	// against; Plan refuses a different version unless forced.
	RehearsalVer string `yaml:"dress_rehearsal_version"`
	// Backfill tunes the memory-safe backfill.
	Backfill BackfillConfig `yaml:"backfill"`

	newDDL string
	hash   string
}

// LoadSpec reads spec.yaml and the referenced new DDL from a directory.
func LoadSpec(dir string) (*Spec, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "spec.yaml"))
	if err != nil {
		return nil, fmt.Errorf("read spec: %w", err)
	}
	var s Spec
	if err := yaml.Unmarshal(raw, &s); err != nil {
		return nil, fmt.Errorf("parse spec: %w", err)
	}
	ddl, err := os.ReadFile(filepath.Join(dir, s.NewDDLFile))
	if err != nil {
		return nil, fmt.Errorf("read new_ddl %s: %w", s.NewDDLFile, err)
	}
	s.newDDL = string(ddl)
	s.hash = hashBytes(raw, ddl)
	if err := s.validate(); err != nil {
		return nil, err
	}
	return &s, nil
}

// SetNewDDL sets the new target DDL for a programmatically-built Spec and
// recomputes the content hash. Returns the spec for chaining.
func (s *Spec) SetNewDDL(ddl string) *Spec {
	s.newDDL = ddl
	s.hash = hashBytes([]byte(s.Name+s.TargetTable), []byte(ddl))
	return s
}

func hashBytes(a, b []byte) string {
	sum := sha256.Sum256(append(append([]byte{}, a...), b...))
	return hex.EncodeToString(sum[:])
}

func (s *Spec) validate() error {
	switch {
	case s.Name == "":
		return fmt.Errorf("spec.name is required")
	case s.TargetTable == "":
		return fmt.Errorf("spec.target_table is required")
	case s.newDDL == "":
		return fmt.Errorf("spec.new_ddl is required (or call SetNewDDL)")
	case s.BoundaryColumn == "":
		return fmt.Errorf("spec.boundary_column is required")
	case len(s.MVs) == 0:
		return fmt.Errorf("spec.mvs must list the materialized views feeding the target")
	}
	return nil
}

func (s *Spec) chunkColumn() string {
	if s.ChunkColumn == "" {
		return "date"
	}
	return s.ChunkColumn
}

// Hash is the spec's content hash. OpID keys its operation state.
func (s *Spec) Hash() string    { return s.hash }
func (s *Spec) OpID() string    { return "rebuild:" + s.Name }
func (s *Spec) V2Table() string { return s.TargetTable + "_v2" }

// BackupTable is the dated name the old target is renamed to at cutover.
func (s *Spec) BackupTable(day time.Time) string {
	return fmt.Sprintf("%s_backup_%s", s.TargetTable, day.UTC().Format("20060102"))
}

// NewDDLForV2 returns the new DDL retargeted to the v2 table name. It rewrites
// the CREATE TABLE clause (not the first textual match), so leading comments
// mentioning the table are safe, and forces IF NOT EXISTS so the create step is
// idempotent across resumes regardless of how the source DDL was written.
func (s *Spec) NewDDLForV2() (string, error) {
	re := regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?` + regexp.QuoteMeta(s.TargetTable) + `\b`)
	out := re.ReplaceAllString(s.newDDL, "CREATE TABLE IF NOT EXISTS "+s.V2Table())
	if out == s.newDDL {
		return "", fmt.Errorf("new_ddl has no `CREATE TABLE %s` statement to retarget", s.TargetTable)
	}
	return out, nil
}
