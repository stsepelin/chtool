package rebuild

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const tsLayout = "2006-01-02 15:04:05"

// esc escapes a value for a single-quoted ClickHouse string literal. Chunk
// values come from live column data, so both backslash (CH's escape char) and
// the quote must be escaped.
func esc(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, `'`, `\'`)
}

// MV is a parsed materialized-view definition, decomposed so the rebuilder can
// re-emit an armed v2 MV (with a boundary WHERE) and the exact-complement backfill.
// Only the canonical shape is supported:
//
//	CREATE MATERIALIZED VIEW db.name TO db.target (cols) AS SELECT proj FROM db.src GROUP BY keys
//
// An MV that already carries a WHERE is rejected — combining an existing filter
// with the boundary is out of scope and would break the exact-complement rule.
type MV struct {
	Name       string
	Target     string
	Columns    string
	Projection string
	Source     string
	GroupBy    string
	raw        string
}

var mvRe = regexp.MustCompile(`(?is)^CREATE MATERIALIZED VIEW\s+(\S+)\s+TO\s+(\S+)\s+\((.*)\)\s+AS\s+SELECT\s+(.*)\s+FROM\s+(\S+)\s+GROUP BY\s+(.*?)\s*;?\s*$`)

// FetchMV reads and parses a materialized view's definition from the server.
func FetchMV(ctx context.Context, conn Conn, db, name string) (*MV, error) {
	rows, err := conn.Query(ctx,
		"SELECT create_table_query FROM system.tables WHERE database = ? AND name = ?", db, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("materialized view %q not found in %q", name, db)
	}
	var raw string
	if err := rows.Scan(&raw); err != nil {
		return nil, err
	}
	return parseMV(raw)
}

func parseMV(raw string) (*MV, error) {
	m := mvRe.FindStringSubmatch(strings.TrimSpace(raw))
	if m == nil {
		return nil, fmt.Errorf("unsupported materialized view shape (need CREATE MATERIALIZED VIEW … TO … (…) AS SELECT … FROM … GROUP BY …):\n%s", raw)
	}
	proj := m[4]
	if regexp.MustCompile(`(?is)\bWHERE\b`).MatchString(proj) {
		return nil, fmt.Errorf("materialized view already contains a WHERE clause; not supported")
	}
	// A nested FROM in the projection would be mis-parsed by the greedy regex
	// into the wrong source/group-by; reject rather than backfill incorrectly.
	if regexp.MustCompile(`(?is)\bFROM\b`).MatchString(proj) {
		return nil, fmt.Errorf("materialized view projection contains a nested FROM/subquery; not supported")
	}
	name := m[1]
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		name = name[i+1:]
	}
	name = strings.Trim(name, "`")
	return &MV{Name: name, Target: m[2], Columns: m[3], Projection: proj, Source: m[5], GroupBy: m[6], raw: raw}, nil
}

func boundaryPred(col string, t time.Time, ge bool) string {
	op := "<"
	if ge {
		op = ">="
	}
	return fmt.Sprintf("%s %s '%s'", col, op, t.UTC().Format(tsLayout))
}

// V2CreateSQL renders the armed-but-idle v2 MV: identical projection/source/
// group-by, retargeted to v2Target, capturing only events at/after T.
func (m *MV) V2CreateSQL(db, v2Name, v2Target, boundaryCol string, t time.Time) string {
	return fmt.Sprintf(
		"CREATE MATERIALIZED VIEW %s.%s TO %s (%s) AS SELECT %s FROM %s WHERE %s GROUP BY %s",
		db, v2Name, v2Target, m.Columns, m.Projection, m.Source, boundaryPred(boundaryCol, t, true), m.GroupBy)
}

// BackfillSQL renders the historical backfill for one source: the MV projection
// verbatim with the boundary predicate FLIPPED (events strictly before T), an
// optional chunk predicate, and a settings clause. The INSERT names the target
// columns the MV writes (a subset of the target).
func (m *MV) BackfillSQL(v2Target, boundaryCol string, t time.Time, chunkPred, settingsClause string) string {
	where := boundaryPred(boundaryCol, t, false)
	if chunkPred != "" {
		where += " AND " + chunkPred
	}
	cols := m.ColumnNames()
	for i := range cols {
		cols[i] = "`" + cols[i] + "`"
	}
	return fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s WHERE %s GROUP BY %s%s",
		v2Target, strings.Join(cols, ", "), m.Projection, m.Source, where, m.GroupBy, settingsClause)
}

// BucketExpr is the deterministic hash expression splitting a chunk into N
// sub-chunks; defaults to hashing the GROUP BY keys.
func (m *MV) BucketExpr(override string) string {
	if override != "" {
		return override
	}
	return "cityHash64(" + m.GroupBy + ")"
}

// CountSQL counts source rows for one chunk value strictly before T.
func (m *MV) CountSQL(boundaryCol, chunkCol, chunkVal string, t time.Time) string {
	return fmt.Sprintf("SELECT count() FROM %s WHERE %s AND %s = '%s'",
		m.Source, boundaryPred(boundaryCol, t, false), chunkCol, esc(chunkVal))
}

// SelectForProbe renders the aggregation SELECT for a chunk with FORMAT Null.
func (m *MV) SelectForProbe(boundaryCol string, t time.Time, chunkPred, settingsClause string) string {
	where := boundaryPred(boundaryCol, t, false)
	if chunkPred != "" {
		where += " AND " + chunkPred
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s GROUP BY %s FORMAT Null%s",
		m.Projection, m.Source, where, m.GroupBy, settingsClause)
}

// OriginalCreateSQL is the unmodified MV definition (to recreate at cutover).
func (m *MV) OriginalCreateSQL() string { return m.raw }

// SourceName returns the bare source table name.
func (m *MV) SourceName() string {
	s := m.Source
	if i := strings.LastIndexByte(s, '.'); i >= 0 {
		s = s[i+1:]
	}
	return strings.Trim(s, "`")
}

// ColumnNames extracts the target column names from the MV's column-list block.
func (m *MV) ColumnNames() []string {
	var names []string
	for _, part := range splitTopLevel(m.Columns) {
		fields := strings.Fields(strings.TrimSpace(part))
		if len(fields) == 0 {
			continue
		}
		names = append(names, strings.Trim(fields[0], "`"))
	}
	return names
}

func splitTopLevel(s string) []string {
	var out []string
	depth, start := 0, 0
	for i, r := range s {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				out = append(out, s[start:i])
				start = i + 1
			}
		}
	}
	return append(out, s[start:])
}
