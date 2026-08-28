// Package structs provides generic, reflection-based helpers for working with
// ClickHouse rows as Go structs tagged with `ch:"column"` (the tag
// clickhouse-go/v2 uses). It has no dependency on any particular schema.
package structs

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Conn = driver.Conn

// Insert batch-inserts rows into table (may be db-qualified). A nil/empty slice
// is a no-op.
//
// The statement names its columns explicitly — INSERT INTO t (`a`, `b`) — built
// from T's `ch:` tags. That decides which struct-vs-table mismatches are safe:
//
//   - A column in the table that T does not tag is left out of the statement,
//     so ClickHouse fills it with its DEFAULT (or the type zero if it has
//     none). Adding a column is therefore a no-op for existing writers, and
//     migrations may ship ahead of the structs that write the table.
//   - A `ch:` tag naming a column the table does not have is an error from the
//     server. It cannot be quietly skipped: the value has nowhere to go, and a
//     silent drop is the failure this package exists to avoid. Dropping a
//     column consequently needs the struct to stop tagging it first — the
//     mirror image of the rule above.
//   - A `ch:` tag naming a MATERIALIZED or ALIAS column is likewise an error,
//     because the server computes those and refuses writes to them. Tag them
//     only on structs used for reads.
//
// An exported field with no `ch` tag is not written, even where clickhouse-go
// would match it against a column by Go field name.
func Insert[T any](ctx context.Context, conn Conn, table string, rows []T) error {
	if len(rows) == 0 {
		return nil
	}
	cols, err := columnsFor(rowType[T]())
	if err != nil {
		return fmt.Errorf("insert %s: %w", table, err)
	}
	batch, err := conn.PrepareBatch(ctx, insertQuery(table, cols))
	if err != nil {
		return fmt.Errorf("prepare batch %s: %w", table, err)
	}
	if err := verifyBatchColumns(batch, cols); err != nil {
		_ = batch.Abort()
		return fmt.Errorf("prepare batch %s: %w", table, err)
	}
	for i := range rows {
		if err := batch.AppendStruct(&rows[i]); err != nil {
			_ = batch.Abort()
			return fmt.Errorf("append row %d: %w", i, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch %s: %w", table, err)
	}
	return nil
}

// insertQuery renders INSERT INTO table (`a`, `b`).
func insertQuery(table string, cols []Column) string {
	var b strings.Builder
	b.Grow(len(table) + 16*len(cols) + 16)
	b.WriteString("INSERT INTO ")
	b.WriteString(table)
	// The space before "(" is load-bearing: clickhouse-go only recognises a
	// column list preceded by whitespace, and a list it fails to recognise
	// falls back to every column of the table on the HTTP protocol, which
	// misaligns the batch rather than failing.
	b.WriteString(" (")
	for i, c := range cols {
		if i > 0 {
			b.WriteString(", ")
		}
		// Backticks, not double quotes: the driver strips the former from the
		// parsed list correctly and mangles the latter on dotted identifiers.
		// checkName has already rejected anything that would escape them.
		b.WriteByte('`')
		b.WriteString(c.Name)
		b.WriteByte('`')
	}
	b.WriteByte(')')
	return b.String()
}

// verifyBatchColumns checks that the driver ended up with the columns the
// statement named. It re-reads the list out of the query text with a regex
// rather than tracking the quoting, and a list it cannot read is not an error
// to it — it falls back to every column of the table, which is the behaviour
// this package moved away from.
//
// That fallback does not corrupt anything: AppendStruct still matches columns
// by name, so the batch either writes what it would have anyway or fails on a
// column the struct lacks. What it does do is quietly withdraw the guarantee
// Insert documents, turning an added column back into "missing destination
// name <some unrelated column>". Comparing against the driver's own answer
// makes that a statement about the actual column list instead, and needs no
// model of the parser here to stay in step with the dependency.
func verifyBatchColumns(batch driver.Batch, cols []Column) error {
	got := batch.Columns()
	mismatch := len(got) != len(cols)
	for i := 0; !mismatch && i < len(got); i++ {
		mismatch = got[i].Name() != cols[i].Name
	}
	if !mismatch {
		return nil
	}
	names := make([]string, len(got))
	for i, c := range got {
		names[i] = c.Name()
	}
	want := make([]string, len(cols))
	for i, c := range cols {
		want[i] = c.Name
	}
	return fmt.Errorf("named columns [%s] but the driver resolved [%s]; a column name here is one clickhouse-go cannot read back out of the statement, so it fell back to the whole table",
		strings.Join(want, ", "), strings.Join(names, ", "))
}

type Column struct {
	Field  string
	Name   string
	GoType string
	typ    reflect.Type
	chType string
}

// Columns reflects T's `ch:`-tagged fields in declaration order, skipping fields
// with no `ch` tag or `ch:"-"`. Fields of an embedded struct are spliced in at
// the position of the embedded field, matching how clickhouse-go flattens them.
//
// Returns nil for a struct whose tags cannot be resolved; Insert and CreateDDL
// report the reason.
func Columns[T any]() []Column {
	cols, err := columnsFor(rowType[T]())
	if err != nil {
		return nil
	}
	// The cached slice is shared, so hand callers their own copy.
	return slices.Clone(cols)
}

func rowType[T any]() reflect.Type {
	t := reflect.TypeFor[T]()
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}

// resolution is one struct type's cached column list, or the reason it has none.
type resolution struct {
	cols []Column
	err  error
}

// columnCache memoises reflection per struct type. The key is a compile-time
// property of the type, so nothing can invalidate an entry.
var columnCache sync.Map // reflect.Type -> resolution

func columnsFor(t reflect.Type) ([]Column, error) {
	if r, ok := columnCache.Load(t); ok {
		res := r.(resolution)
		return res.cols, res.err
	}
	cols, err := resolveColumns(t, nil, map[reflect.Type]bool{})
	if err == nil && len(cols) == 0 {
		err = fmt.Errorf("no ch:-tagged fields on %s", t)
	}
	if err == nil {
		err = checkDuplicates(cols)
	}
	if err != nil {
		cols = nil
	}
	columnCache.Store(t, resolution{cols, err})
	return cols, err
}

// resolveColumns walks t the way clickhouse-go's own struct indexer does, so
// that the column list names exactly the fields the driver will look for. Where
// the driver would quietly contribute nothing for a field, this returns an
// error instead: a value that cannot reach the table must be loud.
func resolveColumns(t reflect.Type, prefix []string, path map[reflect.Type]bool) ([]Column, error) {
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%s is not a struct", t)
	}
	if path[t] {
		// Pointer embedding can be cyclic and still be legal Go — a linked-list
		// node embeds *Node. clickhouse-go never follows an embedded pointer, so
		// a type already on the path contributes nothing more.
		return nil, nil
	}
	path[t] = true
	defer delete(path, t)

	var cols []Column
	for i := range t.NumField() {
		f := t.Field(i)
		tag := f.Tag.Get("ch")
		if tag == "-" {
			continue
		}
		fieldPath := append(slices.Clip(prefix), f.Name)

		if f.Anonymous {
			// The driver flattens an embedded struct, ignoring any tag on the
			// embedded field itself, so a tag here names a column that would
			// never be written — the field is not one value to the driver, it
			// is however many its type carries.
			if tag != "" {
				return nil, fmt.Errorf(
					"embedded field %s is tagged ch:%q, but clickhouse-go flattens embedded structs and ignores that tag, so no such column would be written; tag the fields inside %s instead, or use ch:\"-\" to skip it",
					strings.Join(fieldPath, "."), tag, f.Type)
			}
			et := f.Type
			if et.Kind() == reflect.Pointer {
				// The driver drops an embedded pointer struct entirely, tags and
				// all, so anything tagged under one would never be written.
				if et.Elem().Kind() != reflect.Struct {
					// Nothing can be tagged under a non-struct, so nothing is lost.
					continue
				}
				inner, err := resolveColumns(et.Elem(), fieldPath, path)
				if err != nil {
					return nil, err
				}
				if len(inner) > 0 {
					return nil, fmt.Errorf(
						"embedded pointer %s carries ch:-tagged fields (%s), which clickhouse-go never writes; embed it by value",
						strings.Join(fieldPath, "."), inner[0].Name)
				}
				continue
			}
			if et.Kind() != reflect.Struct {
				return nil, fmt.Errorf("embedded %s is %s, not a struct; clickhouse-go panics on it", strings.Join(fieldPath, "."), et.Kind())
			}
			inner, err := resolveColumns(et, fieldPath, path)
			if err != nil {
				return nil, err
			}
			cols = append(cols, inner...)
			continue
		}

		if tag == "" {
			continue
		}
		if f.PkgPath != "" {
			return nil, fmt.Errorf("field %s is unexported but tagged ch:%q; clickhouse-go skips it, so it would never be written", strings.Join(fieldPath, "."), tag)
		}
		if err := checkName(tag); err != nil {
			return nil, fmt.Errorf("field %s: %w", strings.Join(fieldPath, "."), err)
		}
		cols = append(cols, Column{
			Field: strings.Join(fieldPath, "."), Name: tag, GoType: f.Type.String(),
			typ: f.Type, chType: f.Tag.Get("chtype"),
		})
	}
	return cols, nil
}

// checkName rejects tags that would not survive the trip through clickhouse-go
// and ClickHouse as the name they were written as. These three corrupt the name
// itself; anything else that goes wrong with a column list is caught after the
// fact by verifyBatchColumns, which compares against the driver's own answer
// rather than guessing at its parser.
func checkName(tag string) error {
	switch {
	case strings.Contains(tag, ","):
		// The driver splits the list it re-reads on commas, so a name
		// containing one arrives as two.
		return fmt.Errorf("ch:%q contains a comma; clickhouse-go uses the whole tag as the column name and splits the column list on commas", tag)
	case strings.Contains(tag, "`"):
		// It strips every backtick from the list, not just the quoting pair.
		return fmt.Errorf("ch:%q contains a backtick, which clickhouse-go strips wherever it appears in a column list", tag)
	case strings.Contains(tag, `\`):
		// ClickHouse reads a backslash inside backticks as an escape, so the
		// server resolves a different name (or, for a trailing one, an
		// unterminated identifier). Escaping it here would not help: the driver
		// strips only backticks when it re-reads the list, so its idea of the
		// column name would then differ from the server's.
		return fmt.Errorf(`ch:%q contains a backslash, which ClickHouse treats as an escape inside a quoted identifier`, tag)
	}
	return nil
}

func checkDuplicates(cols []Column) error {
	seen := make(map[string]string, len(cols))
	for _, c := range cols {
		if first, dup := seen[c.Name]; dup {
			return fmt.Errorf("column %q is claimed by both %s and %s", c.Name, first, c.Field)
		}
		seen[c.Name] = c.Field
	}
	return nil
}

type Diff struct {
	Column string
	Issue  string
}

func (d Diff) String() string { return fmt.Sprintf("%s: %s", d.Column, d.Issue) }

// VerifyTags compares T's `ch:` tags against the live columns of db.table. It
// reports struct columns missing from the table and table columns with no
// struct field. An empty slice means the struct and table agree on column set.
//
// Note that only the first of those is fatal to Insert; see its documentation.
func VerifyTags[T any](ctx context.Context, conn Conn, db, table string) ([]Diff, error) {
	cols, err := columnsFor(rowType[T]())
	if err != nil {
		return nil, err
	}
	live, err := liveColumns(ctx, conn, db, table)
	if err != nil {
		return nil, err
	}
	structCols := map[string]bool{}
	var diffs []Diff
	for _, c := range cols {
		structCols[c.Name] = true
		if !live[c.Name] {
			diffs = append(diffs, Diff{c.Name, "in struct but missing from table"})
		}
	}
	for name := range live {
		if !structCols[name] {
			diffs = append(diffs, Diff{name, "in table but not in struct"})
		}
	}
	return diffs, nil
}

func liveColumns(ctx context.Context, conn Conn, db, table string) (map[string]bool, error) {
	rows, err := conn.Query(ctx, "SELECT name FROM system.columns WHERE database = ? AND table = ?", db, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out[n] = true
	}
	return out, rows.Err()
}

// CreateDDL renders a CREATE TABLE from T's `ch:` tags. Column types are
// inferred from the Go types; anything non-trivial should carry an explicit
// `chtype:"…"` tag (e.g. `chtype:"Decimal(14, 6)"`). Returns an error if a
// field's type cannot be mapped and has no chtype override.
func CreateDDL[T any](table, engine, orderBy string) (string, error) {
	cols, err := columnsFor(rowType[T]())
	if err != nil {
		return "", err
	}
	lines := make([]string, len(cols))
	for i, c := range cols {
		chType := c.chType
		if chType == "" {
			var err error
			if chType, err = goToCH(c.typ); err != nil {
				return "", fmt.Errorf("field %s (%s): %w — add a chtype tag", c.Field, c.Name, err)
			}
		}
		lines[i] = fmt.Sprintf("    `%s` %s", c.Name, chType)
	}
	return fmt.Sprintf("CREATE TABLE %s (\n%s\n)\nENGINE = %s\nORDER BY %s",
		table, strings.Join(lines, ",\n"), engine, orderBy), nil
}

var kindToCH = map[reflect.Kind]string{
	reflect.Bool:    "Bool",
	reflect.Int8:    "Int8",
	reflect.Int16:   "Int16",
	reflect.Int32:   "Int32",
	reflect.Int:     "Int64",
	reflect.Int64:   "Int64",
	reflect.Uint8:   "UInt8",
	reflect.Uint16:  "UInt16",
	reflect.Uint32:  "UInt32",
	reflect.Uint:    "UInt64",
	reflect.Uint64:  "UInt64",
	reflect.Float32: "Float32",
	reflect.Float64: "Float64",
	reflect.String:  "String",
}

var timeType = reflect.TypeFor[time.Time]()

func goToCH(t reflect.Type) (string, error) {
	switch {
	case t == timeType:
		return "DateTime", nil
	case t.Kind() == reflect.Slice:
		elem, err := goToCH(t.Elem())
		if err != nil {
			return "", err
		}
		return "Array(" + elem + ")", nil
	}
	if ch, ok := kindToCH[t.Kind()]; ok {
		return ch, nil
	}
	return "", fmt.Errorf("unmapped Go type %s", t.String())
}
