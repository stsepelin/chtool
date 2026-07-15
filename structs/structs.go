// Package structs provides generic, reflection-based helpers for working with
// ClickHouse rows as Go structs tagged with `ch:"column"` (the tag
// clickhouse-go/v2 uses). It has no dependency on any particular schema.
package structs

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Conn = driver.Conn

// Insert batch-inserts rows into table (may be db-qualified). A nil/empty slice
// is a no-op.
func Insert[T any](ctx context.Context, conn Conn, table string, rows []T) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+table)
	if err != nil {
		return fmt.Errorf("prepare batch %s: %w", table, err)
	}
	for i := range rows {
		if err := batch.AppendStruct(&rows[i]); err != nil {
			return fmt.Errorf("append row %d: %w", i, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch %s: %w", table, err)
	}
	return nil
}

type Column struct {
	Field  string
	Name   string
	GoType string
	typ    reflect.Type
	chType string
}

// Columns reflects T's `ch:`-tagged fields in declaration order, skipping fields
// with no `ch` tag or `ch:"-"`.
func Columns[T any]() []Column {
	t := reflect.TypeFor[T]()
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	var cols []Column
	for i := range t.NumField() {
		f := t.Field(i)
		tag := f.Tag.Get("ch")
		if tag == "" || tag == "-" {
			continue
		}
		name, _, _ := strings.Cut(tag, ",")
		cols = append(cols, Column{
			Field: f.Name, Name: name, GoType: f.Type.String(),
			typ: f.Type, chType: f.Tag.Get("chtype"),
		})
	}
	return cols
}

type Diff struct {
	Column string
	Issue  string
}

func (d Diff) String() string { return fmt.Sprintf("%s: %s", d.Column, d.Issue) }

// VerifyTags compares T's `ch:` tags against the live columns of db.table. It
// reports struct columns missing from the table and table columns with no
// struct field. An empty slice means the struct and table agree on column set.
func VerifyTags[T any](ctx context.Context, conn Conn, db, table string) ([]Diff, error) {
	live, err := liveColumns(ctx, conn, db, table)
	if err != nil {
		return nil, err
	}
	structCols := map[string]bool{}
	var diffs []Diff
	for _, c := range Columns[T]() {
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
	cols := Columns[T]()
	if len(cols) == 0 {
		return "", fmt.Errorf("no ch:-tagged fields on %s", reflect.TypeFor[T]().Name())
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
