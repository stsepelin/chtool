//go:build integration

// Integration coverage for the column list Insert generates. Both directions of
// struct-vs-table drift are exercised against a real server: the unit suite
// passed while migrate-first was fatal, because the fake conn discards the query.
package structs

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	chtool "github.com/stsepelin/chtool"
)

// TestIntegrationInsertToleratesAddedColumns pins the migrate-first deploy
// order: a column added to the table ahead of the struct must not stop the
// writer. The DEFAULT values are distinctive so an assertion cannot pass on a
// type zero that happens to match.
func TestIntegrationInsertToleratesAddedColumns(t *testing.T) {
	const db = "chtool_it_structs_added"
	conn, cleanup := scratchConn(t, db)
	defer cleanup()
	ctx := context.Background()

	ddl, err := CreateDDL[row](db+".events", "MergeTree", "id")
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, ddl); err != nil {
		t.Fatalf("create: %v\n%s", err, ddl)
	}

	// Migrate first: three columns the struct knows nothing about.
	for _, q := range []string{
		"ALTER TABLE " + db + ".events ADD COLUMN creative_type String DEFAULT 'banner137'",
		"ALTER TABLE " + db + ".events ADD COLUMN weight Float64 DEFAULT 137.75",
		"ALTER TABLE " + db + ".events ADD COLUMN no_default Int64",
	} {
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	// The struct is unchanged and must still be able to write.
	rows := []row{{ID: 1, Name: "a", Money: "1.50", Tags: []string{"x"}, When: time.Now()}}
	if err := Insert(ctx, conn, db+".events", rows); err != nil {
		t.Fatalf("Insert after an additive migration must succeed: %v", err)
	}

	var (
		creative  string
		weight    float64
		noDefault int64
	)
	if err := conn.QueryRow(ctx,
		"SELECT creative_type, weight, no_default FROM "+db+".events",
	).Scan(&creative, &weight, &noDefault); err != nil {
		t.Fatal(err)
	}
	// A DEFAULT expression must actually be evaluated, not left as a type zero.
	if creative != "banner137" {
		t.Errorf("creative_type = %q, want the DEFAULT %q", creative, "banner137")
	}
	if weight != 137.75 {
		t.Errorf("weight = %v, want the DEFAULT 137.75", weight)
	}
	// No DEFAULT means the type zero, which is what distinguishes "DEFAULT
	// applied" from "we wrote a zero".
	if noDefault != 0 {
		t.Errorf("no_default = %d, want 0", noDefault)
	}
}

// TestIntegrationInsertRejectsColumnMissingFromTable pins the loud direction: a
// `ch` tag naming a column the table lacks used to be discarded silently.
func TestIntegrationInsertRejectsColumnMissingFromTable(t *testing.T) {
	const db = "chtool_it_structs_extra"
	conn, cleanup := scratchConn(t, db)
	defer cleanup()
	ctx := context.Background()

	if err := conn.Exec(ctx,
		"CREATE TABLE "+db+".events (`id` Int64) ENGINE = MergeTree ORDER BY id",
	); err != nil {
		t.Fatal(err)
	}

	type extra struct {
		ID    int64  `ch:"id"`
		Ghost string `ch:"ghost"` // no such column
	}
	err := Insert(ctx, conn, db+".events", []extra{{ID: 1, Ghost: "lost"}})
	if err == nil {
		t.Fatal("a struct field that cannot reach the table must be an error, not a silent drop")
	}
	if !strings.Contains(err.Error(), "ghost") {
		t.Errorf("error should name the unreachable column, got: %v", err)
	}

	var n uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM "+db+".events").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("failed insert wrote %d rows, want 0", n)
	}
}

// TestIntegrationInsertFailsAfterColumnDrop is the mirror of the added-column
// case, and the one that changes for consumers: dropping a column the struct
// still tags now fails loudly, where it used to discard that field per row.
func TestIntegrationInsertFailsAfterColumnDrop(t *testing.T) {
	const db = "chtool_it_structs_dropped"
	conn, cleanup := scratchConn(t, db)
	defer cleanup()
	ctx := context.Background()

	ddl, err := CreateDDL[row](db+".events", "MergeTree", "id")
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, ddl); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, "ALTER TABLE "+db+".events DROP COLUMN tags"); err != nil {
		t.Fatal(err)
	}

	err = Insert(ctx, conn, db+".events", []row{{ID: 1, Name: "a", Money: "1.50", When: time.Now()}})
	if err == nil {
		t.Fatal("insert into a table missing a tagged column must fail rather than drop the field")
	}
	if !strings.Contains(err.Error(), "tags") {
		t.Errorf("error should name the dropped column, got: %v", err)
	}
}

// TestIntegrationInsertWritesEmbeddedFields guards the embedded-struct
// recursion. clickhouse-go flattens embedded structs, so if the generated
// column list did not, ClickHouse would fill the embedded columns with their
// DEFAULTs and the insert would still succeed — silent loss, which is the exact
// failure mode this change exists to remove.
func TestIntegrationInsertWritesEmbeddedFields(t *testing.T) {
	const db = "chtool_it_structs_embed"
	conn, cleanup := scratchConn(t, db)
	defer cleanup()
	ctx := context.Background()

	// Distinctive DEFAULTs, so a dropped field cannot look like a written one.
	if err := conn.Exec(ctx, "CREATE TABLE "+db+".events ("+
		"`id` Int64 DEFAULT 137, `shop` String DEFAULT 'default137', `name` String"+
		") ENGINE = MergeTree ORDER BY id"); err != nil {
		t.Fatal(err)
	}

	want := embeddingRow{embeddedBase: embeddedBase{ID: 42, Shop: "acme"}, Name: "widget"}
	if err := Insert(ctx, conn, db+".events", []embeddingRow{want}); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	var got embeddingRow
	if err := conn.QueryRow(ctx, "SELECT id, shop, name FROM "+db+".events").
		Scan(&got.ID, &got.Shop, &got.Name); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("round-trip = %+v, want %+v (embedded fields must reach the table)", got, want)
	}
}

// TestIntegrationInsertAndComputedColumns pins the MATERIALIZED asymmetry noted
// on Insert: leaving such a column untagged works and the server computes it;
// tagging it is an error, because the server refuses writes to it.
func TestIntegrationInsertAndComputedColumns(t *testing.T) {
	const db = "chtool_it_structs_mat"
	conn, cleanup := scratchConn(t, db)
	defer cleanup()
	ctx := context.Background()

	if err := conn.Exec(ctx, "CREATE TABLE "+db+".events ("+
		"`id` Int64, `doubled` Int64 MATERIALIZED id * 2"+
		") ENGINE = MergeTree ORDER BY id"); err != nil {
		t.Fatal(err)
	}

	type reader struct {
		ID int64 `ch:"id"`
	}
	if err := Insert(ctx, conn, db+".events", []reader{{ID: 137}}); err != nil {
		t.Fatalf("an untagged MATERIALIZED column must not block an insert: %v", err)
	}
	var doubled int64
	if err := conn.QueryRow(ctx, "SELECT doubled FROM "+db+".events").Scan(&doubled); err != nil {
		t.Fatal(err)
	}
	if doubled != 274 {
		t.Errorf("doubled = %d, want 274 (the server should compute it)", doubled)
	}

	type writer struct {
		ID      int64 `ch:"id"`
		Doubled int64 `ch:"doubled"`
	}
	if err := Insert(ctx, conn, db+".events", []writer{{ID: 1, Doubled: 2}}); err == nil {
		t.Error("tagging a MATERIALIZED column should fail; the server will not accept a value for it")
	}
}

// everyKind carries one field per Go type structs can map. The round-trip below
// is driven by reflection over its `ch:` tags, so a field added here — or a new
// type added to kindToCH — is covered without anyone writing an assertion.
type everyKind struct {
	Bool    bool      `ch:"c_bool"`
	Int8    int8      `ch:"c_int8"`
	Int16   int16     `ch:"c_int16"`
	Int32   int32     `ch:"c_int32"`
	Int     int       `ch:"c_int"`
	Int64   int64     `ch:"c_int64"`
	Uint8   uint8     `ch:"c_uint8"`
	Uint16  uint16    `ch:"c_uint16"`
	Uint32  uint32    `ch:"c_uint32"`
	Uint    uint      `ch:"c_uint"`
	Uint64  uint64    `ch:"c_uint64"`
	Float32 float32   `ch:"c_float32"`
	Float64 float64   `ch:"c_float64"`
	String  string    `ch:"c_string"`
	Tags    []string  `ch:"c_tags"`
	When    time.Time `ch:"c_when"`
}

var fixedTime = time.Date(2026, 8, 28, 13, 7, 37, 0, time.UTC)

// distinctive fills v with a value that is neither the Go zero nor anything a
// column DEFAULT is likely to be, so an assertion cannot pass on a value that
// never left the fixture.
func distinctive(v reflect.Value) error {
	switch {
	case v.Type() == reflect.TypeFor[time.Time]():
		v.Set(reflect.ValueOf(fixedTime))
	case v.Kind() == reflect.Bool:
		v.SetBool(true)
	case v.Kind() == reflect.String:
		v.SetString("v137")
	case v.CanInt():
		v.SetInt(37) // fits every signed width, int8 included
	case v.CanUint():
		v.SetUint(137)
	case v.CanFloat():
		v.SetFloat(137.75) // exact in binary, so equality is safe
	case v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.String:
		v.Set(reflect.ValueOf([]string{"a137", "b137"}))
	default:
		return fmt.Errorf("no distinctive value for %s; teach distinctive about it", v.Type())
	}
	return nil
}

// TestIntegrationEveryTaggedFieldRoundTrips asserts every `ch:`-tagged field of
// everyKind survives the trip to ClickHouse and back. A per-field test is how a
// field ends up in a fixture with nothing checking it; this one fails for the
// next field too.
func TestIntegrationEveryTaggedFieldRoundTrips(t *testing.T) {
	const db = "chtool_it_structs_roundtrip"
	conn, cleanup := scratchConn(t, db)
	defer cleanup()
	ctx := context.Background()

	ddl, err := CreateDDL[everyKind](db+".wide", "MergeTree", "c_int64")
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, ddl); err != nil {
		t.Fatalf("create: %v\n%s", err, ddl)
	}

	cols := Columns[everyKind]()
	if len(cols) != reflect.TypeFor[everyKind]().NumField() {
		t.Fatalf("every field of everyKind should be tagged; got %d columns for %d fields",
			len(cols), reflect.TypeFor[everyKind]().NumField())
	}

	var want everyKind
	wv := reflect.ValueOf(&want).Elem()
	for _, c := range cols {
		if err := distinctive(wv.FieldByName(c.Field)); err != nil {
			t.Fatalf("field %s: %v", c.Field, err)
		}
	}

	if err := Insert(ctx, conn, db+".wide", []everyKind{want}); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	names := make([]string, len(cols))
	dests := make([]any, len(cols))
	got := make([]reflect.Value, len(cols))
	for i, c := range cols {
		names[i] = "`" + c.Name + "`"
		got[i] = reflect.New(scanTypeFor(wv.FieldByName(c.Field).Type()))
		dests[i] = got[i].Interface()
	}
	if err := conn.QueryRow(ctx,
		"SELECT "+strings.Join(names, ", ")+" FROM "+db+".wide",
	).Scan(dests...); err != nil {
		t.Fatal(err)
	}

	for i, c := range cols {
		g, w := got[i].Elem(), wv.FieldByName(c.Field)
		if gt, ok := g.Interface().(time.Time); ok {
			// DateTime is second-resolution and comes back in the server's zone.
			if wt := w.Interface().(time.Time); !gt.Equal(wt) {
				t.Errorf("%s (%s) = %s, want %s", c.Field, c.Name, gt, wt)
			}
			continue
		}
		if !reflect.DeepEqual(g.Interface(), w.Convert(g.Type()).Interface()) {
			t.Errorf("%s (%s) = %#v, want %#v", c.Field, c.Name, g.Interface(), w.Interface())
		}
	}
}

// scanTypeFor widens the destination for the two kinds clickhouse-go writes but
// refuses to scan: it maps int and uint to Int64/UInt64 on the way in, then
// insists on the sized Go type on the way out ("converting Int64 to *int is
// unsupported"). Comparing after a widening conversion keeps those fields
// asserted rather than quietly excluded from the round-trip.
func scanTypeFor(t reflect.Type) reflect.Type {
	switch t.Kind() {
	case reflect.Int:
		return reflect.TypeFor[int64]()
	case reflect.Uint:
		return reflect.TypeFor[uint64]()
	}
	return t
}

func httpDSN() string {
	if d := os.Getenv("CHTOOL_TEST_DSN_HTTP"); d != "" {
		return d
	}
	return "http://localhost:8123/default"
}

// TestIntegrationInsertToleratesAddedColumnsOverHTTP repeats the migrate-first
// case on the HTTP protocol, which resolves columns differently: it cannot ask
// the server for a header block, so it runs DESCRIBE TABLE and, given no column
// list, inserts every non-computed column — writing zeros over the DEFAULTs a
// bare native INSERT would have left alone. The explicit list is what makes the
// two protocols agree.
func TestIntegrationInsertToleratesAddedColumnsOverHTTP(t *testing.T) {
	const db = "chtool_it_structs_http"
	ctx := context.Background()

	conn, err := chtool.Open(ctx, httpDSN())
	if err != nil {
		if os.Getenv("CHTOOL_REQUIRE_CH") != "" {
			t.Fatalf("ClickHouse required (CHTOOL_REQUIRE_CH) but HTTP unreachable at %s: %v", httpDSN(), err)
		}
		t.Skipf("no ClickHouse HTTP at %s: %v", httpDSN(), err)
	}
	defer conn.Close()
	for _, q := range []string{"DROP DATABASE IF EXISTS " + db, "CREATE DATABASE " + db} {
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	defer func() { _ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db) }()

	ddl, err := CreateDDL[row](db+".events", "MergeTree", "id")
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, ddl); err != nil {
		t.Fatalf("create: %v\n%s", err, ddl)
	}
	if err := conn.Exec(ctx,
		"ALTER TABLE "+db+".events ADD COLUMN creative_type String DEFAULT 'banner137'",
	); err != nil {
		t.Fatal(err)
	}

	if err := Insert(ctx, conn, db+".events", []row{
		{ID: 1, Name: "a", Money: "1.50", Tags: []string{"x"}, When: time.Now()},
	}); err != nil {
		t.Fatalf("Insert over HTTP after an additive migration must succeed: %v", err)
	}

	var creative string
	if err := conn.QueryRow(ctx, "SELECT creative_type FROM "+db+".events").Scan(&creative); err != nil {
		t.Fatal(err)
	}
	if creative != "banner137" {
		t.Errorf("creative_type = %q, want the DEFAULT %q — an empty string means the "+
			"column list was not recognised and HTTP wrote every column", creative, "banner137")
	}
}
