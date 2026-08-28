package structs

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

type embeddedBase struct {
	ID   int64  `ch:"id"`
	Shop string `ch:"shop"`
}

type embeddingRow struct {
	embeddedBase
	Name string `ch:"name"`
}

// TestInsertQueryNamesItsColumns pins the generated statement. The column list
// is what makes an added column harmless, so its exact shape is the contract:
// the space before "(" is what lets clickhouse-go recognise the list at all,
// and the backticks are what let a name be a keyword.
func TestInsertQueryNamesItsColumns(t *testing.T) {
	conn := &fakeConn{}
	if err := Insert(context.Background(), conn, "analytics.views", []row{{ID: 1}}); err != nil {
		t.Fatal(err)
	}
	const want = "INSERT INTO analytics.views (`id`, `name`, `money`, `tags`, `created_at`)"
	if conn.query != want {
		t.Errorf("query =\n  %s\nwant\n  %s", conn.query, want)
	}
}

// TestInsertQueryHasSpaceBeforeColumnList guards the one character that fails
// silently: clickhouse-go's column-list pattern requires whitespace before the
// "(", and a list it does not recognise becomes "every column of the table" on
// the HTTP protocol, misaligning the batch instead of erroring.
func TestInsertQueryHasSpaceBeforeColumnList(t *testing.T) {
	q := insertQuery("t", []Column{{Name: "a"}})
	if !strings.Contains(q, " (") {
		t.Fatalf("column list must be preceded by a space, got %q", q)
	}
	if strings.Contains(q, "t(") {
		t.Fatalf("no space between table and column list: %q", q)
	}
}

func TestInsertAbortsBatchOnAppendError(t *testing.T) {
	boom := errors.New("boom")
	conn := &fakeConn{batch: &fakeBatch{appendErr: boom}}
	err := Insert(context.Background(), conn, "events", []row{{ID: 1}})
	if !errors.Is(err, boom) {
		t.Fatalf("expected the append error, got %v", err)
	}
	if !conn.batch.aborted {
		t.Error("a batch abandoned mid-append must be aborted, or it holds its connection")
	}
	if conn.batch.sent {
		t.Error("batch should not be sent after an append error")
	}
}

// TestColumnsFlattensEmbeddedStructs pins the recursion. clickhouse-go flattens
// embedded structs when it looks for a field; if Columns did not, the embedded
// names would be missing from the column list and ClickHouse would write their
// DEFAULTs over the caller's values without a word.
func TestColumnsFlattensEmbeddedStructs(t *testing.T) {
	var names []string
	for _, c := range Columns[embeddingRow]() {
		names = append(names, c.Name)
	}
	if got := strings.Join(names, ","); got != "id,shop,name" {
		t.Fatalf("Columns = %q, want embedded fields spliced in at the embedded field's position", got)
	}
}

func TestColumnsQualifiesEmbeddedFieldNames(t *testing.T) {
	cols := Columns[embeddingRow]()
	if cols[0].Field != "embeddedBase.ID" {
		t.Errorf("Field = %q, want the path through the embedded struct", cols[0].Field)
	}
}

func TestInsertRejectsBadTags(t *testing.T) {
	type commaTag struct {
		ID int64 `ch:"id,omitempty"`
	}
	type backtickTag struct {
		// A backtick cannot appear in a backtick-quoted tag, hence the plain literal.
		ID int64 "ch:\"i`d\""
	}
	type backslashTag struct {
		ID int64 `ch:"a\\b"`
	}
	type unexportedTagged struct {
		id int64 `ch:"id"` //nolint:unused // the tag is the point
	}
	type embeddedPointer struct {
		*embeddedBase
		Name string `ch:"name"`
	}
	type embeddedScalar struct {
		int64        //nolint:unused // an embedded non-struct is exactly what this case feeds in
		Name  string `ch:"name"`
	}
	type duplicated struct {
		embeddedBase
		OtherID int64 `ch:"id"`
	}
	type untagged struct {
		Name string
	}

	// Each of these would otherwise reach the driver as a column name it does
	// not use, or as a field it silently ignores.
	for _, tc := range []struct {
		name string
		call func() error
		want string
	}{
		{"comma in tag", insertOf[commaTag], "splits the column list"},
		{"backtick in tag", insertOf[backtickTag], "strips"},
		{"backslash in tag", insertOf[backslashTag], "escape"},
		{"tagged unexported field", insertOf[unexportedTagged], "unexported"},
		{"embedded pointer struct", insertOf[embeddedPointer], "embed it by value"},
		{"embedded non-struct", insertOf[embeddedScalar], "not a struct"},
		{"duplicate column", insertOf[duplicated], "claimed by both"},
		{"no tagged fields", insertOf[untagged], "no ch:-tagged fields"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if err == nil {
				t.Fatal("expected an error rather than a column the driver will not fill")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q should explain the problem (%q)", err, tc.want)
			}
		})
	}
}

// insertOf runs a one-row Insert against the fake conn, for table-driven cases
// that only care whether the tags resolve.
func insertOf[T any]() error {
	var zero T
	return Insert(context.Background(), &fakeConn{}, "t", []T{zero})
}

func TestBadTagsDoNotPrepareABatch(t *testing.T) {
	type commaTag struct {
		ID int64 `ch:"id,omitempty"`
	}
	conn := &fakeConn{}
	if err := Insert(context.Background(), conn, "t", []commaTag{{ID: 1}}); err == nil {
		t.Fatal("expected an error")
	}
	if conn.query != "" {
		t.Errorf("tags must be validated before a batch is prepared, got query %q", conn.query)
	}
}

func BenchmarkInsertColumns(b *testing.B) {
	t := rowType[row]()
	for b.Loop() {
		cols, err := columnsFor(t)
		if err != nil {
			b.Fatal(err)
		}
		_ = insertQuery("analytics.views", cols)
	}
}

func BenchmarkResolveColumnsUncached(b *testing.B) {
	t := rowType[row]()
	for b.Loop() {
		if _, err := resolveColumns(t, nil, map[reflect.Type]bool{}); err != nil {
			b.Fatal(err)
		}
	}
}

// TestInsertAcceptsAwkwardButParseableNames guards against over-tightening
// checkName. These names look alarming but survive clickhouse-go's parsing
// intact, so rejecting them would break schemas that work today.
func TestInsertAcceptsAwkwardButParseableNames(t *testing.T) {
	type awkward struct {
		Quoted string `ch:"my\"col"`
		Dashed string `ch:"my-col"`
		Dotted string `ch:"col.nested"`
		Spaced string `ch:"my col"`
		Parens string `ch:"metric(x)"`
	}
	conn := &fakeConn{}
	if err := Insert(context.Background(), conn, "t", []awkward{{}}); err != nil {
		t.Fatalf("these names round-trip through the driver and must be accepted: %v", err)
	}
	const want = "INSERT INTO t (`my\"col`, `my-col`, `col.nested`, `my col`, `metric(x)`)"
	if conn.query != want {
		t.Errorf("query = %s, want %s", conn.query, want)
	}
}

// node is legal Go — a linked-list node embeds a pointer to itself. Resolving
// it must terminate: clickhouse-go never follows an embedded pointer, so the
// cycle contributes nothing.
type node struct {
	*node       //nolint:unused // the self-reference is the case under test
	ID    int64 `ch:"id"`
}

func TestColumnsTerminatesOnSelfEmbeddedPointer(t *testing.T) {
	var names []string
	for _, c := range Columns[node]() {
		names = append(names, c.Name)
	}
	if got := strings.Join(names, ","); got != "id" {
		t.Fatalf("Columns = %q, want just the tagged field", got)
	}
	if err := Insert(context.Background(), &fakeConn{}, "t", []node{{ID: 1}}); err != nil {
		t.Errorf("Insert: %v", err)
	}
}

type mutualA struct {
	*mutualB
	A int64 `ch:"a"`
}

type mutualB struct {
	*mutualA
	B int64 `ch:"b"`
}

// Mutual embedding must terminate too. Here it terminates in an error rather
// than a column list, because mutualB really does carry a tagged field that
// clickhouse-go would never write through an embedded pointer — the same
// refusal as any other embedded pointer, just reached through a cycle.
func TestColumnsTerminatesOnMutualEmbedding(t *testing.T) {
	err := Insert(context.Background(), &fakeConn{}, "t", []mutualA{{A: 1}})
	if err == nil {
		t.Fatal("a tagged field under an embedded pointer must be refused")
	}
	if !strings.Contains(err.Error(), "embed it by value") {
		t.Errorf("unexpected error: %v", err)
	}
}

// opaque has nothing tagged inside it, so a tag on the embedding field is the
// only thing that could name a column — and clickhouse-go ignores it.
type opaque struct{ A int }

type malformedInner struct {
	ID int64 `ch:"bad,name"`
}

func TestInsertRejectsTagOnEmbeddedField(t *testing.T) {
	type taggedOpaque struct {
		opaque `ch:"payload"`
		ID     int64 `ch:"id"`
	}
	type taggedBase struct {
		embeddedBase `ch:"payload"`
		Name         string `ch:"name"`
	}
	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"embedded struct with nothing tagged inside", insertOf[taggedOpaque]},
		{"embedded struct with tagged fields inside", insertOf[taggedBase]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if err == nil {
				t.Fatal("a tag clickhouse-go ignores must not pass silently: no such column would be written")
			}
			if !strings.Contains(err.Error(), "flattens embedded structs") {
				t.Errorf("error should explain why the tag has no effect, got: %v", err)
			}
		})
	}
}

// A malformed tag beneath an embedded pointer must still surface. Reporting
// nothing would leave every field under the pointer silently unwritten.
func TestInsertReportsBadTagsUnderEmbeddedPointer(t *testing.T) {
	type outer struct {
		*malformedInner
		ID int64 `ch:"id"`
	}
	err := insertOf[outer]()
	if err == nil {
		t.Fatal("expected the malformed inner tag to be reported")
	}
	if !strings.Contains(err.Error(), "splits the column list") {
		t.Errorf("error should name the parse problem, got: %v", err)
	}
}

// An embedded pointer to a non-struct has no tags beneath it, so nothing is
// lost and it must not be rejected.
func TestInsertAllowsEmbeddedPointerToNonStruct(t *testing.T) {
	type withPtr struct {
		*int64       //nolint:unused // an embedded pointer to a non-struct is the case
		ID     int64 `ch:"id"`
	}
	if err := insertOf[withPtr](); err != nil {
		t.Errorf("nothing can be tagged under *int64, so it must be accepted: %v", err)
	}
}

// TestInsertDetectsAnUnrecognisedColumnList covers the case the character
// checks deliberately no longer try to predict: whatever the name, if the
// driver did not end up with the columns the statement named, the promise that
// an unlisted column takes its DEFAULT no longer holds, and saying so beats
// failing later on an unrelated column.
func TestInsertDetectsAnUnrecognisedColumnList(t *testing.T) {
	conn := &fakeConn{batch: &fakeBatch{
		// What the driver reports having resolved when it could not read the
		// list: every column of the table, not the five the statement named.
		resolved: []string{"id", "name", "money", "tags", "created_at", "creative_type"},
	}}
	err := Insert(context.Background(), conn, "events", []row{{ID: 1}})
	if err == nil {
		t.Fatal("a column list the driver did not act on must be reported")
	}
	for _, want := range []string{"creative_type", "whole table"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %q, got: %v", want, err)
		}
	}
	if !conn.batch.aborted {
		t.Error("the batch must be aborted, or it holds its connection")
	}
}

// TestInsertRejectsShadowedColumns covers a value that the tags say is written
// and the driver writes from somewhere else. clickhouse-go indexes untagged
// exported fields under their Go name, last one winning, so an untagged X takes
// over the column a `ch:"X"` above it named — verified against a real server:
// the tagged 137 is discarded and the untagged 999 stored in its place.
func TestInsertRejectsShadowedColumns(t *testing.T) {
	type shadowed struct {
		Tagged int64 `ch:"X"`
		X      int64
	}
	err := insertOf[shadowed]()
	if err == nil {
		t.Fatal("a column filled from a field other than the one that named it must be refused")
	}
	for _, want := range []string{"X", "Tagged", "last field claiming a name wins"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %q, got: %v", want, err)
		}
	}
}

// The same two fields the other way round are fine: the tag is last, so the
// driver resolves the column to the tagged field, which is what was asked for.
func TestInsertAllowsTagWinningOverUntaggedField(t *testing.T) {
	type ordered struct {
		X      int64
		Tagged int64 `ch:"X"`
	}
	if err := insertOf[ordered](); err != nil {
		t.Errorf("the tagged field wins here, so this must be accepted: %v", err)
	}
}

// A promoted field from an embedded struct shadows just the same. The names
// have to collide exactly — `ch:"shop"` and a Go field Shop are two different
// keys to the driver — so this pairs a tag with a field of that very name.
func TestInsertRejectsShadowingFromEmbeddedStruct(t *testing.T) {
	type labelled struct {
		Label int64 `ch:"Note"`
	}
	type promoted struct {
		Note int64
	}
	type outer struct {
		labelled
		promoted
	}
	err := insertOf[outer]()
	if err == nil {
		t.Fatal("a promoted untagged field that takes over a tagged column must be refused")
	}
	if !strings.Contains(err.Error(), "promoted.Note") {
		t.Errorf("error should name the shadowing field, got: %v", err)
	}
}
