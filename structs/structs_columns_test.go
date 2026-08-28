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
	type parenTag struct {
		ID int64 `ch:"metric("`
	}
	type spaceTag struct {
		ID int64 `ch:" id "`
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
		{"comma in tag", insertOf[commaTag], "cannot parse"},
		{"backtick in tag", insertOf[backtickTag], "cannot parse"},
		{"parenthesis in tag", insertOf[parenTag], "cannot parse"},
		{"whitespace in tag", insertOf[spaceTag], "whitespace"},
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
	}
	conn := &fakeConn{}
	if err := Insert(context.Background(), conn, "t", []awkward{{}}); err != nil {
		t.Fatalf("these names round-trip through the driver and must be accepted: %v", err)
	}
	const want = "INSERT INTO t (`my\"col`, `my-col`, `col.nested`)"
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
