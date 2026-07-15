package structs

import (
	"context"
	"testing"
)

func TestInsert(t *testing.T) {
	conn := &fakeConn{}
	rows := []row{{ID: 1, Name: "a"}, {ID: 2, Name: "b"}}
	if err := Insert(context.Background(), conn, "events", rows); err != nil {
		t.Fatal(err)
	}
	if conn.batch.appended != 2 || !conn.batch.sent {
		t.Fatalf("expected 2 appended rows and a send, got %+v", conn.batch)
	}
}

func TestInsertEmptyIsNoOp(t *testing.T) {
	conn := &fakeConn{}
	if err := Insert[row](context.Background(), conn, "events", nil); err != nil {
		t.Fatal(err)
	}
	if conn.batch != nil {
		t.Fatal("empty insert should not prepare a batch")
	}
}

func TestVerifyTagsReportsBothDirections(t *testing.T) {
	// Live table has an extra `legacy` column and is missing `tags`/`created_at`/`money`.
	conn := &fakeConn{liveColumns: []string{"id", "name", "legacy"}}
	diffs, err := VerifyTags[row](context.Background(), conn, "db", "events")
	if err != nil {
		t.Fatal(err)
	}
	byCol := map[string]string{}
	for _, d := range diffs {
		byCol[d.Column] = d.Issue
	}
	for _, missing := range []string{"money", "tags", "created_at"} {
		if _, ok := byCol[missing]; !ok {
			t.Errorf("expected %q flagged as in struct but missing from table", missing)
		}
	}
	if _, ok := byCol["legacy"]; !ok {
		t.Error("expected `legacy` flagged as in table but not in struct")
	}
}

func TestVerifyTagsAgreement(t *testing.T) {
	conn := &fakeConn{liveColumns: []string{"id", "name", "money", "tags", "created_at"}}
	diffs, err := VerifyTags[row](context.Background(), conn, "db", "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) != 0 {
		t.Fatalf("struct and table agree; expected no diffs, got %v", diffs)
	}
}
