//go:build integration

// Integration tests run against a real ClickHouse:
//
//	go test -tags integration ./structs/
//
// Server from CHTOOL_TEST_DSN (default clickhouse://localhost:9000/default).
package structs

import (
	"context"
	"os"
	"testing"
	"time"

	chtool "github.com/stsepelin/chtool"
)

func baseDSN() string {
	if d := os.Getenv("CHTOOL_TEST_DSN"); d != "" {
		return d
	}
	return "clickhouse://localhost:9000/default"
}

func requireCHOrSkip(t *testing.T, err error) {
	t.Helper()
	if os.Getenv("CHTOOL_REQUIRE_CH") != "" {
		t.Fatalf("ClickHouse required (CHTOOL_REQUIRE_CH) but unreachable at %s: %v", baseDSN(), err)
	}
	t.Skipf("no ClickHouse at %s: %v", baseDSN(), err)
}

func scratchConn(t *testing.T, db string) (Conn, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := chtool.Open(ctx, baseDSN())
	if err != nil {
		requireCHOrSkip(t, err)
	}
	for _, q := range []string{"DROP DATABASE IF EXISTS " + db, "CREATE DATABASE " + db} {
		if err := conn.Exec(ctx, q); err != nil {
			conn.Close()
			t.Fatalf("%s: %v", q, err)
		}
	}
	return conn, func() {
		_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db)
		conn.Close()
	}
}

func TestIntegrationInsertVerifyDDL(t *testing.T) {
	const db = "chtool_it_structs"
	conn, cleanup := scratchConn(t, db)
	defer cleanup()
	ctx := context.Background()

	// CreateDDL from the struct, then create the table.
	ddl, err := CreateDDL[row](db+".events", "MergeTree", "id")
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, ddl); err != nil {
		t.Fatalf("create from generated DDL: %v\n%s", err, ddl)
	}

	// Insert a batch.
	rows := []row{
		{ID: 1, Name: "a", Money: "1.50", Tags: []string{"x"}, When: time.Now()},
		{ID: 2, Name: "b", Money: "2.75", Tags: []string{"y", "z"}, When: time.Now()},
	}
	if err := Insert(ctx, conn, db+".events", rows); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	var n uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM "+db+".events").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("expected 2 rows, got %d", n)
	}

	// VerifyTags against the live table must agree.
	diffs, err := VerifyTags[row](ctx, conn, db, "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) != 0 {
		t.Fatalf("struct and table should agree, got diffs: %v", diffs)
	}

	// Drop a column and confirm VerifyTags reports the drift.
	if err := conn.Exec(ctx, "ALTER TABLE "+db+".events DROP COLUMN tags"); err != nil {
		t.Fatal(err)
	}
	diffs, err = VerifyTags[row](ctx, conn, db, "events")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, d := range diffs {
		if d.Column == "tags" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected `tags` flagged after drop, got %v", diffs)
	}
}
