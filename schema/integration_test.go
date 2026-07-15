//go:build integration

// Integration tests run against a real ClickHouse:
//
//	go test -tags integration ./schema/
//
// Server from CHTOOL_TEST_DSN (default clickhouse://localhost:9000/default).
package schema

import (
	"context"
	"os"
	"strings"
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

func scratchConn(t *testing.T, db string) (Conn, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := chtool.Open(ctx, baseDSN())
	if err != nil {
		t.Skipf("no ClickHouse at %s: %v", baseDSN(), err)
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

func TestIntegrationDumpDiff(t *testing.T) {
	const db = "chtool_it_schema"
	conn, cleanup := scratchConn(t, db)
	defer cleanup()
	ctx := context.Background()

	stmts := []string{
		"CREATE TABLE " + db + ".events (created_at DateTime, date Date, country String, hits UInt64) ENGINE = MergeTree ORDER BY created_at",
		"CREATE TABLE " + db + ".events_daily (date Date, country String, hits SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY (date, country)",
		"CREATE MATERIALIZED VIEW " + db + ".events_daily_mv TO " + db + ".events_daily (date Date, country String, hits UInt64) AS SELECT date, country, sum(hits) AS hits FROM " + db + ".events GROUP BY date, country",
	}
	for _, s := range stmts {
		if err := conn.Exec(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}

	objs, err := Dump(ctx, conn, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 3 {
		t.Fatalf("expected 3 objects, got %d: %+v", len(objs), objs)
	}
	// Tables sort before MVs.
	if objs[len(objs)-1].Name != "events_daily_mv" || !objs[len(objs)-1].IsMV {
		t.Fatalf("MV should sort last: %+v", objs)
	}
	// The default index_granularity must have been normalized away.
	for _, o := range objs {
		if strings.Contains(o.DDL, "index_granularity = 8192") {
			t.Errorf("default granularity not normalized in %s: %s", o.Name, o.DDL)
		}
	}

	// A dump against itself has no drift.
	if report := Diff(objs, objs, nil); len(report) != 0 {
		t.Fatalf("self-diff should be empty, got %v", report)
	}

	// Render → Parse round-trips the live dump.
	parsed, err := Parse(Render(objs))
	if err != nil {
		t.Fatal(err)
	}
	if report := Diff(objs, parsed, nil); len(report) != 0 {
		t.Fatalf("render/parse round-trip drifted: %v", report)
	}
}
