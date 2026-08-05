//go:build integration

// Integration tests run against a real ClickHouse. They are excluded from the
// default build and run with:
//
//	go test -tags integration ./...
//
// The server is taken from CHTOOL_TEST_DSN (default clickhouse://localhost:9000/
// default); tests skip if it is unreachable.
package chtool

import (
	"context"
	"os"
	"testing"
	"time"
)

func itDSN() string {
	if d := os.Getenv("CHTOOL_TEST_DSN"); d != "" {
		return d
	}
	return "clickhouse://localhost:9000/default"
}

func TestIntegrationOpenAndPing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := Open(ctx, itDSN())
	if err != nil {
		if os.Getenv("CHTOOL_REQUIRE_CH") != "" {
			t.Fatalf("ClickHouse required (CHTOOL_REQUIRE_CH) but unreachable at %s: %v", itDSN(), err)
		}
		t.Skipf("no ClickHouse at %s: %v", itDSN(), err)
	}
	defer conn.Close()

	var v string
	if err := conn.QueryRow(ctx, "SELECT version()").Scan(&v); err != nil {
		t.Fatalf("query version: %v", err)
	}
	if v == "" {
		t.Fatal("empty server version")
	}
}

// WaitReady returns as soon as the server can actually serve a query.
func TestIntegrationWaitReady(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Probe first so an unreachable server skips rather than burning the budget.
	conn, err := Open(ctx, itDSN())
	if err != nil {
		if os.Getenv("CHTOOL_REQUIRE_CH") != "" {
			t.Fatalf("ClickHouse required (CHTOOL_REQUIRE_CH) but unreachable at %s: %v", itDSN(), err)
		}
		t.Skipf("no ClickHouse at %s: %v", itDSN(), err)
	}
	conn.Close()

	if err := WaitReady(ctx, itDSN()); err != nil {
		t.Fatalf("WaitReady against a live server: %v", err)
	}
}
