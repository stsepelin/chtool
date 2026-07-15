//go:build integration

// Integration tests run against a real ClickHouse:
//
//	go test -tags integration ./migrate/
//
// Server from CHTOOL_TEST_DSN (default clickhouse://localhost:9000/default).
package migrate

import (
	"context"
	"errors"
	"net/url"
	"os"
	"testing"
	"testing/fstest"
	"time"

	chtool "github.com/stsepelin/chtool"
)

func baseDSN() string {
	if d := os.Getenv("CHTOOL_TEST_DSN"); d != "" {
		return d
	}
	return "clickhouse://localhost:9000/default"
}

// scratchDB creates a fresh database and returns a DSN pointing at it plus a
// cleanup func. It skips the test if the server is unreachable.
func scratchDB(t *testing.T, name string) (dsn string, cleanup func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := chtool.Open(ctx, baseDSN())
	if err != nil {
		if os.Getenv("CHTOOL_REQUIRE_CH") != "" {
			t.Fatalf("ClickHouse required (CHTOOL_REQUIRE_CH) but unreachable at %s: %v", baseDSN(), err)
		}
		t.Skipf("no ClickHouse at %s: %v", baseDSN(), err)
	}
	for _, q := range []string{"DROP DATABASE IF EXISTS " + name, "CREATE DATABASE " + name} {
		if err := admin.Exec(ctx, q); err != nil {
			admin.Close()
			t.Fatalf("%s: %v", q, err)
		}
	}
	u, err := url.Parse(baseDSN())
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	return u.String(), func() {
		_ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name)
		admin.Close()
	}
}

func migrationsFS() fstest.MapFS {
	return fstest.MapFS{
		"000001_events.up.sql":   {Data: []byte("CREATE TABLE events (id UInt64) ENGINE = MergeTree ORDER BY id")},
		"000001_events.down.sql": {Data: []byte("DROP TABLE events")},
		"000002_daily.up.sql":    {Data: []byte("CREATE TABLE daily (d Date) ENGINE = MergeTree ORDER BY d")},
		"000002_daily.down.sql":  {Data: []byte("DROP TABLE daily")},
	}
}

func TestIntegrationMigrateLifecycle(t *testing.T) {
	dsn, cleanup := scratchDB(t, "chtool_it_migrate")
	defer cleanup()
	fsys := migrationsFS()

	// Fresh DB → version 0, not dirty.
	if v, dirty, err := Version(fsys, dsn); err != nil || v != 0 || dirty {
		t.Fatalf("fresh Version = (%d,%v,%v), want (0,false,nil)", v, dirty, err)
	}

	// Up applies both migrations.
	if err := Up(fsys, dsn); err != nil {
		t.Fatalf("Up: %v", err)
	}
	if v, dirty, err := Version(fsys, dsn); err != nil || v != 2 || dirty {
		t.Fatalf("after Up Version = (%d,%v,%v), want (2,false,nil)", v, dirty, err)
	}

	// Up again is a no-op (ErrNoChange swallowed).
	if err := Up(fsys, dsn); err != nil {
		t.Fatalf("second Up should be a no-op, got %v", err)
	}

	// Steps(-1) reverts the last migration.
	if err := Steps(fsys, dsn, -1); err != nil {
		t.Fatalf("Steps(-1): %v", err)
	}
	if v, _, _ := Version(fsys, dsn); v != 1 {
		t.Fatalf("after Steps(-1) version = %d, want 1", v)
	}

	// Steps(1) re-applies it.
	if err := Steps(fsys, dsn, 1); err != nil {
		t.Fatalf("Steps(1): %v", err)
	}
	if v, _, _ := Version(fsys, dsn); v != 2 {
		t.Fatalf("after Steps(1) version = %d, want 2", v)
	}

	// Force sets the version without running SQL.
	if err := Force(fsys, dsn, 1); err != nil {
		t.Fatalf("Force: %v", err)
	}
	if v, dirty, _ := Version(fsys, dsn); v != 1 || dirty {
		t.Fatalf("after Force(1) = (%d,%v), want (1,false)", v, dirty)
	}
}

func TestIntegrationErrNoChangeIsSentinel(t *testing.T) {
	if !errors.Is(ErrNoChange, ErrNoChange) {
		t.Fatal("ErrNoChange should be usable with errors.Is")
	}
}
