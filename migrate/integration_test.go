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

// openDSN opens a chtool connection to dsn for direct assertions.
func openDSN(t *testing.T, dsn string) chtool.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := chtool.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open %s: %v", dsn, err)
	}
	return conn
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

// Cancelling mid-run must stop the sequence at a safe point: the in-flight
// migration finishes, later ones never start, the schema is left mid-sequence
// and — crucially — NOT dirty, because nothing was killed mid-statement.
func TestIntegrationUpContextStopsBetweenMigrations(t *testing.T) {
	if raceDetectorEnabled {
		// golang-migrate v4.19.1 has an internal data race on the unsynchronised
		// Migrate.isGracefulStop bool: both runMigrations and the readUp producer
		// goroutine call stop(), which reads it and writes it after receiving on
		// GracefulStop. Using GracefulStop at all trips the detector.
		//
		// It is benign for correctness — the flag is only ever set true, and
		// either goroutine observing it halts the run (readUp returning closes
		// the channel runMigrations ranges over) — but it is upstream's to fix,
		// so this test cannot run under -race.
		t.Skip("upstream data race in golang-migrate's GracefulStop; see comment")
	}
	dsn, cleanup := scratchDB(t, "chtool_it_migrate_ctx")
	defer cleanup()

	// 000001 takes ~2s; 000002 and 000003 are instant. Cancelling well inside
	// 000001 lets it finish, then the run must halt before 000002.
	fsys := fstest.MapFS{
		"000001_slow.up.sql":    {Data: []byte("CREATE TABLE slow ENGINE = MergeTree ORDER BY tuple() AS SELECT sleepEachRow(0.2) AS s FROM numbers(10)")},
		"000001_slow.down.sql":  {Data: []byte("DROP TABLE slow")},
		"000002_two.up.sql":     {Data: []byte("CREATE TABLE two (x UInt8) ENGINE = MergeTree ORDER BY x")},
		"000002_two.down.sql":   {Data: []byte("DROP TABLE two")},
		"000003_three.up.sql":   {Data: []byte("CREATE TABLE three (x UInt8) ENGINE = MergeTree ORDER BY x")},
		"000003_three.down.sql": {Data: []byte("DROP TABLE three")},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := UpContext(ctx, fsys, dsn)
	elapsed := time.Since(start)

	// It reports both why it stopped and that it stopped early.
	if !errors.Is(err, ErrStoppedEarly) {
		t.Fatalf("want ErrStoppedEarly, got %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want the ctx error wrapped too, got %v", err)
	}
	// It waited for the in-flight migration rather than abandoning it mid-DDL.
	if elapsed < time.Second {
		t.Fatalf("returned after %s — it must let the in-flight migration finish, not abort mid-statement", elapsed)
	}

	// Landed mid-sequence, and not dirty: the whole point of stopping at a
	// safe break point instead of killing a statement.
	v, dirty, verr := Version(fsys, dsn)
	if verr != nil {
		t.Fatalf("Version after cancellation: %v", verr)
	}
	if dirty {
		t.Fatalf("schema must not be dirty after a graceful stop (version %d)", v)
	}
	if v != 1 {
		t.Fatalf("expected to stop after migration 1, got version %d", v)
	}

	// Concretely: 000001 applied, 000002/000003 never ran.
	conn := openDSN(t, dsn)
	defer conn.Close()
	for tbl, want := range map[string]uint64{"slow": 1, "two": 0, "three": 0} {
		var n uint64
		if err := conn.QueryRow(context.Background(),
			"SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = ?", tbl).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != want {
			t.Errorf("table %s: count=%d, want %d", tbl, n, want)
		}
	}

	// A resume completes the rest — the stop left a consistent state.
	if err := Up(fsys, dsn); err != nil {
		t.Fatalf("resume after cancellation: %v", err)
	}
	if v, dirty, _ := Version(fsys, dsn); v != 3 || dirty {
		t.Fatalf("after resume want version 3 clean, got %d dirty=%v", v, dirty)
	}
}
