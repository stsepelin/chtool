package migrate

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"
	"testing/fstest"
)

// deadDSN points at a refused local port so New/with reach the driver-open step
// and fail fast (no live ClickHouse needed).
const deadDSN = "clickhouse://localhost:1/db"

func oneMigration() fstest.MapFS {
	return fstest.MapFS{
		"000001_init.up.sql":   {Data: []byte("CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY x")},
		"000001_init.down.sql": {Data: []byte("DROP TABLE t")},
	}
}

// An already-cancelled context must be reported before any connection is
// attempted: against a dead DSN the error must be the ctx error, not a dial
// failure. Without the precheck these would try to connect first.
func TestContextVariantsCheckCtxBeforeWork(t *testing.T) {
	fsys := oneMigration()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("Up", func(t *testing.T) {
		if err := UpContext(ctx, fsys, deadDSN); !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	})
	t.Run("Steps", func(t *testing.T) {
		if err := StepsContext(ctx, fsys, deadDSN, 1); !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	})
	t.Run("Force", func(t *testing.T) {
		if err := ForceContext(ctx, fsys, deadDSN, 1); !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	})
	t.Run("Version", func(t *testing.T) {
		if _, _, err := VersionContext(ctx, fsys, deadDSN); !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	})

	// Sanity: with a live context the same dead DSN fails to connect, so the
	// assertions above really are the precheck and not a coincidence.
	if err := UpContext(context.Background(), fsys, deadDSN); err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("expected a connection error with a live ctx, got %v", err)
	}
}

func TestErrStoppedEarlyIsDistinct(t *testing.T) {
	if errors.Is(ErrStoppedEarly, ErrNoChange) {
		t.Fatal("ErrStoppedEarly must not alias ErrNoChange")
	}
}

func TestNewRejectsBadDSN(t *testing.T) {
	if _, err := New(oneMigration(), "postgres://x/y"); err == nil {
		t.Fatal("New must reject a non-clickhouse DSN")
	}
}

func TestNewFailsToConnect(t *testing.T) {
	if _, err := New(oneMigration(), deadDSN); err == nil {
		t.Fatal("New should fail against a dead endpoint")
	}
}

// The wrappers all funnel through with(); each should surface the connect error
// rather than panic. Covers the wrapper entry + with()'s New-error path.
func TestWrappersSurfaceConnectError(t *testing.T) {
	fsys := oneMigration()
	if err := Up(fsys, deadDSN); err == nil {
		t.Error("Up should error")
	}
	if err := Steps(fsys, deadDSN, 1); err == nil {
		t.Error("Steps should error")
	}
	if err := Force(fsys, deadDSN, 1); err == nil {
		t.Error("Force should error")
	}
	if _, _, err := Version(fsys, deadDSN); err == nil {
		t.Error("Version should error")
	}
}

func TestErrNoChangeReExported(t *testing.T) {
	if ErrNoChange == nil {
		t.Fatal("ErrNoChange should be re-exported")
	}
}

func TestNormalizeDSNRejectsNonClickhouse(t *testing.T) {
	if _, err := normalizeDSN("postgres://localhost/db"); err == nil {
		t.Fatal("expected rejection of a non-clickhouse:// DSN")
	}
}

func TestNormalizeDSNInjectsMultiStatement(t *testing.T) {
	out, err := normalizeDSN("clickhouse://user:pass@localhost:9000/analytics")
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(out)
	if err != nil {
		t.Fatal(err)
	}
	if u.Query().Get("x-multi-statement") != "true" {
		t.Errorf("x-multi-statement not injected: %s", out)
	}
	// credentials and database must survive the round-trip.
	if u.User.Username() != "user" || u.Path != "/analytics" {
		t.Errorf("DSN identity not preserved: %s", out)
	}
}

func TestNormalizeDSNPreservesExplicitMultiStatement(t *testing.T) {
	out, err := normalizeDSN("clickhouse://localhost:9000/db?x-multi-statement=false&other=1")
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(out)
	if u.Query().Get("x-multi-statement") != "false" {
		t.Errorf("explicit x-multi-statement should be honored, not overwritten: %s", out)
	}
	if !strings.Contains(out, "other=1") {
		t.Errorf("existing query params should be preserved: %s", out)
	}
}
