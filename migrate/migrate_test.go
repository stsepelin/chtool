package migrate

import (
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
