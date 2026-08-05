package chtest

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
)

func TestWithDatabase(t *testing.T) {
	cases := []struct {
		name, dsn, db, want string
	}{
		{
			name: "replaces the default database",
			dsn:  "clickhouse://default@localhost:9000/default",
			db:   "scratch",
			want: "clickhouse://default@localhost:9000/scratch",
		},
		{
			name: "sets one when the DSN has no path",
			dsn:  "clickhouse://localhost:9000",
			db:   "scratch",
			want: "clickhouse://localhost:9000/scratch",
		},
		{
			name: "preserves credentials and query parameters",
			dsn:  "clickhouse://user:pw@host:9440/old?secure=true",
			db:   "new",
			want: "clickhouse://user:pw@host:9440/new?secure=true",
		},
		{
			name: "returns an unparseable DSN unchanged",
			dsn:  "://not a dsn",
			db:   "scratch",
			want: "://not a dsn",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := WithDatabase(c.dsn, c.db); got != c.want {
				t.Errorf("WithDatabase(%q, %q) = %q, want %q", c.dsn, c.db, got, c.want)
			}
		})
	}
}

// The shared default must stay the default, an explicit option must win over an
// ambient env var, and env must still work for a CI matrix.
func TestResolveImagePrecedence(t *testing.T) {
	cases := []struct {
		name, optImage, envImage, want, wantSource string
	}{
		{"default when nothing is set", "", "", Image, ""},
		{"env overrides the default", "", "ch:env", "ch:env", ImageEnv},
		{"explicit option overrides the default", "ch:opt", "", "ch:opt", "Options.Image"},
		{"explicit option beats the environment", "ch:opt", "ch:env", "ch:opt", "Options.Image"},
		{"blank option falls through to env", "   ", "ch:env", "ch:env", ImageEnv},
		{"blank env falls through to default", "", "   ", Image, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv(ImageEnv, c.envImage)
			img, src := resolveImage(Options{Image: c.optImage})
			if img != c.want || src != c.wantSource {
				t.Fatalf("resolveImage = (%q, %q), want (%q, %q)", img, src, c.want, c.wantSource)
			}
		})
	}
}

// A non-default image must be visible in test output, not silent.
func TestNonDefaultImageIsLogged(t *testing.T) {
	t.Setenv(DSNEnv, "clickhouse://default@example.invalid:9000/default")

	var logged []string
	opts := Options{
		Image: "clickhouse/clickhouse-server:26.2",
		Logf:  func(f string, a ...any) { logged = append(logged, fmt.Sprintf(f, a...)) },
	}
	if _, _, err := StartContainer(opts); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(logged, "\n")
	if !strings.Contains(joined, "26.2") || !strings.Contains(joined, "Options.Image") {
		t.Fatalf("the override and its source must be logged, got:\n%s", joined)
	}

	// The default must not be announced — that would be noise on every run.
	logged = nil
	if _, _, err := StartContainer(Options{Logf: opts.Logf}); err != nil {
		t.Fatal(err)
	}
	for _, l := range logged {
		if strings.Contains(l, "using image") {
			t.Fatalf("the default image should not be announced, got: %s", l)
		}
	}
}

// Start defaults Logf to the test log, so an override surfaces without the
// caller wiring anything up.
func TestStartWithDefaultsLogfToTestLog(t *testing.T) {
	t.Setenv(DSNEnv, "clickhouse://default@example.invalid:9000/default")
	t.Setenv(ImageEnv, "clickhouse/clickhouse-server:26.2")
	// Fatals on error; the assertion is that it does not panic on a nil Logf.
	if dsn := StartWith(t, Options{}); dsn == "" {
		t.Fatal("expected the env DSN back")
	}
}

// Env must hand back a fresh map: a caller adding to it must not change what
// the next caller gets.
func TestEnvIsFreshEachCall(t *testing.T) {
	a := Env()
	if a["CLICKHOUSE_SKIP_USER_SETUP"] != "1" {
		t.Fatalf("Env must open network access for the default user, got %v", a)
	}
	a["EXTRA"] = "1"
	if _, leaked := Env()["EXTRA"]; leaked {
		t.Fatal("Env returned a shared map; a caller's addition leaked")
	}
}

// With CHTOOL_TEST_DSN set, no container is started and the DSN is passed
// through — this is what lets the same test code run against a CI service
// container.
func TestStartHonoursExistingDSN(t *testing.T) {
	const dsn = "clickhouse://default@example.invalid:9000/default"
	t.Setenv(DSNEnv, dsn)

	if got := Start(t); got != dsn {
		t.Fatalf("Start = %q, want the environment's DSN %q", got, dsn)
	}

	got, cleanup, err := StartMain()
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if got != dsn {
		t.Fatalf("StartMain = %q, want %q", got, dsn)
	}
}

func TestStartMainCleanupIsAlwaysCallable(t *testing.T) {
	t.Setenv(DSNEnv, "clickhouse://default@example.invalid:9000/default")
	_, cleanup, err := StartMain()
	if err != nil {
		t.Fatal(err)
	}
	if cleanup == nil {
		t.Fatal("cleanup must never be nil")
	}
	cleanup()
	cleanup() // must be safe to call twice
}

// The real thing: start a container and prove the DSN it hands back can serve a
// query over the native protocol immediately, with no readiness loop of the
// caller's own. Skipped with -short and when Docker is unavailable.
func TestStartRealContainer(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: skipping container start")
	}
	// Ignore any ambient DSN so this exercises the container path.
	t.Setenv(DSNEnv, "")

	dsn := Start(t)

	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		t.Fatalf("open %s: %v", dsn, err)
	}
	defer db.Close()

	var one int
	if err := db.QueryRow("SELECT 1").Scan(&one); err != nil {
		t.Fatalf("the DSN from Start must be query-ready immediately: %v", err)
	}
	if one != 1 {
		t.Fatalf("SELECT 1 returned %d", one)
	}

	// WithDatabase composes with a database the caller creates.
	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS scratch"); err != nil {
		t.Fatal(err)
	}
	scratch, err := sql.Open("clickhouse", WithDatabase(dsn, "scratch"))
	if err != nil {
		t.Fatal(err)
	}
	defer scratch.Close()

	var name string
	if err := scratch.QueryRow("SELECT currentDatabase()").Scan(&name); err != nil {
		t.Fatalf("query via WithDatabase DSN: %v", err)
	}
	if name != "scratch" {
		t.Fatalf("currentDatabase() = %q, want scratch", name)
	}
}

// The acceptance test for the override: a consumer pinning a specific version
// must actually get that server, not the shared default. This is the case that
// blocked adoption — a repo tracking a managed 26.2 service cannot generate its
// schema artifact on 24.8 without risking systematic false drift.
func TestStartWithImageOverrideRunsThatVersion(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: skipping container start")
	}
	t.Setenv(DSNEnv, "") // exercise the container path, not an ambient server

	const want = "26.2"
	dsn := StartWith(t, Options{Image: "clickhouse/clickhouse-server:" + want})

	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var version string
	if err := db.QueryRow("SELECT version()").Scan(&version); err != nil {
		t.Fatalf("query the overridden server: %v", err)
	}
	if !strings.HasPrefix(version, want) {
		t.Fatalf("server reports %q, want the overridden %s.x — the override did not take effect", version, want)
	}
	// And it is genuinely different from the shared default.
	if strings.HasPrefix(version, strings.TrimPrefix(Image, "clickhouse/clickhouse-server:")) {
		t.Fatalf("got the default image version %q despite the override", version)
	}
}

// The env override must reach the container too, so a CI matrix can vary the
// version without code changes.
func TestEnvImageOverrideRunsThatVersion(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: skipping container start")
	}
	t.Setenv(DSNEnv, "")
	t.Setenv(ImageEnv, "clickhouse/clickhouse-server:26.2")

	db, err := sql.Open("clickhouse", Start(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var version string
	if err := db.QueryRow("SELECT version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(version, "26.2") {
		t.Fatalf("server reports %q, want 26.2.x from %s", version, ImageEnv)
	}
}
