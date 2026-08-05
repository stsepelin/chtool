package chtest

import (
	"database/sql"
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
