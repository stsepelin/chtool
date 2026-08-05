# chtool

[![Go Reference](https://pkg.go.dev/badge/github.com/stsepelin/chtool.svg)](https://pkg.go.dev/github.com/stsepelin/chtool)
[![CI](https://github.com/stsepelin/chtool/actions/workflows/ci.yml/badge.svg)](https://github.com/stsepelin/chtool/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A small, dependency-light **ClickHouse operations toolkit for Go**, built on
[`clickhouse-go/v2`](https://github.com/ClickHouse/clickhouse-go). It bundles the
things you reach for when running ClickHouse in production — connecting,
migrating, snapshotting and diffing schema, mapping structs to columns, and
rebuilding aggregate tables online — as **independent subpackages** so you import
only what you need.

> **Dependency isolation is a design goal.** A consumer that only needs the
> online rebuilder never pulls in `golang-migrate`; only `chtool/migrate` depends
> on it.

---

## Contents

- [Why chtool](#why-chtool)
- [Install & requirements](#install--requirements)
- [Packages at a glance](#packages-at-a-glance)
- [`chtool` — connect](#chtool--connect)
- [`chtool/migrate` — migrations](#chtoolmigrate--migrations)
- [`chtool/schema` — snapshot, drift, lint](#chtoolschema--snapshot-drift-lint)
- [`chtool/structs` — struct ↔ column helpers](#chtoolstructs--struct--column-helpers)
- [`chtool/rebuild` — online table rebuilds](#chtoolrebuild--online-table-rebuilds)
- [`chtool/chtest` — a throwaway ClickHouse for tests](#chtoolchtest--a-throwaway-clickhouse-for-tests)
- [ClickHouse Cloud](#clickhouse-cloud)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)

---

## Why chtool

- **Cloud-aware.** TLS is enabled and verified automatically for non-local hosts;
  the schema normalizer maps Cloud's `Shared*`/`Replicated*` engines back to their
  OSS equivalents so a Cloud dump and an OSS dump compare equal.
- **Correct by construction.** The online rebuilder partitions events at a single
  boundary `T` so the armed materialized views and the historical backfill are
  exact complements — no gap, no double-counting — and it is fully resumable.
- **Small surface, no framework.** Each subpackage is a handful of functions over
  a `driver.Conn`. Bring your own connection lifecycle, logging, and config.

## Install & requirements

```bash
go get github.com/stsepelin/chtool
```

- **Go 1.25+**
- **ClickHouse** — OSS or Cloud. Tested against 24.8 (CI) and 26.x (dev).
- The native protocol (`clickhouse://…:9000`) is used throughout.

## Packages at a glance

| Package | Import | What it does | Extra deps |
|---|---|---|---|
| `chtool` | `github.com/stsepelin/chtool` | `Open(dsn)` — connect with auto-TLS for non-local/Cloud hosts | — |
| `chtool/migrate` | `…/chtool/migrate` | Thin `golang-migrate` wrapper — `Up` / `Steps` / `Force` / `Version` over any `fs.FS` | `golang-migrate` |
| `chtool/schema` | `…/chtool/schema` | Schema `Dump` + Cloud-aware `Normalize` + drift `Diff` + migration `Lint` | — |
| `chtool/structs` | `…/chtool/structs` | Generic `Insert[T]`, `VerifyTags[T]`, `CreateDDL[T]` over `ch:`-tagged structs | — |
| `chtool/rebuild` | `…/chtool/rebuild` | Online `AggregatingMergeTree` rebuild orchestrator | — |
| `chtool/chtest` | `…/chtool/chtest` | Throwaway ClickHouse container for integration tests | **separate module** (testcontainers) |

Full API reference: **[pkg.go.dev/github.com/stsepelin/chtool](https://pkg.go.dev/github.com/stsepelin/chtool)**.

---

## `chtool` — connect

```go
import "github.com/stsepelin/chtool"

conn, err := chtool.Open(ctx, "clickhouse://user:pw@host:9440/db")
if err != nil {
    log.Fatal(err)
}
defer conn.Close()
```

`Open` parses a `clickhouse://` DSN, connects, and pings (10s timeout). For any
**non-local** host it enables **verified** TLS automatically — the driver derives
the expected `ServerName` from the DSN host — because ClickHouse Cloud requires
TLS. `localhost`/loopback hosts are left plaintext.

`Conn` is an alias for `clickhouse-go/v2`'s `driver.Conn`, so it drops straight
into every other subpackage and any existing `clickhouse-go` code. The caller
owns `Close`.

| Need | DSN |
|---|---|
| Local dev | `clickhouse://localhost:9000/db` |
| Cloud / remote | `clickhouse://user:pw@host:9440/db` (TLS auto-on, verified) |
| Self-signed cert | `clickhouse://…?secure=true&skip_verify=true` (explicit opt-in) |

`WaitReady(ctx, dsn)` blocks until the server can serve a query — the gate to put
behind a compose `depends_on` or a freshly started container. A server can accept
a connection while still starting up, so readiness is proved with a real query
rather than a ping alone. Bound it with a deadline; the error wraps `ctx.Err()`
and carries the last connection failure.

```go
ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
defer cancel()
if err := chtool.WaitReady(ctx, dsn); err != nil {
    log.Fatal(err)
}
```

---

## `chtool/migrate` — migrations

A razor-thin wrapper over [`golang-migrate`](https://github.com/golang-migrate/migrate)
for ClickHouse. It keeps golang-migrate's default `schema_migrations` state table
and injects `x-multi-statement=true` so multi-statement files apply over the
native protocol. Migrations come from any `fs.FS` — typically an `embed.FS`.

```go
import (
    "embed"
    "github.com/stsepelin/chtool/migrate"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Apply everything pending (no-op when already current).
if err := migrate.Up(migrations, dsn); err != nil {
    log.Fatal(err)
}

// Inspect / step / recover.
v, dirty, _ := migrate.Version(migrations, dsn) // fresh DB → (0, false, nil)
_ = migrate.Steps(migrations, dsn, -1)          // roll back one
_ = migrate.Force(migrations, dsn, 18)          // clear a dirty state at version 18
```

Migration files follow golang-migrate's `NNNNNN_name.up.sql` / `.down.sql`
convention. Pair this with [`schema.Lint`](#chtoolschema--snapshot-drift-lint) to
enforce house rules before they run.

| Function | Purpose |
|---|---|
| `Up(fsys, dsn)` / `UpContext(ctx, …)` | Apply all pending migrations |
| `Steps(fsys, dsn, n)` / `StepsContext(ctx, …)` | Apply (`n>0`) or revert (`n<0`) `n` migrations |
| `Force(fsys, dsn, version)` / `ForceContext(ctx, …)` | Set the version without running SQL (dirty-state recovery) |
| `Version(fsys, dsn)` / `VersionContext(ctx, …)` | Current `(version, dirty, err)` |
| `Create(dir, name)` | Scaffold the next `NNNNNN_name.up.sql` (gapless, `O_EXCL`) |
| `New(fsys, dsn)` | Build a `*migrate.Migrate` for advanced use |

`Create` puts the constructor next to the validator: it numbers one past the
highest existing migration so the run stays gapless for `schema.Lint`, accepts
only a lowercase `[a-z0-9_]` slug (which also makes `..` and path separators
unrepresentable), and uses `O_EXCL` so it never truncates an existing migration.
It writes only the `.up.sql` — a `.down.sql` is optional, and an empty one is
worse than none, since ClickHouse rejects an empty statement at runtime.

### Cancellation

The `Context` variants bound a run — but it is worth being precise about what
that buys you, because golang-migrate exposes no `context.Context` at all.

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
defer cancel()

if err := migrate.UpContext(ctx, migrations, dsn); err != nil {
    if errors.Is(err, migrate.ErrStoppedEarly) {
        // Stopped between migrations. Find out where we landed.
        v, dirty, _ := migrate.Version(migrations, dsn)
        log.Printf("stopped at version %d (dirty=%v)", v, dirty)
    }
    return err
}
```

`UpContext` / `StepsContext` return `ctx.Err()` immediately if the context is
already done, and otherwise drive the sequence **one migration at a time**,
checking the context between them. So a run stops *between* migrations, **never
mid-statement**. That is the semantics you want on ClickHouse anyway: DDL is
non-transactional, and killing it mid-statement is exactly how you get the dirty
state `Force` exists to repair.

A cancelled run therefore lands mid-sequence and returns an error wrapping both
`ErrStoppedEarly` and `ctx.Err()` — re-check with `Version` to see where. A
cancellation that races with normal completion reports the same way, so treat
`ErrStoppedEarly` as *"re-check"*, not *"nothing was applied"*.

`ForceContext` / `VersionContext` honour `ctx` only before their call begins:
each is a single metadata operation with no safe mid-point to stop at.

**Why stepping, and not golang-migrate's `GracefulStop`?** Because signalling
`GracefulStop` is a data race: `Migrate.stop()` reads and writes the
unsynchronised `isGracefulStop` from two goroutines (v4.19.1). It is benign in
effect, but it would fire the race detector in any consumer that cancels a
migration under `-race`. Stepping reaches the same break points without touching
that flag, at one cost worth knowing: a **cancellable** run takes golang-migrate's
migration lock per migration rather than once for the whole sequence, so a second
migrator could interleave between steps. A non-cancellable context (i.e. every
non-`Context` function here) skips stepping and runs the sequence in a single
locked call, exactly as before.

> This is the only subpackage that imports `golang-migrate`.

---

## `chtool/schema` — snapshot, drift, lint

Turn a live database into a normalized, deterministic schema file you can commit;
diff a running server against it to catch drift; and lint migration files before
they merge.

```go
import "github.com/stsepelin/chtool/schema"

// 1. Snapshot: dump every table/MV, normalized and Cloud-aware.
objs, _ := schema.Dump(ctx, conn, "analytics", "schema_migrations")
snapshot := schema.Render(objs) // deterministic text — commit it as a PR artifact

// 2. Drift: compare the committed snapshot against the live server.
want, _ := schema.Parse(snapshot)
if report := schema.Diff(want, objs, nil); len(report) > 0 {
    fmt.Println(strings.Join(report, "\n")) // "- events: DDL differs", ...
}

// 3. Lint: enforce migration hygiene.
issues, _ := schema.Lint(os.DirFS("migrations"), schema.LintConfig{GrandfatherBelow: 20})
for _, i := range issues {
    fmt.Println(i) // "000021_x.up.sql: destructive statement requires a ..."
}
```

**Normalization.** `Dump` normalizes each object via `NormalizeForDB` (which layers
a database-qualifier strip on top of `Normalize`): Cloud `Shared*`/`Replicated*`
engines are mapped to their OSS equivalents (keeping semantic args like a
`ReplacingMergeTree` version column), the default `index_granularity = 8192` is
stripped, the DDL is reflowed by clause, and the `<db>.` qualifier is removed from
the CREATE target and an MV's `TO`/`FROM`. The upshot: **a dump compares equal
across Cloud vs OSS *and* across databases of different names** — e.g. a snapshot
taken from `default` shows no drift against a live `smoke` database. Use
`Normalize(ddl)` directly when the database qualifier is meaningful.

**Lint rules.** `Lint` checks that filenames match the sequence pattern and form a
gapless run; that every sequence has an `.up` file; that no file uses
`ON CLUSTER` or `POPULATE`; that no file is comment-only; and — for files above
`GrandfatherBelow` — that each holds exactly one statement and that destructive
statements (`DROP TABLE`, `DROP COLUMN`, `TRUNCATE`, `MODIFY COLUMN`) carry a
`-- destructive: acknowledged` marker.

---

## `chtool/structs` — struct ↔ column helpers

Generic, reflection-based helpers for treating ClickHouse rows as Go structs
tagged with `ch:"column"` (the tag `clickhouse-go/v2` already uses).

```go
import "github.com/stsepelin/chtool/structs"

type View struct {
    ID      int64     `ch:"id"`
    Country string    `ch:"country"`
    Revenue string    `ch:"revenue" chtype:"Decimal(14, 6)"` // override the inferred type
    Tags    []string  `ch:"tags"`
    When    time.Time `ch:"created_at"`
    Ignored string    `ch:"-"` // skipped
}

// Batch insert (PrepareBatch → AppendStruct → Send); nil/empty is a no-op.
_ = structs.Insert(ctx, conn, "analytics.views", rows)

// Drift-check the struct against the live table's columns.
diffs, _ := structs.VerifyTags[View](ctx, conn, "analytics", "views")

// Generate a CREATE TABLE from the struct.
ddl, _ := structs.CreateDDL[View]("views", "MergeTree", "id")
```

`CreateDDL` infers column types from Go types (`int64`→`Int64`, `[]string`→
`Array(String)`, `time.Time`→`DateTime`, …). Anything non-trivial — decimals,
`Nullable`, `LowCardinality`, `Enum` — should carry an explicit `chtype:"…"` tag;
a field whose type can't be inferred and has no `chtype` returns an error.

| Function | Returns |
|---|---|
| `Insert[T](ctx, conn, table, rows)` | Batches `rows` into `table` (may be db-qualified) |
| `VerifyTags[T](ctx, conn, db, table)` | `[]Diff` — struct-vs-table column-set mismatches (empty = agree) |
| `CreateDDL[T](table, engine, orderBy)` | `CREATE TABLE` string |
| `Columns[T]()` | The reflected `ch:`-tagged columns |

---

## `chtool/rebuild` — online table rebuilds

`ALTER TABLE … MODIFY ORDER BY` is metadata-only and never re-sorts existing data,
so any real key change to an `AggregatingMergeTree` means **building a new table
and backfilling it**. Doing that on a *live* table — while ingestion continues —
without losing or double-counting in-flight events is fiddly. This package encodes
a correct, resumable procedure for an `ORDER BY` change or a materialized-view
re-point.

### The procedure

```
 1. create v2      — the new table, from your DDL, retargeted to <target>_v2
 2. dual-write     — arm copies of the feeding MVs at a near-future boundary T
                     (they capture events with boundary_column >= T)
 3. lag-drain      — wait past T, then confirm the pre-T row count has gone quiet
 4. backfill       — history (boundary_column < T), the exact complement of the MVs,
                     newest-partition first, in memory-bounded hash-bucket chunks
 5. validate       — old vs new: every aggregate expression must match exactly
 6. cutover        — drop MVs → RENAME → recreate MVs (separate, explicit step)
```

Steps 1–5 are `Run`; cutover is a distinct command you invoke once validation
passes. Everything is journaled to a `StateStore`, so an interrupted run **resumes**
— completed backfill chunks are skipped and the boundary `T` is read back.

### Why it stays correct

The armed v2 MVs capture `boundary_column >= T`; the backfill covers
`boundary_column < T`. Because both predicates share the **same** literal `T`,
they partition every event at exactly one instant — no gap, no overlap — so the
rebuilt table double-counts nothing. `T` is persisted *before* the MVs are armed
and treated as fatal on write, so a resume can never pick a different boundary.

### Usage

```go
import "github.com/stsepelin/chtool/rebuild"

spec, _ := rebuild.LoadSpec("rebuilds/2026-07-14-neworder") // spec.yaml + new_ddl.sql

o := &rebuild.Orchestrator{
    Conn:  conn,
    DB:    "analytics",
    Spec:  spec,
    Store: rebuild.NewSQLStore(conn, "analytics._chtool_ops"),
    Log:   func(f string, a ...any) { fmt.Printf(f+"\n", a...) },
    // ReconcileGuard: rebuild.CompanionInFS(migrations, spec), // optional gate before cutover
}

_ = rebuild.Plan(ctx, o, false)                                  // read-only preflight + cost probe
_ = o.Run(ctx, rebuild.Options{BoundaryOffset: 10 * time.Minute}) // create → … → validate
_ = rebuild.Status(ctx, o)                                       // where are we?
// ...when validation has passed and writers are quiesced:
_ = rebuild.Cutover(ctx, o, time.Now())                          // the swap
```

| Command | Effect |
|---|---|
| `Plan(ctx, o, forceVersion)` | Preflight: server-version gate, size estimate, tuning, a `FORMAT Null` cost probe. Mutates nothing. |
| `o.Run(ctx, opts)` | Create → dual-write → lag-drain → backfill → validate. Resumable. |
| `Status(ctx, o)` | Print the current phase and backfill progress. |
| `Abort(ctx, o)` | Tear down `*_v2` objects before cutover; never touches the live pipeline. |
| `Cutover(ctx, o, now)` | Drop MVs → `RENAME` (old → dated backup, v2 → live) → recreate MVs. |

`CompanionInFS(fsys, spec)` is a ready-made `ReconcileGuard` that refuses cutover
unless the spec's `companion_migration` is present in the given `fs.FS` — pass
the same `embed.FS` the binary migrates from. That is the point: the guarantee
you want is that the *running binary* carries the companion migration, which an
`os.Stat` on a working-directory-relative path does not check. It fails closed —
a spec with no `companion_migration` is an error, not a silent pass.

The connection's **default database must be the rebuild target's database** — your
`new_ddl.sql` runs verbatim (only the table name is retargeted, not the database).

### The spec

A rebuild is described by a directory containing `spec.yaml` and the new-table DDL
it references. See [`rebuild/examples/`](rebuild/examples/) for a complete example.

```yaml
name: events-daily-neworder      # stable identifier; keys the operation's state
target_table: events_daily       # the AggregatingMergeTree being rebuilt
new_ddl: new_ddl.sql             # co-located CREATE TABLE (authored for the real name)
boundary_column: created_at      # immutable, producer-set event time (prefer UTC DateTime)
chunk_column: date               # backfill iterates one value per chunk (default: "date")
mvs:                             # the materialized views feeding the target
  - events_daily_mv
validations:                     # aggregates compared old-vs-new; a mismatch fails
  - sum(hits)
  - sum(revenue)
# new_mvs:                             # optional: supply new MV definitions (see below)
#   events_daily_mv: events_daily_mv.sql
# companion_migration: 000042_reorder   # optional metadata for your ReconcileGuard
# dress_rehearsal_version: 24.8.1        # Plan refuses a different server version unless forced
backfill:                        # all optional — server-adapted defaults otherwise
  target_rows_per_chunk: 50000000
  memory_fraction: 0.3           # external GROUP BY at this fraction of server RAM
  max_buckets: 256
  # rate_limit_bytes_per_sec, max_execution_time, bucket_key
```

### Changing the MVs (adding a sourced column)

By default the rebuilder re-emits each feeding MV **verbatim** from its live
definition — it automates key/DDL changes (reorder the `ORDER BY`, change engine
settings, add a `DEFAULT`/`MATERIALIZED` column) while the MV projection stays the
same.

To also change what the MVs produce — e.g. add a **sourced** dimension to the
aggregate (a new column added to the source tables *and* to each MV's `SELECT` /
`GROUP BY`, which changes the aggregation grain) — give the spec the **new MV
definitions** via `new_mvs`. The rebuilder then arms, backfills, and (at cutover)
recreates those MVs from the new definitions, so the new key **and** the new MVs
swap in atomically. The historical backfill re-aggregates at the new grain;
measure totals are invariant under a finer `GROUP BY`, so `sum(...)` validations
still hold exactly.

```go
spec.SetNewDDL(newTargetDDL)               // v2 target: new column + new ORDER BY
spec.SetMVDDL("raw_web_mv", newWebMVDDL)    // MV SELECT/GROUP BY now includes the column
spec.SetMVDDL("raw_mobile_mv", newMobileMV) // ...one per feeding MV
```

Author each new MV for the real target/MV names with no `WHERE` (the boundary is
added), and add the column to the source tables before running. MVs without a
`new_mvs` entry keep their live definition.

chtool migrates the *aggregate*, not the raw tables — so the source `ALTER` is
yours to do first. To keep that boundary safe, `Plan` and `Run` **preflight**
every MV: each definition's `SELECT` is resolved against its live source (a
zero-row probe) before anything is created or armed, so a missing source column
fails fast with an actionable error instead of half-building the rebuild.

### Memory-safe backfill

Before backfilling, `DeriveTuning` probes the live server for its RAM and which
query settings are adjustable, then sets `max_bytes_before_external_group_by` to
`memory_fraction` of RAM and `max_memory_usage` to twice that (the merge stage
can't spill). Each chunk is split into deterministic hash buckets sized to
`target_rows_per_chunk`. Settings the service marks read-only are skipped rather
than forced.

### Custom state store

`StateStore` is a four-method interface (`Ensure`, `Append`, `Records`,
`SpecHashSeen`). `NewSQLStore` gives you an append-only ClickHouse-backed default;
implement the interface yourself to journal progress anywhere else.

**Prefer keeping `SQLStore` over reimplementing it.** To hold rebuild state in
your own wider table — one that also carries your operator audit columns, say —
point `NewSQLStore` at that table and call `UseExistingTable()`:

```go
store := rebuild.NewSQLStore(conn, "analytics.ops").UseExistingTable()
```

That declares the table yours: `Ensure` runs no DDL, so it can't race your
migration and win by creating a narrow table. On first use it verifies the table
instead — checking it exists, provides `RequiredColumns`, and has a **server-side
default on `ts`** — and fails with a message naming the table and the missing
columns, rather than letting the mistake surface as a raw driver error partway
through a rebuild. The check is cached, so it stays off the per-append path.

The `ts` default earns its own check because getting it wrong fails *silently*:
appends omit `ts` so the server stamps it, so a column without a default takes
the zero value on every row, ordering collapses onto `seq` alone, and `seq`
restarts per store. That is the ordering bug the tiebreaker exists to prevent —
and reimplementing `StateStore` without `seq` is how it was hit for real: it can
rewind a backfill cursor, re-run a chunk, and double-count rows into an
`AggregatingMergeTree`.

---

## `chtool/chtest` — a throwaway ClickHouse for tests

Starting a scratch ClickHouse is the boilerplate every consumer ends up writing
— image pin, readiness polling, cleanup — and then drifting apart on. `chtest`
is that helper, once.

It is a **separate Go module**, so testcontainers-go and the Docker SDK never
enter the dependency graph of the main `chtool` module. Require it only from the
code that runs tests:

```bash
go get github.com/stsepelin/chtool/chtest
```

```go
func TestSomething(t *testing.T) {
    dsn := chtest.Start(t)                       // container + readiness + t.Cleanup
    conn, _ := chtool.Open(t.Context(), dsn)
    // ...
}
```

For a package with several integration tests, share one container from
`TestMain` — repeatedly creating and destroying servers is what makes
ClickHouse's native handshake time out intermittently:

```go
func TestMain(m *testing.M) {
    dsn, cleanup, err := chtest.StartMain()
    if err != nil {
        log.Fatal(err)
    }
    testDSN = dsn
    code := m.Run()
    cleanup()          // before os.Exit, which skips defers
    os.Exit(code)
}
```

| Symbol | Purpose |
|---|---|
| `Start(tb)` | Container + readiness wait + `tb.Cleanup`; returns a DSN |
| `StartWith(tb, Options{…})` | Same, with an explicit image |
| `StartMain()` | Same, for `TestMain`: returns `(dsn, cleanup, err)` |
| `StartContainer(Options{…})` | The `testing.TB`-free entry point the others wrap — usable from a tool, not just a test |
| `WithDatabase(dsn, db)` | Repoint a DSN at another database (create it yourself) |
| `Image` | The shared default image pin |
| `Env()` | The environment a scratch OSS container needs (see below) |

**It composes with CI rather than replacing it.** If `CHTOOL_TEST_DSN` is set,
no container is started and that DSN is handed back — so the same test code runs
against a service container in CI and a throwaway container on a laptop.

### Choosing the image

`Image` is the shared default, and using it is the point: unmanaged pins drifting
apart across repos is the failure mode this package exists to stop.

Tracking a *specific* server is not that failure mode. A repo that deploys
against a managed ClickHouse — or that generates a committed schema artifact from
a scratch container and diffs it against that server — has to match the version
it deploys against, or it risks systematic false drift in its own safety check.
So deviating is possible, but explicit and greppable:

```go
// Pinned in code, as one repo-wide constant: the single source of truth.
dsn := chtest.StartWith(t, chtest.Options{Image: internal.ClickHouseImage})
```

```bash
# Or from the environment, so a CI matrix can vary it with no code change.
CHTOOL_TEST_IMAGE=clickhouse/clickhouse-server:26.2 go test -tags integration ./...
```

`Options.Image` wins over `CHTOOL_TEST_IMAGE`, which wins over `Image` — an
explicit argument should not be silently overridden by an ambient variable. Any
non-default image is logged with where it came from, so an unexpected override
shows up in test output rather than passing unnoticed.

Keep an override in **exactly one place** in the consuming repo. One pin that
deliberately tracks a server is a decision; the same pin copied into three
packages is the drift this package is trying to prevent.

**The gotcha `Env()` encodes:** the official image's entrypoint disables network
access for the `default` user unless told otherwise, logging *"neither
CLICKHOUSE_USER nor CLICKHOUSE_PASSWORD is set, disabling network access for
user 'default'"*. The container then looks healthy — a `clickhouse-client`
health check passes over the local socket — while every connection from outside
is refused. Setting `CLICKHOUSE_DB` alone does **not** fix it.

## ClickHouse Cloud

`chtool` targets Cloud as a first-class case:

- **`chtool.Open`** turns on verified TLS automatically for Cloud hosts.
- **`chtool/schema`** normalizes Cloud's `Shared*`/`Replicated*` engines to OSS, so
  schema snapshots taken from Cloud and OSS environments compare equal.
- **`chtool/schema.Lint`** flags `ON CLUSTER` and `POPULATE`, which Cloud replicates
  transparently / discourages.
- **`chtool/rebuild`** adapts backfill memory settings to the service's reported RAM
  and skips settings the service has made read-only.

## Testing

**Unit tests** run with no external dependencies:

```bash
go test ./...
```

**Integration tests** exercise every DB-bound path — connecting, migrations, dump,
insert/verify, and a full online rebuild + cutover — against a **real ClickHouse**.
They live behind the `integration` build tag, read the server from
`CHTOOL_TEST_DSN` (default `clickhouse://localhost:9000/default`), skip when it's
unreachable, and each uses its own scratch database that it drops afterward:

```bash
# e.g. against a local container:
#   docker run -d --name ch -p 9000:9000 -p 8123:8123 \
#     -e CLICKHOUSE_SKIP_USER_SETUP=1 clickhouse/clickhouse-server:24.8
CHTOOL_TEST_DSN=clickhouse://localhost:9000/default go test -tags integration ./...
```

Note the `CLICKHOUSE_SKIP_USER_SETUP=1` — without it the container refuses
connections from outside while still looking healthy. [`chtool/chtest`](#chtoolchtest--a-throwaway-clickhouse-for-tests)
encodes that (and the readiness wait) so you do not have to.

`chtest` is a separate module, so its own tests run separately:

```bash
cd chtest && go test ./...          # starts a real container
cd chtest && go test -short ./...   # skips it
```

CI runs unit tests (with `-race` + coverage), the integration suite against a
ClickHouse service container, the nested `chtest` module, `golangci-lint`, and
`govulncheck` on every push and PR — see
[`.github/workflows/ci.yml`](.github/workflows/ci.yml). One CI step asserts the
main module's build graph never picks up Docker/testcontainers packages, which
is the whole reason `chtest` is a separate module.

## Contributing

```bash
go test ./...                # unit tests
go vet ./...                 # vet
gofmt -l .                   # must print nothing
golangci-lint run            # lint (config in .golangci.yml)
```

Please keep changes formatted (`gofmt`), lint-clean, and covered by a test.
Releases are cut by pushing a semver tag (`git tag v0.1.0 && git push origin
v0.1.0`); the [release workflow](.github/workflows/release.yml) validates, creates
the GitHub release, and warms the module proxy.

## License

MIT — see [LICENSE](LICENSE).
