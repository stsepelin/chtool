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
| `Up(fsys, dsn)` | Apply all pending migrations |
| `Steps(fsys, dsn, n)` | Apply (`n>0`) or revert (`n<0`) `n` migrations |
| `Force(fsys, dsn, version)` | Set the version without running SQL (dirty-state recovery) |
| `Version(fsys, dsn)` | Current `(version, dirty, err)` |
| `New(fsys, dsn)` | Build a `*migrate.Migrate` for advanced use |

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
    // ReconcileGuard: func() error { ... } // optional gate run before cutover
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
# companion_migration: 000042_reorder   # optional metadata for your ReconcileGuard
# dress_rehearsal_version: 24.8.1        # Plan refuses a different server version unless forced
backfill:                        # all optional — server-adapted defaults otherwise
  target_rows_per_chunk: 50000000
  memory_fraction: 0.3           # external GROUP BY at this fraction of server RAM
  max_buckets: 256
  # rate_limit_bytes_per_sec, max_execution_time, bucket_key
```

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

---

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
#   docker run -d --name ch -p 9000:9000 -p 8123:8123 clickhouse/clickhouse-server
CHTOOL_TEST_DSN=clickhouse://localhost:9000/default go test -tags integration ./...
```

CI runs unit tests (with `-race` + coverage), the integration suite against a
ClickHouse service container, `golangci-lint`, and `govulncheck` on every push and
PR — see [`.github/workflows/ci.yml`](.github/workflows/ci.yml).

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
