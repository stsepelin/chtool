// Package migrate is a razor-thin wrapper over golang-migrate for ClickHouse:
// apply/revert/inspect/force a set of migration files provided as an fs.FS
// (e.g. an embed.FS). It keeps golang-migrate's default schema_migrations state
// table and injects x-multi-statement=true so multi-statement files apply over
// the native protocol.
//
// This subpackage pulls in golang-migrate; import it only if you need
// migrations. The rest of chtool does not depend on it.
//
// # Cancellation
//
// The Context variants bound a run, but be precise about what that buys you.
// golang-migrate exposes no context.Context, so cancellation cannot reach a
// statement already in flight. What UpContext and StepsContext do is:
//
//   - return ctx.Err() immediately if ctx is already done, before any work;
//   - on cancellation mid-run, ask golang-migrate to stop at its next safe
//     break point — that is, after the in-flight migration finishes.
//
// So a run stops BETWEEN migrations, never mid-statement. That is the semantics
// you want on ClickHouse anyway: DDL is non-transactional, and killing it
// mid-statement is how you get the dirty state Force exists to repair.
//
// A cancelled run therefore leaves the schema at some version in the middle of
// the requested sequence. Those calls return an error wrapping both
// ErrStoppedEarly and ctx.Err(); re-check with Version to learn where you landed.
// A cancellation that races with normal completion is reported the same way
// (the outcome is genuinely ambiguous), so treat ErrStoppedEarly as "re-check",
// not as "nothing was applied".
//
// ForceContext and VersionContext honour ctx only before their call begins:
// each is a single metadata operation with no safe mid-point to stop at.
//
// # Cancellation trips the race detector
//
// Cancelling a run under -race reports a data race inside golang-migrate
// (v4.19.1), not in this package: Migrate.stop() reads and writes the
// unsynchronised Migrate.isGracefulStop from both the migration-running
// goroutine and the read-ahead producer goroutine. Merely using GracefulStop is
// enough to trip it.
//
// It is benign for correctness — the flag is only ever set to true, and whichever
// goroutine observes it halts the run (the producer returning closes the channel
// the runner ranges over) — but a consumer whose own suite runs with -race will
// see the report. If that matters, do not cancel migrations under -race until
// the fix lands upstream.
package migrate

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"strings"

	// clickhouse-go registers the "clickhouse" database/sql driver the
	// golang-migrate clickhouse driver opens.
	_ "github.com/ClickHouse/clickhouse-go/v2"
	migrate "github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/clickhouse"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

// ErrNoChange is re-exported so callers can treat "already up to date" as a
// non-error.
var ErrNoChange = migrate.ErrNoChange

// ErrStoppedEarly reports that a run was cut short by a cancelled context. It
// is wrapped alongside ctx.Err(), so both errors.Is(err, ErrStoppedEarly) and
// errors.Is(err, context.Canceled) hold. The run stopped between migrations, so
// the recorded version may sit mid-sequence — re-check it with Version.
var ErrStoppedEarly = errors.New("migration run stopped early")

// New builds a *migrate.Migrate from the migration files in fsys and a
// clickhouse:// DSN. The caller should Close it.
func New(fsys fs.FS, dsn string) (*migrate.Migrate, error) {
	src, err := iofs.New(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("open migrations fs: %w", err)
	}
	ndsn, err := normalizeDSN(dsn)
	if err != nil {
		return nil, err
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, ndsn)
	if err != nil {
		return nil, fmt.Errorf("init migrate: %w", err)
	}
	return m, nil
}

// Up applies all pending migrations (no-op when already current). It is
// UpContext with a background context: nothing can cancel it.
func Up(fsys fs.FS, dsn string) error { return UpContext(context.Background(), fsys, dsn) }

// UpContext applies all pending migrations, stopping between migrations if ctx
// is cancelled (see the package docs on Cancellation).
func UpContext(ctx context.Context, fsys fs.FS, dsn string) error {
	return withContext(ctx, fsys, dsn, func(m *migrate.Migrate) error {
		if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			return err
		}
		return nil
	})
}

// Steps applies (n>0) or reverts (n<0) n migrations. It is StepsContext with a
// background context: nothing can cancel it.
func Steps(fsys fs.FS, dsn string, n int) error {
	return StepsContext(context.Background(), fsys, dsn, n)
}

// StepsContext applies (n>0) or reverts (n<0) n migrations, stopping between
// migrations if ctx is cancelled (see the package docs on Cancellation).
func StepsContext(ctx context.Context, fsys fs.FS, dsn string, n int) error {
	return withContext(ctx, fsys, dsn, func(m *migrate.Migrate) error {
		if err := m.Steps(n); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			return err
		}
		return nil
	})
}

// Force sets the migration version without running SQL (dirty-state recovery).
func Force(fsys fs.FS, dsn string, version int) error {
	return ForceContext(context.Background(), fsys, dsn, version)
}

// ForceContext sets the migration version without running SQL. ctx is honoured
// before the call begins; the version write itself is a single metadata
// operation with no safe mid-point to stop at.
func ForceContext(ctx context.Context, fsys fs.FS, dsn string, version int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return with(fsys, dsn, func(m *migrate.Migrate) error { return m.Force(version) })
}

// Version returns the current applied version and dirty flag. A fresh database
// yields (0, false, nil).
func Version(fsys fs.FS, dsn string) (version uint, dirty bool, err error) {
	return VersionContext(context.Background(), fsys, dsn)
}

// VersionContext returns the current applied version and dirty flag. ctx is
// honoured before the call begins; the read itself is not interruptible.
func VersionContext(ctx context.Context, fsys fs.FS, dsn string) (version uint, dirty bool, err error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	err = with(fsys, dsn, func(m *migrate.Migrate) error {
		v, d, e := m.Version()
		if errors.Is(e, migrate.ErrNilVersion) {
			version, dirty = 0, false
			return nil
		}
		version, dirty = v, d
		return e
	})
	return version, dirty, err
}

func with(fsys fs.FS, dsn string, fn func(*migrate.Migrate) error) error {
	m, err := New(fsys, dsn)
	if err != nil {
		return err
	}
	defer func() { _, _ = m.Close() }()
	return fn(m)
}

// withContext runs fn with a watcher that converts ctx cancellation into
// golang-migrate's GracefulStop, which halts the run after the in-flight
// migration completes. A gracefully stopped run reports no error of its own, so
// cancellation is detected here and surfaced as ErrStoppedEarly.
func withContext(ctx context.Context, fsys fs.FS, dsn string, fn func(*migrate.Migrate) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m, err := New(fsys, dsn)
	if err != nil {
		return err
	}
	defer func() { _, _ = m.Close() }()

	// Registered after the Close defer, so it runs first: the watcher is torn
	// down before the Migrate it references is closed.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			// GracefulStop is buffered (cap 1); the default keeps this
			// non-blocking when a stop is already pending.
			select {
			case m.GracefulStop <- true:
			default:
			}
		case <-done:
		}
	}()

	runErr := fn(m)
	if ctxErr := ctx.Err(); ctxErr != nil {
		stopped := fmt.Errorf("%w: %w; it stopped between migrations, so the recorded version may be mid-sequence — re-check with Version",
			ErrStoppedEarly, ctxErr)
		if runErr != nil {
			return errors.Join(stopped, runErr)
		}
		return stopped
	}
	return runErr
}

func normalizeDSN(dsn string) (string, error) {
	if !strings.HasPrefix(dsn, "clickhouse://") {
		return "", fmt.Errorf("dsn must start with clickhouse://")
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("parse dsn: %w", err)
	}
	q := u.Query()
	if q.Get("x-multi-statement") == "" {
		q.Set("x-multi-statement", "true")
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}
