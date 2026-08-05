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
//   - drive the sequence one migration at a time, checking ctx between them.
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
// # Why not GracefulStop
//
// golang-migrate offers GracefulStop for exactly this, and it is deliberately
// unused here: Migrate.stop() reads and writes the unsynchronised
// Migrate.isGracefulStop from both the migration-running goroutine and the
// read-ahead producer goroutine, so signalling it is a data race (v4.19.1).
// Benign in effect — the flag is only ever set to true — but it would fire the
// race detector in any consumer that cancels a migration under -race.
//
// Stepping avoids the flag entirely and reaches the same break points, at one
// cost worth knowing: a cancellable run takes golang-migrate's migration lock
// per migration rather than once for the whole sequence, so a second migrator
// could interleave between steps. A non-cancellable ctx (context.Background,
// and so every non-Context function here) skips stepping and runs the sequence
// in a single locked call, exactly as before.
package migrate

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
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
		// Not cancellable: run the whole sequence in one call, under one lock.
		if ctx.Done() == nil {
			if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
				return err
			}
			return nil
		}
		return stepAll(ctx, m)
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
		// Not cancellable: run the whole sequence in one call, under one lock.
		if ctx.Done() == nil {
			if err := m.Steps(n); err != nil && !errors.Is(err, migrate.ErrNoChange) {
				return err
			}
			return nil
		}
		return stepN(ctx, m, n)
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

// withContext is with, refusing to start at all on an already-done ctx.
func withContext(ctx context.Context, fsys fs.FS, dsn string, fn func(*migrate.Migrate) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return with(fsys, dsn, fn)
}

// stopped wraps a cancellation so callers can tell why the run ended and what
// to do about it.
func stopped(ctxErr error) error {
	return fmt.Errorf("%w: %w; it stopped between migrations, so the recorded version may be mid-sequence — re-check with Version",
		ErrStoppedEarly, ctxErr)
}

// stepAll applies every pending migration one at a time, checking ctx between
// them. Driving the sequence a migration at a time is what makes cancellation
// possible without golang-migrate's GracefulStop, whose stop flag it races on
// (see the package docs). The break points are identical either way: a
// migration is never interrupted once it has started.
func stepAll(ctx context.Context, m *migrate.Migrate) error {
	for {
		if err := ctx.Err(); err != nil {
			return stopped(err)
		}
		switch err := m.Steps(1); {
		case err == nil:
		case exhausted(err):
			return nil
		default:
			return err
		}
	}
}

// stepN applies (n>0) or reverts (n<0) n migrations one at a time, checking ctx
// between them. Running short reports what a single m.Steps(n) would have: the
// upstream error when nothing moved, ErrShortLimit when only some did.
func stepN(ctx context.Context, m *migrate.Migrate, n int) error {
	step, count := 1, n
	if n < 0 {
		step, count = -1, -n
	}
	for applied := range count {
		if err := ctx.Err(); err != nil {
			return stopped(err)
		}
		switch err := m.Steps(step); {
		case err == nil:
		case errors.Is(err, migrate.ErrNoChange):
			return nil
		case exhausted(err):
			if applied == 0 {
				return err
			}
			return migrate.ErrShortLimit{Short: uint(count - applied)}
		default:
			return err
		}
	}
	return nil
}

// exhausted reports whether err means the source ran out of migrations rather
// than that something went wrong.
func exhausted(err error) bool {
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, migrate.ErrNoChange) {
		return true
	}
	var short migrate.ErrShortLimit
	return errors.As(err, &short)
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
