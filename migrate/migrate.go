// Package migrate is a razor-thin wrapper over golang-migrate for ClickHouse:
// apply/revert/inspect/force a set of migration files provided as an fs.FS
// (e.g. an embed.FS). It keeps golang-migrate's default schema_migrations state
// table and injects x-multi-statement=true so multi-statement files apply over
// the native protocol.
//
// This subpackage pulls in golang-migrate; import it only if you need
// migrations. The rest of chtool does not depend on it.
package migrate

import (
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

// Up applies all pending migrations (no-op when already current).
func Up(fsys fs.FS, dsn string) error {
	return with(fsys, dsn, func(m *migrate.Migrate) error {
		if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			return err
		}
		return nil
	})
}

// Steps applies (n>0) or reverts (n<0) n migrations.
func Steps(fsys fs.FS, dsn string, n int) error {
	return with(fsys, dsn, func(m *migrate.Migrate) error {
		if err := m.Steps(n); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			return err
		}
		return nil
	})
}

// Force sets the migration version without running SQL (dirty-state recovery).
func Force(fsys fs.FS, dsn string, version int) error {
	return with(fsys, dsn, func(m *migrate.Migrate) error { return m.Force(version) })
}

// Version returns the current applied version and dirty flag. A fresh database
// yields (0, false, nil).
func Version(fsys fs.FS, dsn string) (version uint, dirty bool, err error) {
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
