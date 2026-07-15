// Package chtool is a small ClickHouse operations toolkit built on
// clickhouse-go/v2. Its subpackages are independent — import only what you need:
//
//   - chtool          — connection helper (this package)
//   - chtool/migrate  — razor-thin golang-migrate wrapper (apply/status/force)
//   - chtool/schema   — schema snapshot, Cloud-aware normalize, drift, lint
//   - chtool/structs  — generic struct helpers (batch insert, tag verify, DDL)
//   - chtool/rebuild  — online AggregatingMergeTree rebuild orchestrator
package chtool

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Conn = driver.Conn

// Open parses a clickhouse:// DSN, enables verified TLS automatically for
// non-local hosts (ClickHouse Cloud requires it), connects and pings. The
// caller owns Close. For a self-signed cert, opt into skip-verify via the DSN
// (e.g. "?secure=true&skip_verify=true"), which ParseDSN honors and Open keeps.
func Open(ctx context.Context, dsn string) (Conn, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	if opts.TLS == nil && !isLocal(opts.Addr) {
		opts.TLS = &tls.Config{}
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}
	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := conn.Ping(pctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	return conn, nil
}

// isLocal reports whether every address is loopback. An empty list is not
// local, so TLS stays on.
func isLocal(addrs []string) bool {
	if len(addrs) == 0 {
		return false
	}
	for _, a := range addrs {
		host := a
		if h, _, err := net.SplitHostPort(a); err == nil {
			host = h
		}
		if !isLoopbackHost(host) {
			return false
		}
	}
	return true
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}
