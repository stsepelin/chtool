package chtool

import (
	"context"
	"testing"
)

func TestIsLocal(t *testing.T) {
	cases := []struct {
		name  string
		addrs []string
		want  bool
	}{
		{"empty is not local (fail safe to TLS)", nil, false},
		{"localhost host:port", []string{"localhost:9000"}, true},
		{"127.0.0.1 host:port", []string{"127.0.0.1:9000"}, true},
		{"127.x loopback", []string{"127.5.6.7:9000"}, true},
		{"ipv6 loopback", []string{"[::1]:9000"}, true},
		{"bare localhost no port", []string{"localhost"}, true},
		{"cloud host", []string{"abc.clickhouse.cloud:9440"}, false},
		{"substring trap: db-localhost.example.com is NOT local", []string{"db-localhost.example.com:9000"}, false},
		{"substring trap: localhost.attacker.com is NOT local", []string{"localhost.attacker.com:9000"}, false},
		{"all must be local", []string{"localhost:9000", "10.0.0.1:9000"}, false},
		{"both loopback", []string{"localhost:9000", "127.0.0.1:9000"}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := isLocal(c.addrs); got != c.want {
				t.Errorf("isLocal(%v) = %v, want %v", c.addrs, got, c.want)
			}
		})
	}
}

func TestOpenRejectsBadDSN(t *testing.T) {
	if _, err := Open(context.Background(), "not-a-valid-dsn"); err == nil {
		t.Fatal("expected an error for a malformed DSN")
	}
}

// Open a well-formed DSN pointing at a dead endpoint: ParseDSN and clickhouse.Open
// succeed, then Ping fails. Covers the local (no-TLS) and remote (TLS) branches.
func TestOpenPingFailsLocal(t *testing.T) {
	// localhost:1 → connection refused, fast.
	if _, err := Open(context.Background(), "clickhouse://localhost:1/db"); err == nil {
		t.Fatal("expected a ping failure against a dead local port")
	}
}

func TestOpenPingFailsRemoteTLS(t *testing.T) {
	// A non-local host exercises the auto-TLS branch; .invalid never resolves.
	if _, err := Open(context.Background(), "clickhouse://nonexistent.invalid:9440/db"); err == nil {
		t.Fatal("expected a failure against an unresolvable remote host")
	}
}
