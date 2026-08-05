// Package chtest starts a throwaway ClickHouse for integration tests.
//
// It exists so that every consumer of chtool stops reimplementing the same
// container boilerplate — image pin, readiness polling, cleanup — and drifting
// apart in the process.
//
// It is a separate Go module (github.com/stsepelin/chtool/chtest) precisely so
// that testcontainers-go and the Docker SDK never enter the dependency graph of
// the main chtool module. Require it only from the code that runs tests.
//
// # Using an existing server
//
// If CHTOOL_TEST_DSN is set, no container is started and that DSN is returned
// as-is. That is what makes this compose with a CI service container rather
// than replace it: the same test code runs against a service container in CI
// and a throwaway container on a laptop.
//
// # Sharing one container
//
// Start gives each caller a container scoped to one test. For a package with
// many integration tests, prefer StartMain from TestMain: repeatedly creating
// and destroying servers is what makes ClickHouse's native handshake time out
// intermittently under load.
//
// # Choosing the image
//
// Image is the shared default, and using it is the point: unmanaged pins
// drifting apart across repos is the failure mode this package exists to stop.
//
// Tracking a specific server is not that failure mode, though. A repo that
// deploys against a managed ClickHouse — or that generates a committed schema
// artifact from a scratch container and diffs it against that server — needs to
// match the version it deploys against, or it risks systematic false drift.
// Deviating is therefore possible, but explicit and greppable:
//
//   - CHTOOL_TEST_IMAGE redirects the image the way CHTOOL_TEST_DSN redirects
//     the whole server, so a CI matrix can vary it without code changes;
//   - Options.Image (via StartWith or StartContainer) pins it in code, for a
//     repo whose version is a deliberate, single-source-of-truth decision.
//
// Options.Image wins over the environment: an explicit argument should not be
// overridden by an ambient variable. Whenever the image is not Image, it is
// logged with where it came from, so an unexpected override shows up in test
// output instead of being silent.
//
// Keep an override in exactly one place in the consuming repo. One pin that
// deliberately tracks a server is a decision; the same pin copied into three
// packages is the drift this package is trying to prevent.
package chtest

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	// Registers the "clickhouse" database/sql driver that the readiness probe
	// below opens to prove the native protocol is actually serving.
	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Image is the default pin consumers share. Change it here and everyone who has
// not deliberately overridden it moves together. See the package docs on
// choosing the image before overriding it.
const Image = "clickhouse/clickhouse-server:24.8"

// DSNEnv is the environment variable that, when set, makes the Start functions
// hand back an existing server instead of starting a container.
const DSNEnv = "CHTOOL_TEST_DSN"

// ImageEnv is the environment variable that, when set, overrides Image.
// Options.Image takes precedence over it.
const ImageEnv = "CHTOOL_TEST_IMAGE"

// Options configures a container. The zero value is the shared default.
type Options struct {
	// Image overrides both Image and ImageEnv. Set it when the version is a
	// deliberate decision for the repo — tracking the managed server you deploy
	// against, say — and keep it in one place.
	Image string

	// Logf receives progress, notably which image was used when it is not the
	// default. Start and StartWith default it to the test's log; StartContainer
	// leaves it nil, which discards.
	Logf func(format string, args ...any)
}

func (o Options) logf(format string, args ...any) {
	if o.Logf != nil {
		o.Logf(format, args...)
	}
}

// resolveImage returns the image to run and where it came from. An empty source
// means the shared default, which is not worth announcing.
func resolveImage(opts Options) (image, source string) {
	if img := strings.TrimSpace(opts.Image); img != "" {
		return img, "Options.Image"
	}
	if img := strings.TrimSpace(os.Getenv(ImageEnv)); img != "" {
		return img, ImageEnv
	}
	return Image, ""
}

const (
	nativePort = "9000/tcp"
	httpPort   = "8123/tcp"

	startupTimeout = 2 * time.Minute
)

// Env is the environment a scratch OSS ClickHouse container needs for the
// default user to accept connections over the Docker bridge.
//
// This is the one that bites everyone once: the image's entrypoint disables
// network access for the default user unless it is told otherwise, logging
// "neither CLICKHOUSE_USER nor CLICKHOUSE_PASSWORD is set, disabling network
// access for user 'default'". The container then looks healthy — a
// clickhouse-client health check passes over the local socket — while every
// connection from outside the container is refused. Setting CLICKHOUSE_DB alone
// does not fix it.
//
// It returns a fresh map so callers can add to it without mutating the package.
func Env() map[string]string {
	return map[string]string{
		"CLICKHOUSE_SKIP_USER_SETUP": "1",
	}
}

// Start returns a DSN for a ClickHouse the test can use, terminating it via
// tb.Cleanup. If CHTOOL_TEST_DSN is set it is returned as-is and no container
// is started.
//
// Each call starts its own container. For a package with several integration
// tests, share one via StartMain instead.
func Start(tb testing.TB) string {
	tb.Helper()
	return StartWith(tb, Options{})
}

// StartWith is Start with explicit options — typically Options.Image, to track
// a specific server version. See the package docs on choosing the image.
func StartWith(tb testing.TB, opts Options) string {
	tb.Helper()
	if opts.Logf == nil {
		opts.Logf = tb.Logf
	}
	dsn, cleanup, err := StartContainer(opts)
	if err != nil {
		tb.Fatalf("chtest: %v", err)
	}
	tb.Cleanup(cleanup)
	return dsn
}

// StartMain is Start without a testing.TB, for sharing one container across a
// package from TestMain. The returned cleanup is always non-nil and safe to
// call even when err is non-nil, so it can be deferred immediately.
//
//	func TestMain(m *testing.M) {
//		dsn, cleanup, err := chtest.StartMain()
//		if err != nil {
//			log.Fatal(err)
//		}
//		testDSN = dsn
//		code := m.Run()
//		cleanup()
//		os.Exit(code)
//	}
//
// Note that cleanup must run before os.Exit, which does not run deferred
// functions.
func StartMain() (dsn string, cleanup func(), err error) {
	return StartContainer(Options{})
}

// StartContainer starts a throwaway ClickHouse and returns its DSN with a
// cleanup that terminates it. The returned cleanup is always non-nil and safe
// to call even when err is non-nil, so it can be deferred immediately.
//
// This is the entry point behind Start, StartWith and StartMain, and it takes
// no testing.TB deliberately: spinning a scratch server is a reasonable thing
// for a tool to do — generating a schema artifact to diff against a live
// server, say — not only for a test. Set Options.Logf to see progress.
//
// If CHTOOL_TEST_DSN is set, no container is started and that DSN is returned.
func StartContainer(opts Options) (dsn string, cleanup func(), err error) {
	image, source := resolveImage(opts)
	if source != "" {
		opts.logf("chtest: using image %s from %s (default is %s)", image, source, Image)
	}

	if existing := envDSN(); existing != "" {
		if source != "" {
			opts.logf("chtest: %s is set, so %s is being used and image %s is not started", DSNEnv, existing, image)
		}
		return existing, func() {}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Started: true,
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			ExposedPorts: []string{nativePort, httpPort},
			Env:          Env(),
			// Readiness is proved on both protocols. ForSQL opens the native
			// port and runs a query, which is the handshake tests actually
			// depend on — a listening port alone is not enough.
			WaitingFor: wait.ForAll(
				wait.ForHTTP("/ping").WithPort(httpPort).WithStartupTimeout(startupTimeout),
				wait.ForSQL(nativePort, "clickhouse", func(host string, port network.Port) string {
					return fmt.Sprintf("clickhouse://default@%s:%s/default", host, port.Port())
				}).WithStartupTimeout(startupTimeout),
			),
		},
	})
	if err != nil {
		return "", func() {}, fmt.Errorf("start clickhouse container (%s): %w", image, err)
	}
	terminate := func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_ = testcontainers.TerminateContainer(container, testcontainers.StopContext(ctx))
	}

	host, err := container.Host(ctx)
	if err != nil {
		terminate()
		return "", func() {}, fmt.Errorf("container host: %w", err)
	}
	port, err := container.MappedPort(ctx, nativePort)
	if err != nil {
		terminate()
		return "", func() {}, fmt.Errorf("container port: %w", err)
	}
	return fmt.Sprintf("clickhouse://default@%s:%s/default", host, port.Port()), terminate, nil
}

// envDSN returns the pre-existing server DSN, if the environment names one.
func envDSN() string { return strings.TrimSpace(os.Getenv(DSNEnv)) }

// WithDatabase returns dsn pointed at database db. It only rewrites the DSN —
// create the database yourself (CREATE DATABASE IF NOT EXISTS) before using it.
//
// A DSN that cannot be parsed is returned unchanged, so a malformed input
// surfaces as a connection error at the point of use rather than here.
func WithDatabase(dsn, db string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}
	u.Path = "/" + db
	return u.String()
}
