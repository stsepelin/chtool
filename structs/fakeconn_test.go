package structs

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// fakeConn is a minimal programmable driver.Conn for the struct helpers: it
// serves a batch for Insert and a single-column row set for liveColumns.
type fakeConn struct {
	driver.Conn
	batch       *fakeBatch
	prepareErr  error
	liveColumns []string
	queryErr    error
}

func (c *fakeConn) PrepareBatch(_ context.Context, _ string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	if c.prepareErr != nil {
		return nil, c.prepareErr
	}
	if c.batch == nil {
		c.batch = &fakeBatch{}
	}
	return c.batch, nil
}

func (c *fakeConn) Query(_ context.Context, _ string, _ ...any) (driver.Rows, error) {
	if c.queryErr != nil {
		return nil, c.queryErr
	}
	return &fakeRows{cols: c.liveColumns, i: -1}, nil
}

type fakeBatch struct {
	driver.Batch
	appended int
	sent     bool
}

func (b *fakeBatch) AppendStruct(any) error { b.appended++; return nil }
func (b *fakeBatch) Send() error            { b.sent = true; return nil }

type fakeRows struct {
	driver.Rows
	cols []string
	i    int
}

func (r *fakeRows) Next() bool { r.i++; return r.i < len(r.cols) }
func (r *fakeRows) Scan(dest ...any) error {
	p, ok := dest[0].(*string)
	if !ok {
		return fmt.Errorf("unexpected scan dest %T", dest[0])
	}
	*p = r.cols[r.i]
	return nil
}
func (r *fakeRows) Close() error { return nil }
func (r *fakeRows) Err() error   { return nil }
