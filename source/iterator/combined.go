// Copyright © 2022 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package iterator

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"google.golang.org/grpc"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	"github.com/conduitio-labs/conduit-connector-vitess/retrydialer"
)

const (
	// defaultRecordsBufferSize is a default value used for the records chan buffer size.
	defaultRecordsBufferSize = 1000

	// metadataKeyTable is a metadata key for table.
	metadataKeyTable = "vitess.table"
)

var (
	// vitessProtocolName is a name of a custom protocol
	// used for register a new Dialer with GRPC authentication.
	// If the GRPC options is not present, the value will be the default "grpc" one.
	vitessProtocolName = "conduit_vitess_grpc"

	// queryTablePrimaryKey is a SQL query that returns the first primary key found in a table.
	queryTablePrimaryKey = `select column_name
	from information_schema.key_column_usage
	where table_name = '%s' and constraint_name = 'primary'
	limit 1;`

	once sync.Once
)

// Combined is a combined iterator that contains both snapshot and cdc iterators.
type Combined struct {
	snapshot *snapshot
	cdc      *cdc

	conn           *vtgateconn.VTGateConn
	address        string
	keyspace       string
	tabletType     string
	table          string
	keyColumn      string
	orderingColumn string
	columns        []string
}

// CombinedParams is an incoming params for the NewCombined function.
type CombinedParams struct {
	Address        string
	Table          string
	KeyColumn      string
	Keyspace       string
	TabletType     string
	OrderingColumn string
	Columns        []string
	BatchSize      int
	Snapshot       bool
	Username       string
	Password       string
	MaxRetries     int
	RetryTimeout   time.Duration
	Position       *Position
}

// NewCombined creates new instance of the Combined.
func NewCombined(ctx context.Context, params CombinedParams) (*Combined, error) {
	once.Do(func() {
		registerCustomVitessDialer(ctx, params.MaxRetries, params.RetryTimeout, params.Username, params.Password)
	})

	var (
		err      error
		combined = &Combined{
			address:        params.Address,
			keyspace:       params.Keyspace,
			tabletType:     params.TabletType,
			table:          params.Table,
			keyColumn:      params.KeyColumn,
			orderingColumn: params.OrderingColumn,
			columns:        params.Columns,
		}
	)

	combined.conn, err = vtgateconn.DialProtocol(ctx, vitessProtocolName, combined.address)
	if err != nil {
		return nil, fmt.Errorf("vtgateconn dial: %w", err)
	}

	combined.keyColumn, err = combined.getKeyColumn(ctx)
	if err != nil {
		return nil, fmt.Errorf("get key column: %w", err)
	}

	switch position := params.Position; {
	case params.Snapshot && (position == nil || position.Mode == ModeSnapshot):
		combined.snapshot, err = newSnapshot(ctx, snapshotParams{
			Conn:           combined.conn,
			Keyspace:       params.Keyspace,
			TabletType:     params.TabletType,
			Table:          params.Table,
			KeyColumn:      combined.keyColumn,
			OrderingColumn: params.OrderingColumn,
			Columns:        params.Columns,
			BatchSize:      params.BatchSize,
			Position:       params.Position,
		})
		if err != nil {
			return nil, fmt.Errorf("init snapshot iterator: %w", err)
		}

	case !params.Snapshot || (position != nil && position.Mode == ModeCDC):
		combined.cdc, err = newCDC(ctx, cdcParams{
			Conn:           combined.conn,
			Address:        params.Address,
			Keyspace:       params.Keyspace,
			TabletType:     params.TabletType,
			Table:          params.Table,
			KeyColumn:      combined.keyColumn,
			OrderingColumn: params.OrderingColumn,
			Columns:        params.Columns,
			Position:       params.Position,
		})
		if err != nil {
			return nil, fmt.Errorf("init cdc iterator: %w", err)
		}

	default:
		return nil, fmt.Errorf("invalid position mode %q", params.Position.Mode)
	}

	return combined, nil
}

// HasNext returns a bool indicating whether the iterator has the next record to return or not.
// If the underlying snapshot iterator returns false, the combined iterator will try to switch to the cdc iterator.
func (c *Combined) HasNext(ctx context.Context) (bool, error) {
	switch {
	case c.snapshot != nil:
		hasNext, err := c.snapshot.hasNext(ctx)
		if err != nil {
			return false, fmt.Errorf("snapshot has next: %w", err)
		}

		if !hasNext {
			if err := c.switchToCDCIterator(ctx); err != nil {
				return false, fmt.Errorf("switch to cdc iterator: %w", err)
			}

			return false, nil
		}

		return true, nil

	case c.cdc != nil:
		return c.cdc.HasNext(ctx)

	default:
		// this shouldn't happen
		return false, ErrNoIterator
	}
}

// Next returns the next record.
func (c *Combined) Next(ctx context.Context) (sdk.Record, error) {
	switch {
	case c.snapshot != nil:
		return c.snapshot.next(ctx)

	case c.cdc != nil:
		return c.cdc.Next(ctx)

	default:
		return sdk.Record{}, ErrNoInitializedIterator
	}
}

// Stop stops the underlying iterators and closes a database connection.
func (c *Combined) Stop(ctx context.Context) error {
	if c.snapshot != nil {
		return c.snapshot.stop(ctx)
	}

	if c.cdc != nil {
		return c.cdc.Stop(ctx)
	}

	if c.conn != nil {
		c.conn.Close()
	}

	return nil
}

// switchToCDCIterator initializes the cdc iterator, and set the snapshot to nil.
func (c *Combined) switchToCDCIterator(ctx context.Context) error {
	var err error

	c.cdc, err = newCDC(ctx, cdcParams{
		Conn:           c.conn,
		Address:        c.address,
		Keyspace:       c.keyspace,
		TabletType:     c.tabletType,
		Table:          c.table,
		KeyColumn:      c.keyColumn,
		OrderingColumn: c.orderingColumn,
		Columns:        c.columns,
	})
	if err != nil {
		return fmt.Errorf("init cdc iterator: %w", err)
	}

	if err := c.snapshot.stop(ctx); err != nil {
		return fmt.Errorf("stop snapshot iterator: %w", err)
	}

	c.snapshot = nil

	return nil
}

// getKeyColumn first looks at the c.keyColumn and returns it if it's not empty,
// otherwise it tries to get a primary key from a database and returns it if found.
// If both cases are not true the method returns c.orderingColumn.
func (c *Combined) getKeyColumn(ctx context.Context) (string, error) {
	if c.keyColumn != "" {
		return c.keyColumn, nil
	}

	target := strings.Join([]string{c.keyspace, c.tabletType}, "@")
	session := c.conn.Session(target, &query.ExecuteOptions{
		IncludedFields: query.ExecuteOptions_ALL,
	})

	query := fmt.Sprintf(queryTablePrimaryKey, c.table)

	result, err := session.Execute(ctx, query, nil)
	if err != nil {
		return "", fmt.Errorf("session execute: %w", err)
	}

	// get the first row as a raw string
	// if the resulting set of rows is not empty.
	if len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
		if primaryKeyColumnName := result.Rows[0][0].RawStr(); primaryKeyColumnName != "" {
			return primaryKeyColumnName, nil
		}
	}

	return c.orderingColumn, nil
}

// registerCustomVitessDialer registers a custom dialer. If the username and password arguments are provided,
// GRPC authentication will be enabled.
func registerCustomVitessDialer(
	ctx context.Context,
	maxRetries int,
	retryTimeout time.Duration,
	username, password string,
) {
	var grpcDialOptions = []grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
			return retrydialer.DialWithRetries(ctx, maxRetries, retryTimeout, address)
		}),
		grpc.FailOnNonTempDialError(true),
		grpc.WithBlock(),
	}

	if username != "" && password != "" {
		grpcDialOptions = append(grpcDialOptions,
			grpc.WithPerRPCCredentials(
				&grpcclient.StaticAuthClientCreds{
					Username: username,
					Password: password,
				},
			),
		)
	}

	vtgateconn.RegisterDialer(vitessProtocolName, grpcvtgateconn.DialWithOpts(ctx, grpcDialOptions...))
}
