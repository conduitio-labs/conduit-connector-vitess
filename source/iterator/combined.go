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
	"sync"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"google.golang.org/grpc"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

const (
	// defaultRecordsBufferSize is a default value used for the records chan buffer size.
	defaultRecordsBufferSize = 1000

	// metadataKeyTable is a metadata key for table.
	metadataKeyTable = "table"
)

var (
	once sync.Once

	// vitessProtocolName is a name of a custom protocol
	// used for register a new Dialer with GRPC authentication.
	// If the GRPC options is not present, the value will be the default "grpc" one.
	vitessProtocolName = "conduit_vitess_grpc"
)

// Combined is a combined iterator that contains both snapshot and cdc iterators.
type Combined struct {
	snapshot *Snapshot
	cdc      *CDC

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
	Username       string
	Password       string
	Position       *Position
}

// NewCombined creates new instance of the Combined.
func NewCombined(ctx context.Context, params CombinedParams) (*Combined, error) {
	once.Do(func() {
		registerCustomVitessDialer(ctx, params.Username, params.Password)
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

	switch position := params.Position; {
	case position == nil || position.Mode == ModeSnapshot:
		combined.snapshot, err = NewSnapshot(ctx, SnapshotParams{
			Address:        params.Address,
			Keyspace:       params.Keyspace,
			TabletType:     params.TabletType,
			Table:          params.Table,
			KeyColumn:      params.KeyColumn,
			OrderingColumn: params.OrderingColumn,
			Columns:        params.Columns,
			BatchSize:      params.BatchSize,
			Position:       params.Position,
		})
		if err != nil {
			return nil, fmt.Errorf("init snapshot iterator: %w", err)
		}

	case position.Mode == ModeCDC:
		combined.cdc, err = NewCDC(ctx, CDCParams{
			Address:        params.Address,
			Keyspace:       params.Keyspace,
			TabletType:     params.TabletType,
			Table:          params.Table,
			KeyColumn:      params.KeyColumn,
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
		hasNext, err := c.snapshot.HasNext(ctx)
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
		return false, nil
	}
}

// Next returns the next record.
func (c *Combined) Next(ctx context.Context) (sdk.Record, error) {
	switch {
	case c.snapshot != nil:
		return c.snapshot.Next(ctx)

	case c.cdc != nil:
		return c.cdc.Next(ctx)

	default:
		return sdk.Record{}, ErrNoInitializedIterator
	}
}

// Stops the underlying iterators.
func (c *Combined) Stop(ctx context.Context) error {
	if c.snapshot != nil {
		return c.snapshot.Stop(ctx)
	}

	if c.cdc != nil {
		return c.cdc.Stop(ctx)
	}

	return nil
}

// switchToCDCIterator initializes the cdc iterator, and set the snapshot to nil.
func (c *Combined) switchToCDCIterator(ctx context.Context) error {
	var err error

	c.cdc, err = NewCDC(ctx, CDCParams{
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

	if err := c.snapshot.Stop(ctx); err != nil {
		return fmt.Errorf("stop snapshot iterator: %w", err)
	}

	c.snapshot = nil

	return nil
}

// registerCustomVitessDialer registers a custom dialer. If the username and password arguments are provided,
// GRPC authentication will be enabled.
func registerCustomVitessDialer(ctx context.Context, username, password string) {
	var grpcDialOptions []grpc.DialOption
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

	if len(grpcDialOptions) == 0 {
		// if grpcDialOptions is nil we don't need to register a custom protocol,
		// we'll just use the default one.
		vitessProtocolName = *vtgateconn.VtgateProtocol

		return
	}

	vtgateconn.RegisterDialer(vitessProtocolName, grpcvtgateconn.DialWithOpts(ctx, grpcDialOptions...))
}
