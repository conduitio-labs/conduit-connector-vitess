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

	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	// defaultRecordsBufferSize is a default value used for the records chan buffer size.
	defaultRecordsBufferSize = 1000

	// metadataKeyTable is a metadata key for table.
	metadataKeyTable = "table"
	// metadataKeyTable is a metadata key for action.
	metadataKeyAction = "action"

	// actionInsert is a value for the metadataKeyAction that represents insert action.
	actionInsert = "insert"
	// actionUpdate is a value for the metadataKeyAction that represents update action.
	actionUpdate = "update"
	// actionDelete is a value for the metadataKeyAction that represents delete action.
	actionDelete = "delete"
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
	Position       *Position
}

// NewCombined creates new instance of the Combined.
func NewCombined(ctx context.Context, params CombinedParams) (*Combined, error) {
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
	c.snapshot = nil

	return nil
}
