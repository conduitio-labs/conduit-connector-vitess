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

package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/conduitio-labs/conduit-connector-vitess/coltypes"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/huandu/go-sqlbuilder"
	"github.com/jmoiron/sqlx"
	"vitess.io/vitess/go/vt/vitessdriver"
)

const (
	// metadataKeyTable is a metadata key for table.
	metadataKeyTable = "table"
	// metadataKeyTable is a metadata key for action.
	metadataKeyAction = "action"

	// actionInsert is a value for the metadataKeyAction that represents insert action.
	actionInsert = "insert"
)

// Iterator is an implementation of a Snapshot iterator for Vitess.
type Iterator struct {
	db   *sqlx.DB
	rows *sqlx.Rows

	table          string
	keyColumn      string
	orderingColumn string
	columns        []string
	columnTypes    map[string]string
	batchSize      int
	position       *Position
}

// SnaphostParams is an incoming params for the NewIterator function.
type IteratorParams struct {
	Address        string
	Target         string
	Table          string
	KeyColumn      string
	OrderingColumn string
	Columns        []string
	BatchSize      int
	Position       sdk.Position
}

// NewIterator create a new instance of the Iterator iterator.
func NewIterator(ctx context.Context, params IteratorParams) (*Iterator, error) {
	db, err := vitessdriver.Open(params.Address, params.Target)
	if err != nil {
		return nil, fmt.Errorf("connect to vtgate: %w", err)
	}

	if err = db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping vtgate: %w", err)
	}

	iterator := &Iterator{
		db:             sqlx.NewDb(db, "vitess"),
		table:          params.Table,
		keyColumn:      params.KeyColumn,
		orderingColumn: params.OrderingColumn,
		columns:        params.Columns,
		batchSize:      params.BatchSize,
	}

	if params.Position != nil {
		position, posErr := parsePosition(params.Position)
		if posErr != nil {
			return nil, fmt.Errorf("parse position: %w", posErr)
		}

		iterator.position = position
	}

	iterator.columnTypes, err = coltypes.GetColumnTypes(ctx, iterator.db, iterator.table)
	if err != nil {
		return nil, fmt.Errorf("get table column types: %w", err)
	}

	if err = iterator.loadRows(ctx); err != nil {
		return nil, fmt.Errorf("load rows: %w", err)
	}

	return iterator, nil
}

// HasNext returns a bool indicating whether the iterator has the next record to return or not.
func (i *Iterator) HasNext(ctx context.Context) (bool, error) {
	if i.rows.Next() {
		return true, nil
	}

	if err := i.loadRows(ctx); err != nil {
		return false, fmt.Errorf("load rows: %w", err)
	}

	return false, nil
}

// Next returns the next record.
func (i *Iterator) Next(ctx context.Context) (sdk.Record, error) {
	row := make(map[string]any)
	if err := i.rows.MapScan(row); err != nil {
		return sdk.Record{}, fmt.Errorf("scan rows: %w", err)
	}

	transformedRow, err := coltypes.TransformRow(ctx, row, i.columnTypes)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("transform row column types: %w", err)
	}

	i.position = &Position{
		LastProcessedElementValue: transformedRow[i.orderingColumn],
	}

	sdkPosition, err := i.position.marshalSDKPosition()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal position: %w", err)
	}

	transformedRowBytes, err := json.Marshal(transformedRow)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal row: %w", err)
	}

	return sdk.Record{
		Position:  sdkPosition,
		CreatedAt: time.Now(),
		Key: sdk.StructuredData{
			i.keyColumn: transformedRow[i.keyColumn],
		},
		Metadata: map[string]string{
			metadataKeyTable:  i.table,
			metadataKeyAction: actionInsert,
		},
		Payload: sdk.RawData(transformedRowBytes),
	}, nil
}

// Stop closes the underlying db connection.
func (i *Iterator) Stop(ctx context.Context) error {
	if i.rows != nil {
		if err := i.rows.Close(); err != nil {
			return fmt.Errorf("close rows: %w", err)
		}
	}

	if i.db != nil {
		return i.db.Close()
	}

	return nil
}

// loadRows selects a batch of rows from a database, based on the Iterator's
// table, columns, orderingColumn, batchSize and the current position.
func (i *Iterator) loadRows(ctx context.Context) error {
	selectBuilder := sqlbuilder.NewSelectBuilder()

	if len(i.columns) > 0 {
		selectBuilder.Select(i.columns...)
	} else {
		selectBuilder.Select("*")
	}

	selectBuilder.From(i.table)

	if i.position != nil {
		selectBuilder.Where(
			selectBuilder.GreaterThan(i.orderingColumn, i.position.LastProcessedElementValue),
		)
	}

	sql, args := selectBuilder.
		OrderBy(i.orderingColumn).
		Limit(i.batchSize).
		Build()

	rows, err := i.db.QueryxContext(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("execute select query: %w", err)
	}

	i.rows = rows

	return nil
}
