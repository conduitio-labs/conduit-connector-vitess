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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/conduitio-labs/conduit-connector-vitess/columntypes"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/doug-martin/goqu/v9"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	// we need the import to work with the mysql dialect.
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
)

// snapshot is an implementation of a snapshot iterator for Vitess.
type snapshot struct {
	session *vtgateconn.VTGateSession
	// records is a channel that contains read and processed table rows
	// coming from streaming queries of a vtagte.
	// It's a convenient way to have a simple queue with batching.
	records chan sdk.Record
	// fields contains all fields that vstream returns,
	// fields can change, for example, the field type can change,
	// and storing the fields here we can handle this.
	fields         []*query.Field
	table          string
	keyColumn      string
	orderingColumn string
	columns        []string
	batchSize      int
	keyspace       string
	position       *Position
}

// snapshotParams is an incoming params for the newSnapshot function.
type snapshotParams struct {
	Conn           *vtgateconn.VTGateConn
	Keyspace       string
	TabletType     string
	Table          string
	KeyColumn      string
	OrderingColumn string
	Columns        []string
	BatchSize      int
	Position       *Position
}

// newSnapshot creates a new instance of the snapshot iterator.
func newSnapshot(ctx context.Context, params snapshotParams) (*snapshot, error) {
	snapshot := &snapshot{
		records:        make(chan sdk.Record, defaultRecordsBufferSize),
		table:          params.Table,
		keyColumn:      params.KeyColumn,
		orderingColumn: params.OrderingColumn,
		columns:        params.Columns,
		batchSize:      params.BatchSize,
		keyspace:       params.Keyspace,
	}

	target := strings.Join([]string{params.Keyspace, params.TabletType}, "@")
	snapshot.session = params.Conn.Session(target, &query.ExecuteOptions{
		IncludedFields: query.ExecuteOptions_ALL,
	})

	if params.Position != nil {
		snapshot.position = params.Position
	}

	return snapshot, nil
}

// hasNext returns a bool indicating whether the iterator has the next record to return or not.
func (s *snapshot) hasNext(ctx context.Context) (bool, error) {
	if len(s.records) == 0 {
		if err := s.loadRecords(ctx); err != nil {
			return false, fmt.Errorf("load records: %w", err)
		}
	}

	return len(s.records) > 0, nil
}

// next returns the next record.
func (s *snapshot) next(ctx context.Context) (sdk.Record, error) {
	select {
	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()

	case record := <-s.records:
		return record, nil
	}
}

// stop does nothing.
func (s *snapshot) stop(ctx context.Context) error {
	sdk.Logger(ctx).Debug().Msgf("stop snapshot iterator")

	return nil
}

// loadRecords selects a batch of rows from a database, based on the snapshot's
// table, columns, orderingColumn, batchSize and the current position,
// and converts them to the sdk.Record.
func (s *snapshot) loadRecords(ctx context.Context) error {
	selectDataset := goqu.Dialect("mysql").Select()

	if len(s.columns) > 0 {
		keyColumnPresent := false
		// add one to the capacity to have a space for the keyColumn
		// if it's not present in the columns list.
		cols := make([]any, 0, len(s.columns)+1)
		for i := 0; i < len(s.columns); i++ {
			if s.columns[i] == s.keyColumn {
				keyColumnPresent = true
			}

			cols = append(cols, s.columns[i])
		}

		if !keyColumnPresent {
			cols = append(cols, s.keyColumn)
		}

		selectDataset = selectDataset.Select(cols...)
	}

	selectDataset = selectDataset.
		From(s.table).
		Order(goqu.C(s.orderingColumn).Asc()).
		Limit(uint(s.batchSize))

	if s.position != nil {
		selectDataset = selectDataset.Where(
			goqu.Ex{
				s.orderingColumn: goqu.Op{"gt": s.position.LastProcessedElementValue},
			},
		)
	}

	query, _, err := selectDataset.ToSQL()
	if err != nil {
		return fmt.Errorf("construct select query: %w", err)
	}

	resultStream, err := s.session.StreamExecute(ctx, query, nil)
	if err != nil {
		return fmt.Errorf("stream execute select query: %w", err)
	}

	if err := s.processStreamResults(ctx, resultStream); err != nil {
		return fmt.Errorf("process stream results: %w", err)
	}

	return nil
}

// processStreamResults receives a result from the sqltypes.ResultStream,
// converts its rows to the sdk.Record and sends them to the i.records channel.
func (s *snapshot) processStreamResults(ctx context.Context, resultStream sqltypes.ResultStream) error {
	for {
		result, err := resultStream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return fmt.Errorf("stream recv: %w", err)
		}

		if result.Fields != nil {
			s.fields = result.Fields
		}

		for _, row := range result.Rows {
			if s.fields == nil {
				// shouldn't happen cause VEventType_FIELD always comes before VEventType_ROW.
				sdk.Logger(ctx).Warn().Msgf("i.fields is nil, skipping the row")

				continue
			}

			transformedRow, err := columntypes.TransformValuesToNative(ctx, s.fields, row)
			if err != nil {
				return fmt.Errorf("transform row: %w", err)
			}

			s.position = &Position{
				Mode:                      ModeSnapshot,
				Keyspace:                  s.keyspace,
				LastProcessedElementValue: transformedRow[s.orderingColumn],
			}

			sdkPosition, err := s.position.MarshalSDKPosition()
			if err != nil {
				return fmt.Errorf("marshal position: %w", err)
			}

			transformedRowBytes, err := json.Marshal(transformedRow)
			if err != nil {
				return fmt.Errorf("marshal row: %w", err)
			}

			metadata := make(sdk.Metadata)
			metadata.SetCreatedAt(time.Now())
			metadata[metadataKeyTable] = s.table

			s.records <- sdk.Util.Source.NewRecordSnapshot(sdkPosition, metadata, sdk.StructuredData{
				s.keyColumn: transformedRow[s.keyColumn],
			}, sdk.RawData(transformedRowBytes))
		}
	}
}
