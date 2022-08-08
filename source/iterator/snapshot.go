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

	"github.com/conduitio-labs/conduit-connector-vitess/coltypes"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	"github.com/doug-martin/goqu/v9"
	// we need the import to work with the mysql dialect.
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
)

// Snapshot is an implementation of a Snapshot iterator for Vitess.
type Snapshot struct {
	conn    *vtgateconn.VTGateConn
	session *vtgateconn.VTGateSession
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

// SnapshotParams is an incoming params for the NewSnapshot function.
type SnapshotParams struct {
	Address        string
	Keyspace       string
	TabletType     string
	Table          string
	KeyColumn      string
	OrderingColumn string
	Columns        []string
	BatchSize      int
	Position       *Position
}

// NewSnapshot creates a new instance of the Snapshot iterator.
func NewSnapshot(ctx context.Context, params SnapshotParams) (*Snapshot, error) {
	conn, err := vtgateconn.Dial(ctx, params.Address)
	if err != nil {
		return nil, fmt.Errorf("vtgateconn dial: %w", err)
	}

	target := strings.Join([]string{params.Keyspace, params.TabletType}, "@")
	session := conn.Session(target, &query.ExecuteOptions{
		IncludedFields: query.ExecuteOptions_ALL,
	})

	snapshot := &Snapshot{
		conn:           conn,
		session:        session,
		records:        make(chan sdk.Record, defaultRecordsBufferSize),
		table:          params.Table,
		keyColumn:      params.KeyColumn,
		orderingColumn: params.OrderingColumn,
		columns:        params.Columns,
		batchSize:      params.BatchSize,
		keyspace:       params.Keyspace,
	}

	if params.Position != nil {
		snapshot.position = params.Position
	}

	if err = snapshot.loadRecords(ctx); err != nil {
		return nil, fmt.Errorf("load rows: %w", err)
	}

	return snapshot, nil
}

// HasNext returns a bool indicating whether the iterator has the next record to return or not.
func (s *Snapshot) HasNext(ctx context.Context) (bool, error) {
	if len(s.records) == 0 {
		if err := s.loadRecords(ctx); err != nil {
			return false, fmt.Errorf("load records: %w", err)
		}
	}

	return len(s.records) > 0, nil
}

// Next returns the next record.
func (s *Snapshot) Next(ctx context.Context) (sdk.Record, error) {
	select {
	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()

	case record := <-s.records:
		return record, nil
	}
}

// Stop closes the underlying db connection.
func (s *Snapshot) Stop(ctx context.Context) error {
	if s.conn != nil {
		s.conn.Close()
	}

	return nil
}

// loadRecords selects a batch of rows from a database, based on the Snapshot's
// table, columns, orderingColumn, batchSize and the current position,
// and converts them to the sdk.Record.
func (s *Snapshot) loadRecords(ctx context.Context) error {
	selectDataset := goqu.Dialect("mysql").Select()

	if len(s.columns) > 0 {
		cols := make([]any, len(s.columns))
		for i := 0; i < len(s.columns); i++ {
			cols[i] = s.columns[i]
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
func (s *Snapshot) processStreamResults(ctx context.Context, resultStream sqltypes.ResultStream) error {
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

			transformedRow, err := coltypes.TransformValuesToNative(ctx, s.fields, row)
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

			s.records <- sdk.Record{
				Position:  sdkPosition,
				CreatedAt: time.Now(),
				Key: sdk.StructuredData{
					s.keyColumn: transformedRow[s.keyColumn],
				},
				Metadata: map[string]string{
					metadataKeyTable:  s.table,
					metadataKeyAction: actionInsert,
				},
				Payload: sdk.RawData(transformedRowBytes),
			}
		}
	}
}
