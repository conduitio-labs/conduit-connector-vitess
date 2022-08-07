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
	"fmt"
	"time"

	"github.com/conduitio-labs/conduit-connector-vitess/coltypes"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	"github.com/doug-martin/goqu/v9"
	// we need the import to work with the mysql dialect.
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
)

const (
	// defaultInitialGtid is a default gtid to start with.
	defaultInitialGtid = "current"
)

// CDC is an implementation of a CDC iterator for Vitess.
type CDC struct {
	conn   *vtgateconn.VTGateConn
	reader vtgateconn.VStreamReader
	// fields contains all fields that vstream returns,
	// fields can change, for example, the field type can change,
	// and storing the fields here we can handle this.
	fields         []*query.Field
	records        chan sdk.Record
	errCh          chan error
	table          string
	keyColumn      string
	orderingColumn string
	position       *Position
}

// CDCParams is incoming params for the NewCDC function.
type CDCParams struct {
	Address        string
	Keyspace       string
	Table          string
	TabletType     string
	KeyColumn      string
	OrderingColumn string
	Columns        []string
	Position       *Position
}

// NewCDC creates new instance of the CDC.
func NewCDC(ctx context.Context, params CDCParams) (*CDC, error) {
	cdc := &CDC{
		records:        make(chan sdk.Record, defaultRecordsBufferSize),
		errCh:          make(chan error, 1),
		table:          params.Table,
		keyColumn:      params.KeyColumn,
		orderingColumn: params.OrderingColumn,
		position: &Position{
			Mode: ModeCDC,
			Gtid: defaultInitialGtid,
		},
	}

	if params.Position != nil {
		cdc.position = params.Position
	}

	if err := cdc.setupVStream(ctx, params); err != nil {
		return nil, fmt.Errorf("setup vstream: %w", err)
	}

	go cdc.listen(ctx)

	return cdc, nil
}

// HasNext returns a bool indicating whether the iterator has the next record to return or not.
func (c *CDC) HasNext(ctx context.Context) (bool, error) {
	return len(c.records) > 0, nil
}

// Next returns the next record.
func (c *CDC) Next(ctx context.Context) (sdk.Record, error) {
	select {
	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()

	case err := <-c.errCh:
		return sdk.Record{}, err

	case record := <-c.records:
		return record, nil
	}
}

// Stop closes the underlying db connection.
func (c *CDC) Stop(ctx context.Context) error {
	if c.conn != nil {
		c.conn.Close()
	}

	return nil
}

// setupVStream opens a connection to a vtgate and create a VStream reader.
// The method returns the connection, the VStream reader and a gtid.
func (c *CDC) setupVStream(ctx context.Context, params CDCParams) error {
	shardGtid := &binlogdata.ShardGtid{
		Keyspace: params.Keyspace,
		// if the Shard is empty, we'll get rows from all the available shards.
		Shard: "",
		Gtid:  c.position.Gtid,
	}

	vgtid := &binlogdata.VGtid{
		ShardGtids: []*binlogdata.ShardGtid{shardGtid},
	}

	ruleFilter, err := c.constructRuleFilter(params.Table, params.OrderingColumn, params.Columns)
	if err != nil {
		return fmt.Errorf("construct rule filter: %w", err)
	}

	filter := &binlogdata.Filter{
		Rules: []*binlogdata.Rule{{
			Match:  params.Table,
			Filter: ruleFilter,
		}},
	}

	conn, err := vtgateconn.Dial(ctx, params.Address)
	if err != nil {
		return fmt.Errorf("vtgateconn dial: %w", err)
	}
	c.conn = conn

	tabletType := topodata.TabletType(topodata.TabletType_value[params.TabletType])

	reader, err := conn.VStream(ctx, tabletType, vgtid, filter, &vtgate.VStreamFlags{
		MinimizeSkew: false,
	})
	if err != nil {
		return fmt.Errorf("create vstream reader: %w", err)
	}
	c.reader = reader

	return nil
}

// constructRuleFilter constructs an SQL query for the binlogdata.Filter.Rules.
func (c *CDC) constructRuleFilter(table, orderingColumn string, columns []string) (string, error) {
	selectDataset := goqu.Dialect("mysql").Select()

	if len(columns) > 0 {
		cols := make([]any, len(columns))
		for i := 0; i < len(columns); i++ {
			cols[i] = columns[i]
		}

		selectDataset = selectDataset.Select(cols...)
	}

	query, _, err := selectDataset.
		From(table).
		Order(goqu.C(orderingColumn).Asc()).
		ToSQL()
	if err != nil {
		return "", fmt.Errorf("construct rule filter query: %w", err)
	}

	return query, nil
}

// listen listens for VStream events.
// If the VStream encountered an error the method will send it to the errCh channel.
// All the data are sent to the records channel.
func (c *CDC) listen(ctx context.Context) {
	for {
		events, err := c.reader.Recv()
		if err != nil {
			c.errCh <- fmt.Errorf("read from vstream: %w", err)

			return
		}

		for _, event := range events {
			switch event.Type {
			case binlogdata.VEventType_VGTID:
				c.position = &Position{
					Mode: ModeCDC,
					Gtid: event.Vgtid.ShardGtids[0].Gtid,
				}

			case binlogdata.VEventType_FIELD:
				c.fields = event.FieldEvent.Fields

			case binlogdata.VEventType_ROW:
				if c.fields == nil {
					// shouldn't happen cause VEventType_FIELD always comes before VEventType_ROW.
					sdk.Logger(ctx).Warn().Msgf("i.fields is nil, skipping the row")

					continue
				}

				if err := c.processRowEvent(ctx, c.fields, event); err != nil {
					c.errCh <- fmt.Errorf("process row event: %w", err)

					return
				}

			default:
			}
		}
	}
}

// processRowEvent makes rows from the event.RowEvent.RowChanges trusted and
// constructs the resulting slice containing all needed sqltypes.Values.
func (c *CDC) processRowEvent(ctx context.Context, fields []*query.Field, event *binlogdata.VEvent) error {
	action := actionInsert

	for _, change := range event.RowEvent.RowChanges {
		var values []sqltypes.Value

		switch after, before := change.After, change.Before; {
		case after != nil && before != nil:
			action = actionUpdate
			values = sqltypes.MakeRowTrusted(fields, change.After)

		case before != nil:
			action = actionDelete
			values = sqltypes.MakeRowTrusted(fields, change.Before)

		default:
			values = sqltypes.MakeRowTrusted(fields, change.After)
		}

		transformedRow, err := coltypes.TransformRow(ctx, c.fields, values)
		if err != nil {
			return fmt.Errorf("transform value: %w", err)
		}

		// set this in order to avoid the 'same position' error,
		// as the event can have multiple rows under one gtid.
		c.position.LastProcessedElementValue = transformedRow[c.orderingColumn]

		sdkPosition, err := c.position.marshalSDKPosition()
		if err != nil {
			return fmt.Errorf("marshal position to sdk position: %w", err)
		}

		transformedRowBytes, err := json.Marshal(transformedRow)
		if err != nil {
			return fmt.Errorf("marshal row: %w", err)
		}

		c.records <- sdk.Record{
			Position: sdkPosition,
			Metadata: map[string]string{
				metadataKeyTable:  c.table,
				metadataKeyAction: action,
			},
			CreatedAt: time.Now(),
			Key: sdk.StructuredData{
				c.keyColumn: transformedRow[c.keyColumn],
			},
			Payload: sdk.RawData(transformedRowBytes),
		}
	}

	return nil
}
