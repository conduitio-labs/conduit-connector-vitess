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

	"github.com/conduitio-labs/conduit-connector-vitess/columntypes"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/doug-martin/goqu/v9"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/vtctl/vtctlclient"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	// we need the goqu/v9/dialect/mysql to work with the mysql dialect.
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
	// we need the grpcvtctlclient to interact with the vtctl client via gRPC.
	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
)

const (
	// defaultInitialGtid is the default gtid to start with.
	defaultInitialGtid = "current"
	// defaultVtctlCommandTimeout is the default action timeout for any vtctl command.
	defaultVtctlCommandTimeout = time.Second * 5

	// findAllShardsInKeyspaceCommand is a name for vtctl's FindAllShardsInKeyspace command.
	findAllShardsInKeyspaceCommand = "FindAllShardsInKeyspace"
)

// cdc is an implementation of a CDC iterator for Vitess.
type cdc struct {
	reader vtgateconn.VStreamReader
	// fields contains all fields that vstream returns,
	// fields can change, for example, the field type can change,
	// and storing the fields here we can handle this.
	fields         []*query.Field
	records        chan opencdc.Record
	errCh          chan error
	stopCh         chan struct{}
	table          string
	keyColumn      string
	orderingColumn string
	position       *Position
}

// cdcParams is incoming params for the newCDC function.
type cdcParams struct {
	Conn           *vtgateconn.VTGateConn
	Address        string
	Keyspace       string
	Table          string
	TabletType     string
	KeyColumn      string
	OrderingColumn string
	Columns        []string
	Position       *Position
}

// newCDC creates new instance of the CDC.
func newCDC(ctx context.Context, params cdcParams) (*cdc, error) {
	cdc := &cdc{
		records:        make(chan opencdc.Record, defaultRecordsBufferSize),
		errCh:          make(chan error, 1),
		stopCh:         make(chan struct{}, 1),
		table:          params.Table,
		keyColumn:      params.KeyColumn,
		orderingColumn: params.OrderingColumn,
		position:       params.Position,
	}

	if cdc.position == nil {
		shardGtids, err := cdc.findAllShardsInKeyspace(ctx, params.Address, params.Keyspace)
		if err != nil {
			return nil, fmt.Errorf("find all shards in keyspace %q: %w", params.Keyspace, err)
		}

		cdc.position = &Position{
			Mode:       ModeCDC,
			Keyspace:   params.Keyspace,
			ShardGtids: shardGtids,
		}
	}

	if err := cdc.setupVStream(ctx, params); err != nil {
		return nil, fmt.Errorf("setup vstream: %w", err)
	}

	go cdc.listen(ctx)

	return cdc, nil
}

// HasNext returns a bool indicating whether the iterator has the next record to return or not.
func (c *cdc) HasNext(context.Context) (bool, error) {
	return len(c.records) > 0, nil
}

// Next returns the next record.
func (c *cdc) Next(ctx context.Context) (opencdc.Record, error) {
	select {
	case <-ctx.Done():
		return opencdc.Record{}, ctx.Err()

	case err := <-c.errCh:
		return opencdc.Record{}, err

	case record := <-c.records:
		return record, nil
	}
}

// Stop does nothing.
func (c *cdc) Stop(ctx context.Context) error {
	sdk.Logger(ctx).Debug().Msgf("stop cdc iterator")

	c.stopCh <- struct{}{}

	return nil
}

// setupVStream opens a connection to a vtgate and create a VStream reader.
// The method returns the connection, the VStream reader and a gtid.
func (c *cdc) setupVStream(ctx context.Context, params cdcParams) error {
	vgtid := &binlogdata.VGtid{
		ShardGtids: c.position.ShardGtids,
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

	tabletType := topodata.TabletType(topodata.TabletType_value[params.TabletType])

	reader, err := params.Conn.VStream(ctx, tabletType, vgtid, filter, &vtgate.VStreamFlags{
		MinimizeSkew: false,
	})
	if err != nil {
		return fmt.Errorf("create vstream reader: %w", err)
	}
	c.reader = reader

	return nil
}

// findAllShardsInKeyspace executes a vtctl's FindAllShardsInKeyspace command, parses and returns the result.
func (c *cdc) findAllShardsInKeyspace(ctx context.Context, address, keyspace string) ([]*binlogdata.ShardGtid, error) {
	vtctlClient, err := vtctlclient.New(ctx, address)
	if err != nil {
		return nil, fmt.Errorf("vtctlclient connect: %w", err)
	}
	defer vtctlClient.Close()

	// for more details,
	// see https://vitess.io/docs/14.0/reference/programs/vtctldclient/vtctldclient_findallshardsinkeyspace/
	eventStream, err := vtctlClient.ExecuteVtctlCommand(
		ctx, []string{findAllShardsInKeyspaceCommand, keyspace}, defaultVtctlCommandTimeout,
	)
	if err != nil {
		return nil, fmt.Errorf("execute FindAllShardsInKeyspace vtctl command: %w", err)
	}

	event, err := eventStream.Recv()
	if err != nil {
		return nil, fmt.Errorf("receive FindAllShardsInKeyspace vtctl command result: %w", err)
	}

	eventStr := event.GetValue()
	if eventStr == "" {
		return nil, ErrFindAllShardsInKeyspaceReturnedNothing
	}

	shardsInfo := make(map[string]any)
	if err := json.Unmarshal([]byte(eventStr), &shardsInfo); err != nil {
		return nil, fmt.Errorf("unmarshal shards info: %w", err)
	}

	shardGtids := make([]*binlogdata.ShardGtid, 0, len(shardsInfo))
	for shard := range shardsInfo {
		shardGtids = append(shardGtids, &binlogdata.ShardGtid{
			Keyspace: keyspace,
			Shard:    shard,
			Gtid:     defaultInitialGtid,
		})
	}

	return shardGtids, nil
}

// constructRuleFilter constructs an SQL query for the binlogdata.Filter.Rules.
func (c *cdc) constructRuleFilter(table, orderingColumn string, columns []string) (string, error) {
	selectDataset := goqu.Dialect("mysql").Select()

	if len(columns) > 0 {
		keyColumnPresent := false
		// add one to the capacity to have a space for the keyColumn
		// if it's not present in the columns list.
		cols := make([]any, 0, len(columns)+1)
		for i := 0; i < len(columns); i++ {
			if columns[i] == c.keyColumn {
				keyColumnPresent = true
			}

			cols = append(cols, columns[i])
		}

		if !keyColumnPresent {
			cols = append(cols, c.keyColumn)
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
func (c *cdc) listen(ctx context.Context) {
	for {
		select {
		case <-c.stopCh:
			return

		default:
			events, err := c.reader.Recv()
			if err != nil {
				c.errCh <- fmt.Errorf("read from vstream: %w", err)

				return
			}

			var rowEvent *binlogdata.VEvent

			for _, event := range events {
				switch eventType := event.Type; {
				case eventType == binlogdata.VEventType_VGTID && rowEvent != nil:
					c.position.ShardGtids = event.Vgtid.ShardGtids

					if err := c.processRowEvent(rowEvent); err != nil {
						sdk.Logger(ctx).Error().Str("err", err.Error()).Msg("process row event")
						c.errCh <- fmt.Errorf("process row event: %w", err)

						return
					}

				case eventType == binlogdata.VEventType_FIELD:
					c.fields = event.FieldEvent.Fields

				case eventType == binlogdata.VEventType_ROW:
					if c.fields == nil {
						// shouldn't happen cause VEventType_FIELD always comes before VEventType_ROW.
						sdk.Logger(ctx).Warn().Msgf("i.fields is nil, skipping the row")

						continue
					}

					rowEvent = event

				default:
				}
			}
		}
	}
}

// processRowEvent makes rows from the event.RowEvent.RowChanges trusted,
// constructs the resulting slice containing all needed sqltypes.Values,
// transforms it to a opencdc.Record and sends the record to a c.records channel.
func (c *cdc) processRowEvent(event *binlogdata.VEvent) error {
	for _, change := range event.RowEvent.RowChanges {
		var (
			valuesBefore []sqltypes.Value
			valuesAfter  []sqltypes.Value
			operation    = opencdc.OperationCreate
		)

		switch after, before := change.After, change.Before; {
		case after != nil && before != nil:
			operation = opencdc.OperationUpdate
			valuesBefore = sqltypes.MakeRowTrusted(c.fields, before)
			valuesAfter = sqltypes.MakeRowTrusted(c.fields, after)

		case before != nil:
			operation = opencdc.OperationDelete
			valuesAfter = sqltypes.MakeRowTrusted(c.fields, before)

		default:
			valuesAfter = sqltypes.MakeRowTrusted(c.fields, after)
		}

		record, err := c.transformRowsToRecord(valuesBefore, valuesAfter, operation)
		if err != nil {
			return fmt.Errorf("transform rows to record: %w", err)
		}

		c.records <- record
	}

	return nil
}

// transformRowsToRecord transforms after and before of type []sqltypes.Values to a opencdc.Record,
// based on the provided operation.
func (c *cdc) transformRowsToRecord(before, after []sqltypes.Value, operation opencdc.Operation) (opencdc.Record, error) {
	var (
		transformedRowBefore opencdc.StructuredData
		transformedRowAfter  opencdc.StructuredData
		orderingColumnValue  any
		key                  opencdc.StructuredData
		err                  error
	)

	if len(before) > 0 {
		_, _, transformedRowBefore, err = c.transformValuesToNative(before)
		if err != nil {
			return opencdc.Record{}, fmt.Errorf("transform values to native: %w", err)
		}
	}

	if len(after) > 0 {
		key, orderingColumnValue, transformedRowAfter, err = c.transformValuesToNative(after)
		if err != nil {
			return opencdc.Record{}, fmt.Errorf("transform values to native: %w", err)
		}

		// set this in order to avoid the 'same position' error,
		// as the event can have multiple rows under one gtid.
		c.position.LastProcessedElementValue = orderingColumnValue
	}

	sdkPosition, err := c.position.MarshalSDKPosition()
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("marshal position to sdk position: %w", err)
	}

	metadata := make(opencdc.Metadata)
	metadata.SetCreatedAt(time.Now())
	metadata[metadataKeyTable] = c.table

	switch operation {
	case opencdc.OperationCreate:
		return sdk.Util.Source.NewRecordCreate(sdkPosition, metadata, key, transformedRowAfter), nil

	case opencdc.OperationUpdate:
		return sdk.Util.Source.NewRecordUpdate(
			sdkPosition, metadata, key, transformedRowBefore, transformedRowAfter,
		), nil

	case opencdc.OperationDelete:
		return sdk.Util.Source.NewRecordDelete(sdkPosition, metadata, key, nil), nil

	default:
		// shouldn't happen
		return opencdc.Record{}, fmt.Errorf("unknown operation: %q", operation)
	}
}

// transformValuesToNative transforms a provided row to native values.
// The methods returns extracted value for opencdc.Record.Key, ordering column's value,
// transormed row's bytes and an error.
func (c *cdc) transformValuesToNative(row []sqltypes.Value) (opencdc.StructuredData, any, opencdc.StructuredData, error) {
	transformedRow, err := columntypes.TransformValuesToNative(c.fields, row)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("transform row value: %w", err)
	}

	return opencdc.StructuredData{
		c.keyColumn: transformedRow[c.keyColumn],
	}, transformedRow[c.orderingColumn], opencdc.StructuredData(transformedRow), nil
}
