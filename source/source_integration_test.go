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

package source

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-vitess/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vitessdriver"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

const (
	queryCreateTestTable = `
	create table %s ( 
		int_column INT, 
		varchar_column VARCHAR(20), 
		tinyint_column TINYINT, 
		text_column TEXT, 
		date_column DATE, 
		smallint_column SMALLINT, 
		mediumint_column MEDIUMINT, 
		bigint_column BIGINT, 
		float_column FLOAT(10, 2), 
		double_column DOUBLE, 
		decimal_column DECIMAL(10, 2), 
		datetime_column DATETIME, 
		timestamp_column TIMESTAMP, 
		time_column TIME, 
		year_column YEAR, 
		char_column CHAR(10), 
		tinyblob_column TINYBLOB, 
		tinytext_column TINYTEXT, 
		blob_column BLOB, 
		mediumblob_column MEDIUMBLOB, 
		mediumtext_column MEDIUMTEXT, 
		longblob_column LONGBLOB, 
		longtext_column LONGTEXT, 
		enum_column ENUM( '1', '2', '3' ), 
		set_column SET( '1', '2', '3' ), 
		bool_column BOOL, 
		binary_column BINARY( 20 ), 
		varbinary_column VARBINARY( 20 ), 
		json_column JSON, 
		PRIMARY KEY(int_column)
	);`

	queryCreateTestVindex = "alter vschema on %s add vindex hash(int_column) using hash;"

	queryInsertTestData = `
	insert into %s (int_column, varchar_column, tinyint_column, text_column, date_column, smallint_column, 
		mediumint_column, bigint_column, float_column, double_column, decimal_column, 
		datetime_column, timestamp_column, time_column, year_column, char_column, tinyblob_column, 
		tinytext_column, blob_column, mediumblob_column, mediumtext_column, longblob_column, 
		longtext_column, enum_column, set_column, bool_column, binary_column, varbinary_column, 
		json_column) values
		(1, 'varchar_super', 1, 'Text_column', '2000-09-19', 23, 1897, 8437348, 2.3, 2.3, 
		12.2, '2000-02-12 12:38:56', '2000-01-01 00:00:01', '11:12', 2012, 'c', 'tinyblob', 
		'tinytext', 'blob', 'mediumblob', 'mediumtext', 'longblob', 'longtext', '1', '2', TRUE, 
		'a', 'v', '{"key1": "value1", "key2": "value2"}'),
		
		(2, 'varchar_super_2', 2, 'Text_column', '2003-09-19', 23, 1897, 8437348, 2.3, 2.3, 
		12.2, '2000-02-12 12:38:56', '2000-01-01 00:00:01', '11:12', 2012, 'c', 'tinyblob', 
		'tinytext', 'blob', 'mediumblob', 'mediumtext', 'longblob', 'longtext', '1', '2', TRUE, 
		'a', 'v', '{"key1": "value1"}'),

		(3, 'varchar_super_3', 3, 'Text_column', '2004-09-19', 23, 1897, 8437348, 2.3, 2.3, 
		12.2, '2000-02-12 12:38:56', '2000-01-01 00:00:01', '11:12', 2012, 'c', 'tinyblob', 
		'tinytext', 'blob', 'mediumblob', 'mediumtext', 'longblob', 'longtext', '1', '2', FALSE, 
		'a', 'v', null);
	`

	queryInsertOneRow = `insert into %s (int_column, text_column) values (%d, '%s');`
	queryUpdateOneRow = `update %s set text_column = '%s' where int_column = %d;`
	queryDeleteOneRow = `delete from %s where int_column = %d;`

	queryDropTestTable = `drop table %s;`
)

func TestSource_Snapshot_Success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctx := context.Background()

	tableName := "conduit_source_snapshot_integration_test_success"

	cfg := prepareConfig(t, tableName)
	cfg[ConfigKeyColumns] = "int_column,varchar_column,tinyint_column,text_column," +
		"date_column,smallint_column,mediumint_column,bigint_column,float_column," +
		"double_column,decimal_column,datetime_column,timestamp_column,time_column," +
		"year_column,char_column,tinyblob_column,tinytext_column,blob_column," +
		"mediumblob_column,mediumtext_column,longblob_column,longtext_column," +
		"enum_column,set_column,bool_column,binary_column,varbinary_column,json_column"

	err := prepareData(
		ctx, cfg[config.KeyAddress], cfg[config.KeyKeyspace], cfg[config.KeyTabletType], tableName, false,
	)
	is.NoErr(err)

	t.Cleanup(func() {
		err = clearData(ctx, cfg[config.KeyAddress], cfg[config.KeyKeyspace], cfg[config.KeyTabletType], tableName)
		is.NoErr(err)
	})

	s := new(Source)

	err = s.Configure(ctx, cfg)
	is.NoErr(err)

	// Start first time with nil position.
	err = s.Open(ctx, nil)

	is.NoErr(err)

	// Check first read.
	r, err := s.Read(ctx)
	is.NoErr(err)

	// check right converting.
	expectedRecordPayload := sdk.RawData(
		`{"bigint_column":8437348,"binary_column":"YQAAAAAAAAAAAAAAAAAAAAAAAAA=",` +
			`"blob_column":"YmxvYg==","bool_column":true,"char_column":"c",` +
			`"date_column":"2000-09-19T00:00:00Z","datetime_column":"2000-02-12T12:38:56Z",` +
			`"decimal_column":"12.20","double_column":2.3,"enum_column":"1",` +
			`"float_column":2.3,"int_column":1,"json_column":{"key1":"value1","key2":"value2"},` +
			`"longblob_column":"bG9uZ2Jsb2I=","longtext_column":"longtext",` +
			`"mediumblob_column":"bWVkaXVtYmxvYg==","mediumint_column":1897,` +
			`"mediumtext_column":"mediumtext","set_column":"2","smallint_column":23,` +
			`"text_column":"Text_column","time_column":"0000-01-01T11:12:00Z",` +
			`"timestamp_column":"2000-01-01T00:00:01Z","tinyblob_column":"dGlueWJsb2I=",` +
			`"tinyint_column":1,"tinytext_column":"tinytext","varbinary_column":"dg==",` +
			`"varchar_column":"varchar_super","year_column":2012}`,
	)

	is.Equal(r.Payload.After, expectedRecordPayload)

	err = s.Teardown(ctx)
	is.NoErr(err)
}

func TestSource_Snapshot_Continue(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctx := context.Background()

	tableName := "conduit_source_snapshot_integration_test_continue"

	cfg := prepareConfig(t, tableName)
	cfg[ConfigKeyColumns] = "int_column,text_column"

	err := prepareData(
		ctx, cfg[config.KeyAddress], cfg[config.KeyKeyspace], cfg[config.KeyTabletType], tableName, false,
	)
	is.NoErr(err)

	t.Cleanup(func() {
		err = clearData(ctx, cfg[config.KeyAddress], cfg[config.KeyKeyspace], cfg[config.KeyTabletType], tableName)
		is.NoErr(err)
	})

	s := new(Source)

	err = s.Configure(ctx, cfg)
	is.NoErr(err)

	// Start first time with nil position.
	err = s.Open(ctx, nil)
	is.NoErr(err)

	// Check first read.
	r, err := s.Read(ctx)
	is.NoErr(err)

	var wantedKey sdk.StructuredData
	wantedKey = map[string]interface{}{"int_column": int64(1)}

	is.Equal(r.Key, wantedKey)

	err = s.Teardown(ctx)
	is.NoErr(err)

	// Open from previous position.
	err = s.Open(ctx, r.Position)
	is.NoErr(err)

	r, err = s.Read(ctx)
	is.NoErr(err)

	wantedKey = map[string]interface{}{"int_column": int64(2)}

	is.Equal(r.Key, wantedKey)

	err = s.Teardown(ctx)
	is.NoErr(err)
}

func TestSource_CDC_Success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctx := context.Background()

	tableName := "conduit_source_cdc_integration_test_success"

	cfg := prepareConfig(t, tableName)
	cfg[ConfigKeyColumns] = "int_column,text_column"

	err := prepareData(
		ctx, cfg[config.KeyAddress], cfg[config.KeyKeyspace], cfg[config.KeyTabletType], tableName, true,
	)
	is.NoErr(err)

	t.Cleanup(func() {
		err = clearData(ctx, cfg[config.KeyAddress], cfg[config.KeyKeyspace], cfg[config.KeyTabletType], tableName)
		is.NoErr(err)
	})

	s := new(Source)

	err = s.Configure(ctx, cfg)
	is.NoErr(err)

	// Start first time with nil position.
	err = s.Open(ctx, nil)
	is.NoErr(err)

	// switch to CDC iterator
	_, err = s.Read(ctx)
	is.Equal(err, sdk.ErrBackoffRetry)

	err = insertRow(ctx, cfg[config.KeyAddress], cfg[config.KeyKeyspace], cfg[config.KeyTabletType], tableName, 1, "Bob")
	is.NoErr(err)

	readCtx, cancel := context.WithTimeout(ctx, time.Second*20)
	defer cancel()

	record, err := readWithRetry(readCtx, t, s, time.Second*5)
	is.NoErr(err)
	is.Equal(record.Operation, sdk.OperationCreate)
	is.Equal(record.Key.Bytes(), []byte(`{"int_column":1}`))
	is.Equal(record.Payload.After.Bytes(), []byte(`{"int_column":1,"text_column":"Bob"}`))

	err = updateRow(ctx, cfg[config.KeyAddress], cfg[config.KeyKeyspace], cfg[config.KeyTabletType], tableName, 1, "Alex")
	is.NoErr(err)

	record, err = readWithRetry(readCtx, t, s, time.Second*5)
	is.NoErr(err)
	is.Equal(record.Operation, sdk.OperationUpdate)
	is.Equal(record.Key.Bytes(), []byte(`{"int_column":1}`))
	is.Equal(record.Payload.After.Bytes(), []byte(`{"int_column":1,"text_column":"Alex"}`))

	err = deleteRow(ctx, cfg[config.KeyAddress], cfg[config.KeyKeyspace], cfg[config.KeyTabletType], tableName, 1)
	is.NoErr(err)

	record, err = readWithRetry(readCtx, t, s, time.Second*5)
	is.NoErr(err)
	is.Equal(record.Operation, sdk.OperationDelete)
	is.Equal(record.Key.Bytes(), []byte(`{"int_column":1}`))

	err = s.Teardown(ctx)
	is.NoErr(err)

	err = insertRow(ctx, cfg[config.KeyAddress], cfg[config.KeyKeyspace], cfg[config.KeyTabletType], tableName, 2, "Ben")
	is.NoErr(err)

	// Open from the previous position.
	err = s.Open(ctx, record.Position)
	is.NoErr(err)

	record, err = readWithRetry(readCtx, t, s, time.Second*5)
	is.NoErr(err)
	is.Equal(record.Operation, sdk.OperationCreate)
	is.Equal(record.Key.Bytes(), []byte(`{"int_column":2}`))
	is.Equal(record.Payload.After.Bytes(), []byte(`{"int_column":2,"text_column":"Ben"}`))

	err = s.Teardown(ctx)
	is.NoErr(err)
}

func TestSource_Snapshot_Empty_Table(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	tableName := "conduit_source_snapshot_integration_test_empty_table"

	cfg := prepareConfig(t, tableName)
	cfg[ConfigKeyColumns] = "int_column,text_column"

	ctx := context.Background()

	err := prepareData(
		ctx, cfg[config.KeyAddress], cfg[config.KeyKeyspace], cfg[config.KeyTabletType], tableName, true,
	)
	is.NoErr(err)

	t.Cleanup(func() {
		err = clearData(ctx, cfg[config.KeyAddress], cfg[config.KeyKeyspace], cfg[config.KeyTabletType], tableName)
		is.NoErr(err)
	})

	s := new(Source)

	err = s.Configure(ctx, cfg)
	is.NoErr(err)

	// Start first time with nil position.
	err = s.Open(ctx, nil)
	is.NoErr(err)

	// Check read from empty table.
	_, err = s.Read(ctx)
	is.Equal(err, sdk.ErrBackoffRetry)

	err = s.Teardown(ctx)
	is.NoErr(err)
}

func TestSource_CDC_Empty_Table(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	tableName := "conduit_source_cdc_integration_test_empty_table"

	cfg := prepareConfig(t, tableName)

	ctx := context.Background()

	err := prepareData(
		ctx, cfg[config.KeyAddress], cfg[config.KeyKeyspace], cfg[config.KeyTabletType], tableName, true,
	)
	is.NoErr(err)

	t.Cleanup(func() {
		err = clearData(ctx, cfg[config.KeyAddress], cfg[config.KeyKeyspace], cfg[config.KeyTabletType], tableName)
		is.NoErr(err)
	})

	s := new(Source)

	err = s.Configure(ctx, cfg)
	is.NoErr(err)

	// Start first time with non nil CDC position.
	err = s.Open(ctx, sdk.Position(
		[]byte(`
		{
			"mode": "cdc",
			"keyspace": "test",
			"gtid": "current",
			"shard_gtids": [
				{"keyspace": "test", "shard": "-40", "gtid": "current"},
				{"keyspace": "test", "shard": "40-80", "gtid": "current"},
				{"keyspace": "test", "shard": "80-c0", "gtid": "current"},
				{"keyspace": "test", "shard": "c0-", "gtid": "current"}
			]
		}`),
	))
	is.NoErr(err)

	// Check read from empty table.
	_, err = s.Read(ctx)
	is.Equal(err, sdk.ErrBackoffRetry)

	err = s.Teardown(ctx)
	is.NoErr(err)
}

func prepareConfig(t *testing.T, tableName string) map[string]string {
	address := os.Getenv("VITESS_ADDRESS")
	if address == "" {
		t.Skip("VITESS_ADDRESS env var must be set")

		return nil
	}

	return map[string]string{
		config.KeyAddress:       address,
		config.KeyTable:         tableName,
		config.KeyKeyColumn:     "int_column",
		config.KeyKeyspace:      "test",
		config.KeyTabletType:    "primary",
		ConfigKeyOrderingColumn: "int_column",
	}
}

func prepareData(ctx context.Context, address, keyspace, tabletType, tableName string, empty bool) error {
	conn, err := vtgateconn.DialProtocol(ctx, *vtgateconn.VtgateProtocol, address)
	if err != nil {
		return err
	}
	defer conn.Close()

	target := strings.Join([]string{keyspace, tabletType}, "@")
	session := conn.Session(target, &query.ExecuteOptions{
		IncludedFields: query.ExecuteOptions_ALL,
	})

	_, err = session.ExecuteBatch(ctx, []string{
		fmt.Sprintf(queryCreateTestTable, tableName),
		fmt.Sprintf(queryCreateTestVindex, tableName),
	}, nil)
	if err != nil {
		return err
	}

	if !empty {
		_, err = session.Execute(ctx, fmt.Sprintf(queryInsertTestData, tableName), nil)
		if err != nil {
			return err
		}
	}

	return nil
}

func insertRow(
	ctx context.Context, address, keyspace, tabletType, tableName string, intColumn int, textColumn string,
) error {
	conn, err := vtgateconn.DialProtocol(ctx, *vtgateconn.VtgateProtocol, address)
	if err != nil {
		return err
	}
	defer conn.Close()

	target := strings.Join([]string{keyspace, tabletType}, "@")
	session := conn.Session(target, &query.ExecuteOptions{
		IncludedFields: query.ExecuteOptions_ALL,
	})

	_, err = session.Execute(ctx, fmt.Sprintf(queryInsertOneRow, tableName, intColumn, textColumn), nil)
	if err != nil {
		return err
	}

	return nil
}

func updateRow(
	ctx context.Context, address, keyspace, tabletType, tableName string, intColumn int, textColumn string,
) error {
	conn, err := vtgateconn.DialProtocol(ctx, *vtgateconn.VtgateProtocol, address)
	if err != nil {
		return err
	}
	defer conn.Close()

	target := strings.Join([]string{keyspace, tabletType}, "@")
	session := conn.Session(target, &query.ExecuteOptions{
		IncludedFields: query.ExecuteOptions_ALL,
	})

	_, err = session.Execute(ctx, fmt.Sprintf(queryUpdateOneRow, tableName, textColumn, intColumn), nil)
	if err != nil {
		return err
	}

	return nil
}

func deleteRow(
	ctx context.Context, address, keyspace, tabletType, tableName string, intColumn int,
) error {
	conn, err := vtgateconn.DialProtocol(ctx, *vtgateconn.VtgateProtocol, address)
	if err != nil {
		return err
	}
	defer conn.Close()

	target := strings.Join([]string{keyspace, tabletType}, "@")
	session := conn.Session(target, &query.ExecuteOptions{
		IncludedFields: query.ExecuteOptions_ALL,
	})

	_, err = session.Execute(ctx, fmt.Sprintf(queryDeleteOneRow, tableName, intColumn), nil)
	if err != nil {
		return err
	}

	return nil
}

func clearData(ctx context.Context, address, keyspace, tabletType, tableName string) error {
	target := strings.Join([]string{keyspace, tabletType}, "@")
	db, err := vitessdriver.Open(address, target)
	if err != nil {
		return err
	}

	defer db.Close()

	err = db.PingContext(ctx)
	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf(queryDropTestTable, tableName))
	if err != nil {
		return err
	}

	return nil
}

func readWithRetry(ctx context.Context, t *testing.T, source sdk.Source, duration time.Duration) (sdk.Record, error) {
	for {
		record, err := source.Read(ctx)
		if errors.Is(err, sdk.ErrBackoffRetry) {
			t.Logf("source returned backoff retry error, backing off for %v", duration)
			select {
			case <-ctx.Done():
				return sdk.Record{}, ctx.Err()
			case <-time.After(duration):
				continue
			}
		}

		return record, err
	}
}
