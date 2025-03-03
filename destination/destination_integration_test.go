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

package destination

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vitessdriver"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

const (
	testTableNameFormat = "conduit_destination_integration_test_%d"
	queryCreateTable    = "create table %s (customer_id bigint, email varchar(128), primary key(customer_id))"
	queryCreateVindex   = "alter vschema on %s add vindex hash(customer_id) using hash;"
	queryDropTable      = "drop table if exists %s"
	querySelectEmail    = "select email from %s where customer_id = %d;"
	grpcProtocol        = "grpc"
)

//nolint:paralleltest // test cannot be parallelized because all tests use the same test table
func TestDestination_Write_Success_Insert(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	d := new(Destination)

	cfg := prepareConfig()

	err := prepareData(ctx, cfg)
	is.NoErr(err)

	t.Cleanup(func() {
		err = clearData(ctx,
			cfg[ConfigAddress], cfg[ConfigKeyspace], cfg[ConfigTabletType], cfg[ConfigTable],
		)
		is.NoErr(err)
	})

	err = d.Configure(ctx, cfg)
	is.NoErr(err)

	err = d.Open(ctx)
	is.NoErr(err)

	var written int
	written, err = d.Write(ctx, []opencdc.Record{
		{
			Payload: opencdc.Change{
				After: opencdc.StructuredData{
					"customer_id": 1,
					"email":       "example@gmail.com",
				},
			},
		},
	})
	is.NoErr(err)
	is.Equal(written, 1)

	err = d.Teardown(ctx)
	is.NoErr(err)
}

//nolint:paralleltest // test cannot be parallelized because all tests use the same test table
func TestDestination_Write_Success_Update(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	d := new(Destination)

	cfg := prepareConfig()

	err := prepareData(ctx, cfg)
	is.NoErr(err)

	db, err := getTestConnection(ctx, cfg[ConfigAddress], cfg[ConfigKeyspace], cfg[ConfigTabletType])
	is.NoErr(err)

	t.Cleanup(func() {
		err = clearData(ctx,
			cfg[ConfigAddress], cfg[ConfigKeyspace], cfg[ConfigTabletType], cfg[ConfigTable],
		)
		is.NoErr(err)

		db.Close()
	})

	err = d.Configure(ctx, cfg)
	is.NoErr(err)

	err = d.Open(ctx)
	is.NoErr(err)

	var written int
	written, err = d.Write(ctx, []opencdc.Record{
		{
			Operation: opencdc.OperationUpdate,
			Key: opencdc.StructuredData{
				"customer_id": 1,
			},
			Payload: opencdc.Change{
				After: opencdc.StructuredData{
					"email": "new@gmail.com",
				},
			},
		},
	})
	is.NoErr(err)
	is.Equal(written, 1)

	row := db.QueryRowContext(context.Background(),
		fmt.Sprintf(querySelectEmail, cfg[ConfigTable], 1),
	)

	var email string
	err = row.Scan(&email)
	is.NoErr(err)

	is.Equal(email, "new@gmail.com")

	err = d.Teardown(ctx)
	is.NoErr(err)
}

//nolint:paralleltest // test cannot be parallelized because all tests use the same test table
func TestDestination_Write_Success_UpdateKeyWithinPayload(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	d := new(Destination)

	cfg := prepareConfig()

	err := prepareData(ctx, cfg)
	is.NoErr(err)

	db, err := getTestConnection(ctx, cfg[ConfigAddress], cfg[ConfigKeyspace], cfg[ConfigTabletType])
	is.NoErr(err)

	t.Cleanup(func() {
		err = clearData(ctx,
			cfg[ConfigAddress], cfg[ConfigKeyspace], cfg[ConfigTabletType], cfg[ConfigTable],
		)
		is.NoErr(err)

		db.Close()
	})

	err = d.Configure(ctx, cfg)
	is.NoErr(err)

	err = d.Open(ctx)
	is.NoErr(err)

	var written int
	written, err = d.Write(ctx, []opencdc.Record{
		{
			Operation: opencdc.OperationUpdate,
			Payload: opencdc.Change{
				After: opencdc.StructuredData{
					"customer_id": 1,
					"email":       "haha@gmail.com",
				},
			},
		},
	})
	is.NoErr(err)
	is.Equal(written, 1)

	row := db.QueryRowContext(context.Background(),
		fmt.Sprintf(querySelectEmail, cfg[ConfigTable], 1),
	)

	var email string
	err = row.Scan(&email)
	is.NoErr(err)

	is.Equal(email, "haha@gmail.com")

	err = d.Teardown(ctx)
	is.NoErr(err)
}

//nolint:paralleltest // test cannot be parallelized because all tests use the same test table
func TestDestination_Write_Success_Delete(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	d := new(Destination)

	cfg := prepareConfig()

	err := prepareData(ctx, cfg)
	is.NoErr(err)

	db, err := getTestConnection(ctx, cfg[ConfigAddress], cfg[ConfigKeyspace], cfg[ConfigTabletType])
	is.NoErr(err)

	t.Cleanup(func() {
		err = clearData(ctx,
			cfg[ConfigAddress], cfg[ConfigKeyspace], cfg[ConfigTabletType], cfg[ConfigTable],
		)
		is.NoErr(err)

		db.Close()
	})

	err = d.Configure(ctx, cfg)
	is.NoErr(err)

	err = d.Open(ctx)
	is.NoErr(err)

	var written int
	written, err = d.Write(ctx, []opencdc.Record{
		{
			Operation: opencdc.OperationDelete,
			Key: opencdc.StructuredData{
				"customer_id": 1,
			},
		},
	})
	is.NoErr(err)
	is.Equal(written, 1)

	row := db.QueryRowContext(context.Background(),
		fmt.Sprintf(querySelectEmail, cfg[ConfigTable], 1),
	)

	err = row.Scan()
	is.Equal(err, sql.ErrNoRows)

	err = d.Teardown(ctx)
	is.NoErr(err)
}

//nolint:paralleltest // test cannot be parallelized because all tests use the same test table
func TestDestination_Write_FailNonExistentColumn(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	d := new(Destination)

	cfg := prepareConfig()

	err := prepareData(ctx, cfg)
	is.NoErr(err)

	t.Cleanup(func() {
		err = clearData(ctx,
			cfg[ConfigAddress], cfg[ConfigKeyspace], cfg[ConfigTabletType], cfg[ConfigTable],
		)
		is.NoErr(err)
	})

	err = d.Configure(ctx, cfg)
	is.NoErr(err)

	err = d.Open(ctx)
	is.NoErr(err)

	var written int
	written, err = d.Write(ctx, []opencdc.Record{
		{
			Key: opencdc.StructuredData{
				"customer_id": 1,
			},
			Payload: opencdc.Change{
				After: opencdc.StructuredData{
					// non-existent column "name"
					"name":  "bob",
					"email": "hi@gmail.com",
				},
			},
		},
	})
	is.Equal(err != nil, true)
	is.Equal(written, 0)

	err = d.Teardown(ctx)
	is.NoErr(err)
}

// prepareConfig prepares a test config.
func prepareConfig() map[string]string {
	return map[string]string{
		ConfigAddress:    "localhost:33575",
		ConfigTable:      generateTableName(),
		ConfigKeyColumn:  "customer_id",
		ConfigKeyspace:   "test",
		ConfigTabletType: "primary",
	}
}

// prepareData connects to a test vtgate instance, and creates a test table.
func prepareData(ctx context.Context, cfg map[string]string) error {
	conn, err := vtgateconn.DialProtocol(ctx, grpcProtocol, cfg[ConfigAddress])
	if err != nil {
		return fmt.Errorf("dial protocol: %w", err)
	}
	defer conn.Close()

	target := strings.Join([]string{cfg[ConfigKeyspace], cfg[ConfigTabletType]}, "@")
	session := conn.Session(target, &query.ExecuteOptions{
		IncludedFields: query.ExecuteOptions_ALL,
	})

	_, err = session.ExecuteBatch(ctx, []string{
		fmt.Sprintf(queryCreateTable, cfg[ConfigTable]),
		fmt.Sprintf(queryCreateVindex, cfg[ConfigTable]),
	}, nil)
	if err != nil {
		return fmt.Errorf("")
	}

	return nil
}

// clearData connects to a test vtgate instance and drops a test table.
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

	_, err = db.Exec(fmt.Sprintf(queryDropTable, tableName))
	if err != nil {
		return err
	}

	return nil
}

// getTestConnection returns a test connection to a test vitess database.
func getTestConnection(ctx context.Context, address, keyspace, tabletType string) (*sql.DB, error) {
	target := strings.Join([]string{keyspace, tabletType}, "@")
	db, err := vitessdriver.Open(address, target)
	if err != nil {
		return nil, err
	}

	err = db.PingContext(ctx)
	if err != nil {
		return nil, err
	}

	return db, nil
}

// generateTableName generates a random table name in a format testTableNameFormat_<current_unix_time>.
func generateTableName() string {
	return fmt.Sprintf(testTableNameFormat,
		//nolint:gosec // random uint64 for testing purpose
		uint64(rand.Uint32())<<32+uint64(rand.Uint32()),
	)
}
