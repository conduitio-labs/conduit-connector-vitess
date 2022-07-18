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
	"testing"

	"github.com/conduitio-labs/conduit-connector-vitess/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"vitess.io/vitess/go/vt/vitessdriver"
)

const (
	testTableNameFormat = "conduit_destination_integration_test_%d"
	queryCreateTable    = "create table %s (customer_id bigint, email varchar(128), primary key(customer_id))"
	queryCreateVindex   = "alter vschema on %s add vindex hash(customer_id) using hash;"
	queryDropTable      = "drop table if exists %s"
)

func TestDestination_Write_Success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctx := context.Background()

	d := new(Destination)

	cfg, err := prepareConfig()
	is.NoErr(err)

	db, err := prepareData(ctx, cfg)
	is.NoErr(err)

	t.Cleanup(func() {
		err := clearData(ctx, db, cfg[config.ConfigKeyTable])
		is.NoErr(err)

		db.Close()
	})

	err = d.Configure(ctx, cfg)
	is.NoErr(err)

	//nolint:paralleltest // don't need paralleltest here
	t.Run("insert", func(t *testing.T) {
		err = d.Open(ctx)
		is.NoErr(err)

		err = d.Write(ctx, sdk.Record{
			Payload: sdk.StructuredData{
				"customer_id": 1,
				"email":       "example@gmail.com",
			},
		})
		is.NoErr(err)

		err = d.Teardown(ctx)
		is.NoErr(err)
	})

	//nolint:paralleltest // don't need paralleltest here
	t.Run("update", func(t *testing.T) {
		err = d.Open(ctx)
		is.NoErr(err)

		err = d.Write(ctx, sdk.Record{
			Metadata: map[string]string{
				"action": "update",
			},
			Key: sdk.StructuredData{
				"customer_id": 1,
			},
			Payload: sdk.StructuredData{
				"email": "new@gmail.com",
			},
		})
		is.NoErr(err)

		row := db.QueryRowContext(context.Background(),
			fmt.Sprintf("select email from %s where customer_id = %d;", cfg[config.ConfigKeyTable], 1),
		)

		var email string
		err = row.Scan(&email)
		is.NoErr(err)

		is.Equal(email, "new@gmail.com")

		err = d.Teardown(ctx)
		is.NoErr(err)
	})

	//nolint:paralleltest // don't need paralleltest here
	t.Run("update_key_within_payload", func(t *testing.T) {
		err = d.Open(ctx)
		is.NoErr(err)

		err = d.Write(ctx, sdk.Record{
			Metadata: map[string]string{
				"action": "update",
			},
			Payload: sdk.StructuredData{
				"customer_id": 1,
				"email":       "haha@gmail.com",
			},
		})
		is.NoErr(err)

		row := db.QueryRowContext(context.Background(),
			fmt.Sprintf("select email from %s where customer_id = %d;", cfg[config.ConfigKeyTable], 1),
		)

		var email string
		err = row.Scan(&email)
		is.NoErr(err)

		is.Equal(email, "haha@gmail.com")

		err = d.Teardown(ctx)
		is.NoErr(err)
	})

	//nolint:paralleltest // don't need paralleltest here
	t.Run("delete", func(t *testing.T) {
		err = d.Open(ctx)
		is.NoErr(err)

		err = d.Write(ctx, sdk.Record{
			Metadata: map[string]string{
				"action": "delete",
			},
			Key: sdk.StructuredData{
				"customer_id": 1,
			},
		})
		is.NoErr(err)

		row := db.QueryRowContext(context.Background(),
			fmt.Sprintf("select * from %s where customer_id = %d;", cfg[config.ConfigKeyTable], 1),
		)

		err = row.Scan()
		is.Equal(err, sql.ErrNoRows)

		err = d.Teardown(ctx)
		is.NoErr(err)
	})
}

func TestDestination_Write_Failed(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctx := context.Background()

	d := new(Destination)

	cfg, err := prepareConfig()
	is.NoErr(err)

	db, err := prepareData(ctx, cfg)
	is.NoErr(err)

	t.Cleanup(func() {
		err := clearData(ctx, db, cfg[config.ConfigKeyTable])
		is.NoErr(err)

		db.Close()
	})

	err = d.Configure(ctx, cfg)
	is.NoErr(err)

	err = d.Open(ctx)
	is.NoErr(err)

	err = d.Write(ctx, sdk.Record{
		Key: sdk.StructuredData{
			"customer_id": 1,
		},
		Payload: sdk.StructuredData{
			// non-existent column "name"
			"name":  "bob",
			"email": "hi@gmail.com",
		},
	})
	is.Equal(err != nil, true)

	err = d.Teardown(ctx)
	is.NoErr(err)
}

// prepareConfig prepares a test config.
func prepareConfig() (map[string]string, error) {
	return map[string]string{
		config.ConfigKeyAddress:   "localhost:33575",
		config.ConfigKeyTable:     generateTableName(),
		config.ConfigKeyKeyColumn: "customer_id",
		config.ConfigKeyTarget:    "test@primary",
	}, nil
}

// prepareData connects to a test vtgate instance, creates a test table and returns the *sql.DB.
func prepareData(ctx context.Context, cfg map[string]string) (*sql.DB, error) {
	db, err := vitessdriver.Open(cfg[config.ConfigKeyAddress], cfg[config.ConfigKeyTarget])
	if err != nil {
		return nil, fmt.Errorf("connector to vtgate: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping vtgate: %w", err)
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(queryCreateTable, cfg[config.ConfigKeyTable]))
	if err != nil {
		return nil, fmt.Errorf("exec create table query: %w", err)
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(queryCreateVindex, cfg[config.ConfigKeyTable]))
	if err != nil {
		return nil, fmt.Errorf("exec create vindex query: %w", err)
	}

	return db, nil
}

// clearData connects to a test vtgate instance and drops a test table.
func clearData(ctx context.Context, db *sql.DB, tableName string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf(queryDropTable, tableName))
	if err != nil {
		return fmt.Errorf("exec drop table query: %w", err)
	}

	return nil
}

// generateTableName generates a random table name in a format testTableNameFormat_<current_unix_time>.
func generateTableName() string {
	return fmt.Sprintf(testTableNameFormat,
		// nolint:gosec // random uint64 for testing purpose
		uint64(rand.Uint32())<<32+uint64(rand.Uint32()),
	)
}
