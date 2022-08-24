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

// Package vitess implements Vitess connector for Conduit.
// It provides both, a source and a destination Vitess connector.

package vitess

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/conduitio-labs/conduit-connector-vitess/config"
	"github.com/conduitio-labs/conduit-connector-vitess/source"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"go.uber.org/goleak"
	"vitess.io/vitess/go/vt/vitessdriver"
)

var (
	queryCreateTestTable  = `create table %s (id int, name text, primary key(id));`
	queryCreateTestVindex = `alter vschema on %s add vindex hash(id) using hash;`
	queryDropTestTable    = `drop table %s;`
)

type driver struct {
	sdk.ConfigurableAcceptanceTestDriver

	counter int64
}

// GenerateRecord generates a random sdk.Record.
func (d *driver) GenerateRecord(t *testing.T, operation sdk.Operation) sdk.Record {
	atomic.AddInt64(&d.counter, 1)

	return sdk.Record{
		Position:  nil,
		Operation: operation,
		Metadata: map[string]string{
			config.KeyTable: d.Config.DestinationConfig[config.KeyTable],
		},
		Key: sdk.StructuredData{
			"id": d.counter,
		},
		Payload: sdk.Change{
			After: sdk.RawData(
				fmt.Sprintf(
					`{"id":%d,"name":"%s"}`, d.counter, gofakeit.Name(),
				),
			),
		},
	}
}

//nolint:paralleltest // we don't need paralleltest for the Acceptance tests.
func TestAcceptance(t *testing.T) {
	cfg := prepareConfig(t)

	sdk.AcceptanceTest(t, &driver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector:         Connector,
				SourceConfig:      cfg,
				DestinationConfig: cfg,
				BeforeTest:        beforeTest(t, cfg),
				GoleakOptions: []goleak.Option{
					// this leak spawn Vitess libraries, there's no way to stop it manually
					goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
				},
			},
		},
	})
}

// beforeTest creates new table before each test.
func beforeTest(t *testing.T, cfg map[string]string) func(t *testing.T) {
	return func(t *testing.T) {
		is := is.New(t)

		table := randomIdentifier(t)
		t.Logf("table under test: %v", table)

		cfg[config.KeyTable] = table

		err := prepareData(t, cfg)
		is.NoErr(err)
	}
}

func prepareConfig(t *testing.T) map[string]string {
	address := os.Getenv("VITESS_ADDRESS")
	if address == "" {
		t.Skip("VITESS_ADDRESS env var must be set")

		return nil
	}

	return map[string]string{
		config.KeyAddress:              address,
		config.KeyKeyColumn:            "id",
		config.KeyKeyspace:             "test",
		config.KeyTabletType:           "primary",
		source.ConfigKeyOrderingColumn: "id",
		source.ConfigKeyColumns:        "id,name",
	}
}

func prepareData(t *testing.T, cfg map[string]string) error {
	target := strings.Join([]string{cfg[config.KeyKeyspace], cfg[config.KeyTabletType]}, "@")
	db, err := vitessdriver.Open(cfg[config.KeyAddress], target)
	if err != nil {
		return err
	}

	err = db.PingContext(context.Background())
	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf(queryCreateTestTable, cfg[config.KeyTable]))
	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf(queryCreateTestVindex, cfg[config.KeyTable]))
	if err != nil {
		return err
	}

	if err = db.Close(); err != nil {
		return err
	}

	// drop table
	t.Cleanup(func() {
		db, err = vitessdriver.Open(cfg[config.KeyAddress], target)
		if err != nil {
			t.Errorf("open vitess connection: %v", err)
		}

		queryDropTable := fmt.Sprintf(queryDropTestTable, cfg[config.KeyTable])

		_, err = db.Exec(queryDropTable)
		if err != nil {
			t.Errorf("drop test table: %v", err)
		}

		if err = db.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	})

	return nil
}

func randomIdentifier(t *testing.T) string {
	return fmt.Sprintf("%v_%d",
		strings.ReplaceAll(strings.ToLower(t.Name()), "/", "_"),
		time.Now().UnixMicro()%1000)
}
