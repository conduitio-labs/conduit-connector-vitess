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
	"github.com/conduitio-labs/conduit-connector-vitess/destination"
	"github.com/conduitio-labs/conduit-connector-vitess/source"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"go.uber.org/goleak"
	"vitess.io/vitess/go/vt/vitessdriver"
)

var (
	queryCreateTestTable  = `create table %s (id int, name text, primary key(id));`
	queryCreateTestVindex = `alter vschema on %s add vindex hash(id) using hash;`
	queryDropTestTable    = `drop table if exists %s;`
)

type driver struct {
	sdk.ConfigurableAcceptanceTestDriver

	idCounter int64
}

// GenerateRecord generates a random opencdc.Record.
func (d *driver) GenerateRecord(_ *testing.T, operation opencdc.Operation) opencdc.Record {
	atomic.AddInt64(&d.idCounter, 1)

	return opencdc.Record{
		Position:  nil,
		Operation: operation,
		Metadata: map[string]string{
			destination.ConfigTable: d.Config.DestinationConfig[destination.ConfigTable],
		},
		Key: opencdc.StructuredData{
			"id": d.idCounter,
		},
		Payload: opencdc.Change{
			After: opencdc.StructuredData{
				"id":   d.idCounter,
				"name": gofakeit.Name(),
			},
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
				BeforeTest:        beforeTest(cfg),
				AfterTest:         afterTest(cfg),
				GoleakOptions: []goleak.Option{
					// this leak spawn Vitess libraries, there's no way to stop it manually
					goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
					goleak.IgnoreTopFunction("google.golang.org/grpc.(*ccBalancerWrapper).watcher"),
					goleak.IgnoreTopFunction("google.golang.org/grpc/internal/transport.(*http2Client).keepalive"),
					goleak.IgnoreTopFunction("google.golang.org/grpc/internal/transport.(*controlBuffer).get"),
					goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
				},
			},
		},
	})
}

// beforeTest creates new table before each test.
func beforeTest(cfg map[string]string) func(t *testing.T) {
	return func(t *testing.T) {
		is := is.New(t)

		table := randomIdentifier(t)
		t.Logf("table under test: %v", table)

		cfg[destination.ConfigTable] = table

		err := prepareData(cfg)
		is.NoErr(err)
	}
}

// afterTest drops a test table.
func afterTest(cfg map[string]string) func(t *testing.T) {
	return func(t *testing.T) {
		target := strings.Join([]string{cfg[source.ConfigKeyspace], cfg[source.ConfigTabletType]}, "@")

		db, err := vitessdriver.Open(cfg[source.ConfigAddress], target)
		if err != nil {
			t.Errorf("open vitess connection: %v", err)
		}

		queryDropTable := fmt.Sprintf(queryDropTestTable, cfg[source.ConfigTable])

		_, err = db.Exec(queryDropTable)
		if err != nil {
			t.Errorf("drop test table: %v", err)
		}

		if err = db.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	}
}

func prepareConfig(t *testing.T) map[string]string {
	address := os.Getenv("VITESS_ADDRESS")
	if address == "" {
		t.Skip("VITESS_ADDRESS env var must be set")

		return nil
	}

	return map[string]string{
		source.ConfigAddress:        address,
		source.ConfigKeyspace:       "test",
		source.ConfigTabletType:     "primary",
		source.ConfigOrderingColumn: "id",
		source.ConfigKeyColumn:      "id",
		source.ConfigColumns:        "id,name",
	}
}

func prepareData(cfg map[string]string) error {
	target := strings.Join([]string{cfg[source.ConfigKeyspace], cfg[source.ConfigTabletType]}, "@")
	db, err := vitessdriver.Open(cfg[source.ConfigAddress], target)
	if err != nil {
		return err
	}

	err = db.PingContext(context.Background())
	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf(queryCreateTestTable, cfg[source.ConfigTable]))
	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf(queryCreateTestVindex, cfg[source.ConfigTable]))
	if err != nil {
		return err
	}

	return db.Close()
}

func randomIdentifier(t *testing.T) string {
	return fmt.Sprintf("%v_%d",
		strings.ReplaceAll(strings.ToLower(t.Name()), "/", "_"),
		time.Now().UnixMicro()%1000)
}
