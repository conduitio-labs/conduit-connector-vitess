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
	"fmt"

	"github.com/conduitio-labs/conduit-connector-vitess/config"
	"github.com/conduitio-labs/conduit-connector-vitess/destination/writer"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"vitess.io/vitess/go/vt/vitessdriver"
)

// Writer defines a writer interface needed for the Destination.
type Writer interface {
	InsertRecord(ctx context.Context, record sdk.Record) error
	Close(ctx context.Context) error
}

// Destination Vitess Connector persists records to a MySQL database via VTgate instance.
type Destination struct {
	sdk.UnimplementedDestination

	writer Writer
	config config.Config
}

// NewDestination creates new instance of the Destination.
func NewDestination() sdk.Destination {
	return &Destination{}
}

// Configure parses and initializes the config.
func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	configuration, err := config.Parse(cfg)
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	d.config = configuration

	return nil
}

// Open makes sure everything is prepared to receive records.
func (d *Destination) Open(ctx context.Context) error {
	db, err := vitessdriver.Open(d.config.Address, d.config.Target)
	if err != nil {
		return fmt.Errorf("connect to vtgate: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping vtgate: %w", err)
	}

	d.writer = writer.NewWriter(ctx, writer.Params{
		DB:        db,
		Table:     d.config.Table,
		KeyColumn: d.config.KeyColumn,
	})

	return nil
}

// Write writes a record into a Destination.
func (d *Destination) Write(ctx context.Context, record sdk.Record) error {
	return d.writer.InsertRecord(ctx, record)
}

// Teardown gracefully closes connections.
func (d *Destination) Teardown(ctx context.Context) error {
	if d.writer != nil {
		return d.writer.Close(ctx)
	}

	return nil
}
