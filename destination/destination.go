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
	"net"
	"strings"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"google.golang.org/grpc"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vitessdriver"

	"github.com/conduitio-labs/conduit-connector-vitess/config"
	"github.com/conduitio-labs/conduit-connector-vitess/destination/writer"
	"github.com/conduitio-labs/conduit-connector-vitess/retry"
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
	config Config
}

// NewDestination creates new instance of the Destination.
func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

// Parameters is a map of named Parameters that describe how to configure the Destination.
func (d *Destination) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		config.KeyAddress: {
			Default:     "",
			Required:    true,
			Description: "An address pointed to a VTGate instance.",
		},
		config.KeyTable: {
			Default:     "",
			Required:    true,
			Description: "A name of the table that the connector should write to.",
		},
		ConfigKeyKeyColumn: {
			Default:  "",
			Required: true,
			Description: "A column name that used to detect if the target table" +
				" already contains the record.",
		},
		config.KeyKeyspace: {
			Default:     "",
			Required:    true,
			Description: "Specifies the VTGate keyspace.",
		},
		config.KeyUsername: {
			Default:     "",
			Required:    false,
			Description: "A username of a VTGate user.",
		},
		config.KeyPassword: {
			Default:     "",
			Required:    false,
			Description: "A password of a VTGate user.",
		},
		config.KeyTabletType: {
			Default:     "primary",
			Required:    false,
			Description: "Specified the VTGate tablet type.",
		},
		config.KeyRetries: {
			Default:     "3",
			Required:    false,
			Description: "Specifies the grpc retries to vitess",
		},
	}
}

// Configure parses and initializes the config.
func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	configuration, err := ParseConfig(cfg)
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	d.config = configuration

	return nil
}

// Open makes sure everything is prepared to receive records.
func (d *Destination) Open(ctx context.Context) error {
	configuration := vitessdriver.Configuration{
		Address: d.config.Address,
		Target:  strings.Join([]string{d.config.Keyspace, d.config.TabletType}, "@"),
		GRPCDialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
				return retry.DialWithAttempts(ctx, d.config.Retries, address)
			}),
			grpc.FailOnNonTempDialError(true),
			grpc.WithBlock(),
		},
	}

	if d.config.Username != "" && d.config.Password != "" {
		configuration.GRPCDialOptions = append(configuration.GRPCDialOptions,
			grpc.WithPerRPCCredentials(
				&grpcclient.StaticAuthClientCreds{
					Username: d.config.Username,
					Password: d.config.Password,
				},
			),
		)
	}

	db, err := vitessdriver.OpenWithConfiguration(configuration)
	if err != nil {
		return fmt.Errorf("connect to vtgate: %w", err)
	}

	if err = db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping vtgate: %w", err)
	}

	d.writer, err = writer.NewWriter(ctx, writer.Params{
		DB:        db,
		Table:     d.config.Table,
		KeyColumn: d.config.KeyColumn,
	})
	if err != nil {
		return fmt.Errorf("init writer: %w", err)
	}

	return nil
}

// Write writes a record into a Destination.
func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	for i, record := range records {
		if err := d.writer.InsertRecord(ctx, record); err != nil {
			return i, fmt.Errorf("insert record: %w", err)
		}
	}

	return len(records), nil
}

// Teardown gracefully closes connections.
func (d *Destination) Teardown(ctx context.Context) error {
	if d.writer != nil {
		return d.writer.Close(ctx)
	}

	return nil
}
