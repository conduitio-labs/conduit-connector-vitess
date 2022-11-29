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
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio-labs/conduit-connector-vitess/config"
	"github.com/conduitio-labs/conduit-connector-vitess/source/iterator"
)

// Iterator defines an Iterator interface needed for the Source.
type Iterator interface {
	HasNext(ctx context.Context) (bool, error)
	Next(ctx context.Context) (sdk.Record, error)
	Stop(ctx context.Context) error
}

// Source is a Vitess source plugin.
type Source struct {
	sdk.UnimplementedSource

	iterator Iterator
	config   Config
}

// NewSource creates new instance of the Source.
func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

// Parameters is a map of named Parameters that describe how to configure the Source.
func (s *Source) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		config.KeyAddress: {
			Default:     "",
			Required:    true,
			Description: "An address pointed to a VTGate instance.",
		},
		config.KeyTable: {
			Default:     "",
			Required:    true,
			Description: "A name of the table that the connector should read from.",
		},
		config.KeyKeyspace: {
			Default:     "",
			Required:    true,
			Description: "Specifies the VTGate keyspace.",
		},
		ConfigKeyOrderingColumn: {
			Default:     "",
			Required:    true,
			Description: "A name of a column that the connector will use for ordering rows.",
		},
		ConfigKeyKeyColumn: {
			Default:     "primary key of a table or value of the orderingColumn",
			Required:    false,
			Description: "Column name that records should use for their Key fields.",
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
		ConfigKeyColumns: {
			Default:     "all columns",
			Required:    false,
			Description: "A comma separated list of column names that should be included in each Record's payload.",
		},
		ConfigKeyBatchSize: {
			Default:     "1000",
			Required:    false,
			Description: "A size of rows batch.",
		},
		config.KeyMaxRetries: {
			Default:     "3",
			Required:    false,
			Description: "The number of reconnect retries the connector will make before giving up if a connection goes down.",
		},
		config.KeyRetryTimeout: {
			Default:     "1",
			Required:    false,
			Description: "The number of seconds that will be waited between retries.",
		},
		ConfigKeySnapshot: {
			Default:  "true",
			Required: false,
			Description: "The field determines whether or not the connector " +
				"will take a snapshot of the entire collection before starting CDC mode.",
		},
	}
}

// Configure parses and initializes the config.
func (s *Source) Configure(ctx context.Context, cfgRaw map[string]string) (err error) {
	s.config, err = ParseConfig(cfgRaw)
	if err != nil {
		return fmt.Errorf("parse source config: %w", err)
	}

	return nil
}

// Open makes sure everything is prepared to read records.
func (s *Source) Open(ctx context.Context, sdkPosition sdk.Position) (err error) {
	var position *iterator.Position
	if sdkPosition != nil {
		position, err = iterator.ParsePosition(sdkPosition)
		if err != nil {
			return fmt.Errorf("parse position: %w", err)
		}
	}

	s.iterator, err = iterator.NewCombined(ctx, iterator.CombinedParams{
		Address:        s.config.Address,
		Table:          s.config.Table,
		KeyColumn:      s.config.KeyColumn,
		Keyspace:       s.config.Keyspace,
		TabletType:     s.config.TabletType,
		OrderingColumn: s.config.OrderingColumn,
		Columns:        s.config.Columns,
		BatchSize:      s.config.BatchSize,
		MaxRetries:     s.config.MaxRetries,
		RetryTimeout:   s.config.RetryTimeout,
		Snapshot:       s.config.Snapshot,
		Username:       s.config.Username,
		Password:       s.config.Password,
		Position:       position,
	})
	if err != nil {
		return fmt.Errorf("init combined iterator: %w", err)
	}

	return nil
}

// Read fetches a record from an iterator.
// If there's no record will return sdk.ErrBackoffRetry.
func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	hasNext, err := s.iterator.HasNext(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("has next: %w", err)
	}

	if !hasNext {
		return sdk.Record{}, sdk.ErrBackoffRetry
	}

	record, err := s.iterator.Next(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("get next record: %w", err)
	}

	return record, nil
}

// Ack does nothing. We don't need acks for the Snapshot or CDC iterators.
// It just returns nil here in order to pass the acceptance tests properly.
func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	sdk.Logger(ctx).Debug().Str("position", string(position)).Msg("got ack")

	return nil
}

// Teardown closes connections, stops iterator.
func (s *Source) Teardown(ctx context.Context) error {
	if s.iterator != nil {
		return s.iterator.Stop(ctx)
	}

	return nil
}
