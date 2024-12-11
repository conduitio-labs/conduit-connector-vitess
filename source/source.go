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

	commonsConfig "github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio-labs/conduit-connector-vitess/source/iterator"
)

//go:generate mockgen -package mock -source ./source.go -destination ./mock/source.go

// Iterator defines an Iterator interface needed for the Source.
type Iterator interface {
	HasNext(ctx context.Context) (bool, error)
	Next(ctx context.Context) (opencdc.Record, error)
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
func (s *Source) Parameters() commonsConfig.Parameters {
	return s.config.Parameters()
}

// Configure parses and initializes the config.
func (s *Source) Configure(ctx context.Context, cfgRaw commonsConfig.Config) (err error) {
	err = sdk.Util.ParseConfig(ctx, cfgRaw, &s.config, NewSource().Parameters())
	if err != nil {
		return err
	}

	err = s.config.validate()
	if err != nil {
		return fmt.Errorf("error validating configuration: %w", err)
	}

	return nil
}

// Open makes sure everything is prepared to read records.
func (s *Source) Open(ctx context.Context, sdkPosition opencdc.Position) (err error) {
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
func (s *Source) Read(ctx context.Context) (opencdc.Record, error) {
	hasNext, err := s.iterator.HasNext(ctx)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("has next: %w", err)
	}

	if !hasNext {
		return opencdc.Record{}, sdk.ErrBackoffRetry
	}

	record, err := s.iterator.Next(ctx)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("get next record: %w", err)
	}

	return record, nil
}

// Ack does nothing. We don't need acks for the Snapshot or CDC iterators.
// It just returns nil here in order to pass the acceptance tests properly.
func (s *Source) Ack(ctx context.Context, position opencdc.Position) error {
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
