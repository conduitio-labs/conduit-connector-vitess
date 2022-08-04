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

	"github.com/conduitio-labs/conduit-connector-vitess/source/iterator"
	sdk "github.com/conduitio/conduit-connector-sdk"
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
	return &Source{}
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

// Teardown closes connections, stops iterator.
func (s *Source) Teardown(ctx context.Context) error {
	if s.iterator != nil {
		return s.iterator.Stop(ctx)
	}

	return nil
}
