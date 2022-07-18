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
)

// Iterator defines an Iterator interface needed for the Source.
type Iterator interface {
	HasNext(ctx context.Context) bool
	Next(ctx context.Context) (sdk.Record, error)
	Stop() error
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
func (s *Source) Open(ctx context.Context, position sdk.Position) error {
	return nil
}

// Read fetches a record from an iterator.
// If there's no record will return sdk.ErrBackoffRetry.
func (s *Source) Read(context.Context) (sdk.Record, error) {
	return sdk.Record{}, nil
}

// Teardown closes connections, stops iterator.
func (s *Source) Teardown(context.Context) error {
	return nil
}
