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
	"fmt"
	"strconv"
	"strings"

	"github.com/conduitio-labs/conduit-connector-vitess/config"
	"github.com/conduitio-labs/conduit-connector-vitess/validator"
)

const (
	// ConfigKeyOrderingColumn is a config name for an ordering column.
	ConfigKeyOrderingColumn = "orderingColumn"
	// ConfigKeyKeyColumn is a config name for an key column.
	ConfigKeyKeyColumn = "keyColumn"
	// ConfigKeyColumns is a config name for columns.
	ConfigKeyColumns = "columns"
	// ConfigKeyBatchSize is a config name for a batch size.
	ConfigKeyBatchSize = "batchSize"

	// defaultBatchSize is a default value for a BatchSize field.
	defaultBatchSize = 1000
)

// Config holds source specific configurable values.
type Config struct {
	config.Config

	// OrderingColumn is a name of a column that the connector will use for ordering rows.
	OrderingColumn string `key:"orderingColumn" validate:"required,max=64"`
	// KeyColumn is a column name that records should use for their Key fields.
	// Max length is 64, see [MySQL Identifier Length Limits].
	//
	// [MySQL Identifier Length Limits]: https://dev.mysql.com/doc/refman/8.0/en/identifier-length.html
	KeyColumn string `key:"keyColumn" validate:"max=64"`
	// Columns is a comma separated list of column names that should be included in each Record's payload.
	Columns []string `key:"columns" validate:"contains_or_default=OrderingColumn,dive,max=64"`
	// BatchSize is a size of rows batch.
	BatchSize int `key:"batchSize" validate:"gte=1,lte=100000"`
}

// ParseConfig maps the incoming map to the Config and validates it.
func ParseConfig(cfg map[string]string) (Config, error) {
	common, err := config.Parse(cfg)
	if err != nil {
		return Config{}, fmt.Errorf("parse common config: %w", err)
	}

	sourceConfig := Config{
		Config:         common,
		OrderingColumn: strings.ToLower(cfg[ConfigKeyOrderingColumn]),
		KeyColumn:      strings.ToLower(cfg[ConfigKeyKeyColumn]),
		BatchSize:      defaultBatchSize,
	}

	if columns := cfg[ConfigKeyColumns]; columns != "" {
		sourceConfig.Columns = strings.Split(columns, ",")
	}

	if batchSize := cfg[ConfigKeyBatchSize]; batchSize != "" {
		sourceConfig.BatchSize, err = strconv.Atoi(batchSize)
		if err != nil {
			return Config{}, fmt.Errorf("parse batchSize: %w", err)
		}
	}

	if err := validator.ValidateStruct(&sourceConfig); err != nil {
		return Config{}, fmt.Errorf("validate source config: %w", err)
	}

	return sourceConfig, nil
}
