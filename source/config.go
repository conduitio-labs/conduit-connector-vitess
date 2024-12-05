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

	"github.com/conduitio-labs/conduit-connector-vitess/config"
)

//go:generate paramgen -output=paramgen.go Config

// Config holds source specific configurable values.
type Config struct {
	config.Config

	// OrderingColumn is a name of a column that the connector will use for ordering rows.
	OrderingColumn string `json:"orderingColumn" validate:"required"`
	// KeyColumn is a column name that records should use for their Key fields.
	// Max length is 64, see [MySQL Identifier Length Limits].
	//
	// [MySQL Identifier Length Limits]: https://dev.mysql.com/doc/refman/8.0/en/identifier-length.html
	KeyColumn string `json:"keyColumn"`
	// Columns is a comma separated list of column names that should be included in each Record's payload.
	Columns []string `json:"columns"`
	// BatchSize is a size of rows batch.
	BatchSize int `json:"batchSize" validate:"gt=0,lt=100001" default:"1000"`
	// Snapshot determines whether or not the connector will take a snapshot
	// of the entire collection before starting CDC mode.
	Snapshot bool `json:"snapshot" default:"true"`
}

func (c *Config) validate() error {
	if len(c.OrderingColumn) > 64 {
		return fmt.Errorf("%s value must be less than or equal to %d", ConfigOrderingColumn, 64)
	}

	if len(c.KeyColumn) > 64 {
		return fmt.Errorf("%s value must be less than or equal to %d", ConfigKeyColumn, 64)
	}

	var containsOrderingColumn bool
	for _, col := range c.Columns {
		if len(col) > 64 {
			return fmt.Errorf("%s value must be less than or equal to %d", ConfigOrderingColumn, 64)
		}
		if col == c.OrderingColumn {
			containsOrderingColumn = true
		}
	}

	if !containsOrderingColumn && len(c.Columns) > 0 {
		return fmt.Errorf("%v value must contains values of these fields: %v", ConfigColumns, ConfigOrderingColumn)
	}

	return nil
}
