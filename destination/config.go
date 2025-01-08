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
	"fmt"

	"github.com/conduitio-labs/conduit-connector-vitess/config"
)

//go:generate paramgen -output=paramgen.go Config

// Config holds destination specific configurable values.
type Config struct {
	config.Config

	// KeyColumn is a column name that is used to detect if the target table already contains the record.
	// Max length is 64, see [MySQL Identifier Length Limits].
	//
	// [MySQL Identifier Length Limits]: https://dev.mysql.com/doc/refman/8.0/en/identifier-length.html
	KeyColumn string `json:"keyColumn" validate:"required"`
}

// Validate executes manual validations beyond what is defined in struct tags.
func (c *Config) validate() error {
	if len(c.KeyColumn) > 64 {
		return fmt.Errorf("%s value must be less than or equal to %d", ConfigKeyColumn, 64)
	}

	return c.Config.Validate() //nolint:wrapcheck // not needed here
}
