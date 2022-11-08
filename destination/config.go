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
	"strings"

	"github.com/conduitio-labs/conduit-connector-vitess/config"
	"github.com/conduitio-labs/conduit-connector-vitess/validator"
)

// ConfigKeyKeyColumn is a config name for an key column.
const ConfigKeyKeyColumn = "keyColumn"

// Config holds destination specific configurable values.
type Config struct {
	config.Config

	// KeyColumn is a column name that is used to detect if the target table already contains the record.
	// Max length is 64, see [MySQL Identifier Length Limits].
	//
	// [MySQL Identifier Length Limits]: https://dev.mysql.com/doc/refman/8.0/en/identifier-length.html
	KeyColumn string `key:"keyColumn" validate:"required,max=64"`
}

// ParseConfig maps the incoming map to the Config and validates it.
func ParseConfig(cfg map[string]string) (Config, error) {
	common, err := config.Parse(cfg)
	if err != nil {
		return Config{}, fmt.Errorf("parse common config: %w", err)
	}

	destinationConfig := Config{
		Config:    common,
		KeyColumn: strings.ToLower(cfg[ConfigKeyKeyColumn]),
	}

	if err := validator.ValidateStruct(&destinationConfig); err != nil {
		return Config{}, fmt.Errorf("validate destination config: %w", err)
	}

	return destinationConfig, nil
}
