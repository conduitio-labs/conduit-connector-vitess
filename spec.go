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

package vitess

import (
	"github.com/conduitio-labs/conduit-connector-vitess/config"
	"github.com/conduitio-labs/conduit-connector-vitess/source"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Spec struct{}

// Specification returns the Plugin's Specification.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:    "vitess",
		Summary: "The Vitess source and destination plugin for Conduit, written in Go.",
		Description: "The Vitess connector is one of Conduit plugins. " +
			"It provides both, a source and a destination Vitess connector.",
		Version: "v0.1.0",
		Author:  "Meroxa, Inc.",
		SourceParams: map[string]sdk.Parameter{
			config.ConfigKeyAddress: {
				Default:     "",
				Required:    true,
				Description: "An address pointed to a VTGate instance.",
			},
			config.ConfigKeyTable: {
				Default:     "",
				Required:    true,
				Description: "A name of the table that the connector should write to.",
			},
			config.ConfigKeyKeyColumn: {
				Default:  "",
				Required: true,
				Description: "A column name that used to detect if the target table" +
					" already contains the record (destination).",
			},
			source.ConfigKeyOrderingColumn: {
				Default:     "",
				Required:    true,
				Description: "A name of a column that the connector will use for ordering rows.",
			},
			config.ConfigKeyUsername: {
				Default:     "",
				Required:    false,
				Description: "A username of a VTGate user.",
			},
			config.ConfigKeyPassword: {
				Default:     "",
				Required:    false,
				Description: "A password of a VTGate user.",
			},
			config.ConfigKeyTarget: {
				Default:     "@primary",
				Required:    false,
				Description: "Specifies the VTGate target.",
			},
			source.ConfigKeyColumns: {
				Default:     "",
				Required:    false,
				Description: "A comma separated list of column names that should be included in each Record's payload.",
			},
			source.ConfigKeyBatchSize: {
				Default:     "1000",
				Required:    false,
				Description: "A size of rows batch.",
			},
		},
		DestinationParams: map[string]sdk.Parameter{
			config.ConfigKeyAddress: {
				Default:     "",
				Required:    true,
				Description: "An address pointed to a VTGate instance.",
			},
			config.ConfigKeyTable: {
				Default:     "",
				Required:    true,
				Description: "A name of the table that the connector should write to.",
			},
			config.ConfigKeyKeyColumn: {
				Default:  "",
				Required: true,
				Description: "A column name that used to detect if the target table" +
					" already contains the record (destination).",
			},
			config.ConfigKeyUsername: {
				Default:     "",
				Required:    false,
				Description: "A username of a VTGate user.",
			},
			config.ConfigKeyPassword: {
				Default:     "",
				Required:    false,
				Description: "A password of a VTGate user.",
			},
			config.ConfigKeyTarget: {
				Default:     "@primary",
				Required:    false,
				Description: "Specifies the VTGate target.",
			},
		},
	}
}
