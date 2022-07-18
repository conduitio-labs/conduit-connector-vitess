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
		DestinationParams: map[string]sdk.Parameter{
			config.ConfigKeyAddress: {
				Default:     "An address pointed to a VTGate instance",
				Required:    true,
				Description: "",
			},
			config.ConfigKeyTable: {
				Default:     "A name of the table that the connector should write to.",
				Required:    true,
				Description: "",
			},
			config.ConfigKeyKeyColumn: {
				Default: "A column name that used to detect if the target table" +
					" already contains the record.",
				Required:    true,
				Description: "",
			},
			config.ConfigKeyUsername: {
				Default:     "A username of a VTGate user.",
				Required:    false,
				Description: "",
			},
			config.ConfigKeyPassword: {
				Default:     "A password of a VTGate user.",
				Required:    false,
				Description: "",
			},
			config.ConfigKeyTarget: {
				Default:     "@primary",
				Required:    false,
				Description: "Specifies the VTGate target.",
			},
		},
	}
}
