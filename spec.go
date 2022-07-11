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
	}
}
