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

package iterator

import (
	"encoding/json"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Mode defines an iterator mode.
type Mode string

const (
	// IteratorTypeSnapshot represents a snapshot iterator type.
	ModeSnapshot = "snapshot"
	// IteratorTypeCDC represents a CDC iterator type.
	ModeCDC = "cdc"
)

// Position is a combined iterator's position.
type Position struct {
	Mode Mode `json:"mode"`
	// LastProcessedElementValue is a value of the element
	// at which the iterator stopped reading rows.
	// The iterator will continue reading from the element if it's not empty.
	// IteratorType: snapshot.
	LastProcessedElementValue any `json:"last_processed_element_value,omitempty"`
	// Gtid specifies a gtid to start with.
	// IteratorType: cdc.
	Gtid      string `json:"gtid,omitempty"`
	Timestamp int64  `json:"timestamp"`
}

// marshalPosition marshals the underlying position into a sdk.Position as JSON bytes.
func (p *Position) marshalSDKPosition() (sdk.Position, error) {
	positionBytes, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("marshal position: %w", err)
	}

	return sdk.Position(positionBytes), nil
}

// parsePosition converts an sdk.Position into a Position.
func ParsePosition(sdkPosition sdk.Position) (*Position, error) {
	var position Position

	if sdkPosition == nil {
		return nil, nil
	}

	if err := json.Unmarshal(sdkPosition, &position); err != nil {
		return nil, fmt.Errorf("unmarshal sdk.Position into Position: %w", err)
	}

	return &position, nil
}
