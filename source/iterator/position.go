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
	"vitess.io/vitess/go/vt/proto/binlogdata"
)

// Mode defines an iterator mode.
type Mode string

const (
	// ModeSnapshot represents a snapshot iterator mode.
	ModeSnapshot = "snapshot"
	// ModeCDC represents a CDC iterator mode.
	ModeCDC = "cdc"
)

// Position is a combined iterator's position.
type Position struct {
	Mode Mode `json:"mode"`
	// Keyspace holds a name of a VTGate keyspace that the connector interact with.
	// Mode: snapshot, cdc.
	Keyspace string `json:"keyspace"`
	// Table holds a name of a Vitess table that the connector interact with.
	// Mode: snapshot, cdc.
	Table string `json:"table"`
	// LastProcessedElementValue is a value of the element
	// at which the iterator stopped reading rows.
	// The iterator will continue reading from the element if it's not empty.
	// Mode: snapshot, cdc.
	LastProcessedElementValue any `json:"last_processed_element_value,omitempty"`
	// Gtid is the most recent gtid to start with.
	// Mode: cdc.
	Gtid string `json:"gtid,omitempty"`
	// ShardGtids holds a list of shards which the connector will read events from.
	// Mode: cdc.
	ShardGtids []*binlogdata.ShardGtid `json:"shard_gtids,omitempty"`
}

// GetBinlogShardGtids returns Position's ShardGtids and
// sets the ShardGtids's Gtid field to the Position's Gtid which is the most recent Gtid.
func (p *Position) GetBinlogShardGtids() []*binlogdata.ShardGtid {
	if len(p.ShardGtids) == 0 {
		return nil
	}

	// make sure that all shard gtids have the most current gtid.
	for i := 0; i < len(p.ShardGtids); i++ {
		p.ShardGtids[i].Gtid = p.Gtid
	}

	return p.ShardGtids
}

// MarshalSDKPosition marshals the underlying position into a sdk.Position as JSON bytes.
func (p *Position) MarshalSDKPosition() (sdk.Position, error) {
	positionBytes, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("marshal position: %w", err)
	}

	return sdk.Position(positionBytes), nil
}

// ParsePosition converts an sdk.Position into a Position.
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
