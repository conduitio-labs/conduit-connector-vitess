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

package config

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/conduitio-labs/conduit-connector-vitess/validator"
	"vitess.io/vitess/go/vt/proto/topodata"
)

// ErrUnknownTabletType occurs when a provided tablet type is not valid.
var ErrUnknownTabletType = errors.New("unknown tablet type")

// defaultTabletType is a default Vitess tablet type.
var defaultTabletType = strings.ToLower(topodata.TabletType_name[int32(topodata.TabletType_PRIMARY)])

const (
	// KeyAddress is a config name for an address.
	KeyAddress = "address"
	// KeyTable is a config name for an table.
	KeyTable = "table"
	// KeyUsername is a config name for an username.
	KeyUsername = "username"
	// KeyPassword is a config name for an password.
	KeyPassword = "password"
	// KeyKeyspace is a config name for a keyspace.
	KeyKeyspace = "keyspace"
	// KeyTabletType is a config name for a tabletType.
	KeyTabletType = "tabletType"
	// KeyMaxRetries is a config name for a grpc max retries.
	KeyMaxRetries = "maxRetries"
	// KeyRetryTimeout is a config name for grpc retry timeout.
	KeyRetryTimeout = "retryTimeout"
)

const (
	// DefaultRetryTimeout is a default timeout that is used
	// when a timeout provided to the DialWithRetries function is less or equal to zero.
	DefaultRetryTimeout = time.Second

	// DefaultMaxRetries is a default retries, that given to dial to vitess grpc server.
	DefaultMaxRetries = 3
)

// Config contains configurable values
// shared between source and destination Vitess connector.
type Config struct {
	// Address is an address pointed to a VTGate instance.
	Address string `key:"address" validate:"required,hostname_port"`
	// Table is a name of the table that the connector should write to or read from.
	// Max length is 64, see Identifier Length Limits
	// https://dev.mysql.com/doc/refman/8.0/en/identifier-length.html
	Table string `key:"table" validate:"required,max=64"`
	// Keyspace specifies a VTGate keyspace.
	Keyspace string `key:"keyspace" validate:"required"`
	// Username is a username of a VTGate user.
	Username string `key:"username" validate:"required_with=Password"`
	// Password is a password of a VTGate user.
	Password string `key:"password" validate:"required_with=Username"`
	// TabletType is a tabletType.
	TabletType string `key:"tabletType"`
	// MaxRetries is the number of reconnect retries the connector will make before giving up if a connection goes down.
	MaxRetries int `key:"maxRetries"`
	// RetryTimeout is the time period that will be waited between retries.
	RetryTimeout time.Duration `key:"retryTimeout" validate:"gte=0"`
}

// Parse attempts to parse a provided map[string]string into a Config struct.
func Parse(cfg map[string]string) (Config, error) {
	config := Config{
		Address:      cfg[KeyAddress],
		Table:        strings.ToLower(cfg[KeyTable]),
		Username:     cfg[KeyUsername],
		Password:     cfg[KeyPassword],
		Keyspace:     cfg[KeyKeyspace],
		TabletType:   defaultTabletType,
		MaxRetries:   DefaultMaxRetries,
		RetryTimeout: DefaultRetryTimeout,
	}

	// validate tablet type
	if tabletType := cfg[KeyTabletType]; tabletType != "" {
		_, ok := topodata.TabletType_value[strings.ToUpper(tabletType)]
		if !ok {
			return Config{}, ErrUnknownTabletType
		}

		config.TabletType = strings.ToLower(tabletType)
	}

	if maxRetriesStr := cfg[KeyMaxRetries]; maxRetriesStr != "" {
		retries, err := strconv.Atoi(maxRetriesStr)
		if err != nil {
			return Config{}, fmt.Errorf("invalid retries: %w", err)
		}

		config.MaxRetries = retries
	}

	if retryTimeoutStr := cfg[KeyRetryTimeout]; retryTimeoutStr != "" {
		retryTimeout, err := time.ParseDuration(retryTimeoutStr)
		if err != nil {
			return Config{}, fmt.Errorf("invalid retry timeout: %w", err)
		}

		if retryTimeout != 0 {
			config.RetryTimeout = retryTimeout
		}
	}

	if err := validator.ValidateStruct(&config); err != nil {
		return Config{}, fmt.Errorf("validate config: %w", err)
	}

	return config, nil
}
