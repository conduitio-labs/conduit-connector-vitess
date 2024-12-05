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
	"regexp"
	"strconv"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/proto/topodata"
)

const (
	// KeyAddress is a config name for an address.
	KeyAddress = "address"
	// KeyUsername is a config name for an username.
	KeyUsername = "username"
	// KeyPassword is a config name for an password.
	KeyPassword = "password"
)

// ErrUnknownTabletType occurs when a provided tablet type is not valid.
var ErrUnknownTabletType = errors.New("unknown tablet type")

// defaultTabletType is a default Vitess tablet type.
var defaultTabletType = strings.ToLower(topodata.TabletType_name[int32(topodata.TabletType_PRIMARY)])

// Config contains configurable values
// shared between source and destination Vitess connector.
type Config struct {
	// Address is an address pointed to a VTGate instance.
	Address string `json:"address" validate:"required"`
	// Table is a name of the table that the connector should write to or read from.
	// Max length is 64, see Identifier Length Limits
	// https://dev.mysql.com/doc/refman/8.0/en/identifier-length.html
	Table string `json:"table" validate:"required"`
	// Keyspace specifies a VTGate keyspace.
	Keyspace string `json:"keyspace" validate:"required"`
	// Username is a username of a VTGate user.
	Username string `json:"username"`
	// Password is a password of a VTGate user.
	Password string `json:"password"`
	// TabletType is a tabletType.
	TabletType string `json:"tabletType"`
	// MaxRetries is the number of reconnect retries the connector will make before giving up if a connection goes down.
	MaxRetries int `json:"maxRetries" default:"3"`
	// RetryTimeout is the time period that will be waited between retries.
	RetryTimeout time.Duration `json:"retryTimeout" default:"1s"`
}

func (c *Config) Validate() error {
	if !validateAddress(c.Address) {
		return fmt.Errorf("%s value must be in the form hostname:port", KeyAddress)
	}

	if len(c.Table) > 64 {
		return fmt.Errorf("table value must be less than or equal to 64")
	}

	if c.Username != "" && c.Password == "" {
		return fmt.Errorf("%q value is required if %q is provided", KeyPassword, KeyUsername)
	}

	if c.Password != "" && c.Username == "" {
		return fmt.Errorf("%q value is required if %q is provided", KeyUsername, KeyPassword)
	}

	if c.TabletType == "" {
		c.TabletType = defaultTabletType
	} else {
		_, ok := topodata.TabletType_value[strings.ToUpper(c.TabletType)]
		if !ok {
			return ErrUnknownTabletType
		}

		c.TabletType = strings.ToLower(c.TabletType)
	}

	return nil
}

func validateAddress(addr string) bool {
	re := regexp.MustCompile(`^([a-zA-Z0-9.-]+):(\d+)$`)
	matches := re.FindStringSubmatch(addr)

	if len(matches) != 3 {
		return false
	}

	port := matches[2]
	if len(port) > 0 {
		_, err := strconv.Atoi(port)
		if err != nil {
			return false
		}
	}

	return true
}
