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

package retry

import (
	"context"
	"fmt"
	"net"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	// defaultTimeout is a default timeout that is used
	// when a timeout provided to the DialWithAttempts function is less or equal to zero.
	defaultTimeout = time.Millisecond * 10

	// defaultRetries is a default retries, that given to dial to vitess grpc server.
	defaultRetries = 3
)

// DialWithAttempts keeps trying dial to vitess to grpc until it connects or number of retry
// will exceed max retries.
func DialWithAttempts(ctx context.Context, maxRetries int, address string) (net.Conn, error) {
	var (
		logger  = sdk.Logger(ctx)
		ticker  = time.NewTicker(defaultTimeout)
		attempt = 1
		err     error
		dialer  net.Dialer
		conn    net.Conn
	)
	if maxRetries <= 0 {
		maxRetries = defaultRetries
	}

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("do ctx done: %w", ctx.Err())

		case <-ticker.C:
			if attempt > maxRetries {
				logger.Error().Msgf("unable to dial: %s", err.Error())

				return nil, err
			}

			logger.Info().Msgf("[%d]: creating attempt to dial to server...", attempt)
			conn, err = dialer.DialContext(ctx, "tcp", address)
			if err == nil {
				return conn, err
			}
			logger.Warn().Msgf("[%d]: unable to dial to server, retrying...", attempt)

			attempt++
		}
	}
}
