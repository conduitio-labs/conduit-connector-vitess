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

package retrydialer

import (
	"context"
	"fmt"
	"net"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

// DialWithRetries keeps trying to dial the server until it connects or
// the number of retries exceeds the max retries.
func DialWithRetries(
	ctx context.Context,
	maxRetries int,
	retryTimeout time.Duration,
	address string,
) (net.Conn, error) {
	var (
		logger  = sdk.Logger(ctx)
		ticker  = time.NewTicker(retryTimeout)
		attempt = 1
		err     error
		dialer  net.Dialer
		conn    net.Conn
	)

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("do ctx done: %w", ctx.Err())

		case <-ticker.C:
			if attempt > maxRetries {
				return nil, fmt.Errorf("exceed retry limit: %w", err)
			}

			conn, err = dialer.DialContext(ctx, "tcp", address)
			if err == nil {
				return conn, nil
			}

			logger.Warn().
				Int("attempt", attempt).
				Msg("unable to dial the server, retrying...")

			attempt++
		}
	}
}
