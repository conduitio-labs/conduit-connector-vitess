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
	"testing"

	"github.com/conduitio-labs/conduit-connector-vitess/config"
)

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	type args struct {
		cfg *Config
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name: "success, all required fields",
			args: args{
				cfg: &Config{
					Config: config.Config{
						Address:  "localhost:15999",
						Table:    "users",
						Keyspace: "test",
					},
					KeyColumn: "id",
				},
			},
			wantErr: nil,
		},
		{
			name: "fail, invalid keyColumn, length is greater than 64",
			args: args{
				cfg: &Config{
					Config: config.Config{
						Address:  "localhost:15999",
						Table:    "users",
						Keyspace: "test",
					},
					KeyColumn: "ABRATQkOlvPWqfTgUssUuGYCVkQJd4YlkQ1BEe51cctLMqCzjLanlwARrlXZVmd4vbJLne",
				},
			},
			wantErr: fmt.Errorf("%s value must be less than or equal to %d", ConfigKeyColumn, 64),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.args.cfg.validate()
			if err != nil && tt.wantErr != nil && err.Error() != tt.wantErr.Error() {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
