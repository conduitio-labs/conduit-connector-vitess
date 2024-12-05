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

package source

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
			name: "success, no error",
			args: args{
				cfg: &Config{
					Config: config.Config{
						Address:  "localhost:15999",
						Table:    "users",
						Keyspace: "test",
					},
					OrderingColumn: "id",
					KeyColumn:      "id",
					Columns:        []string{"id", "name", "age"},
					BatchSize:      200,
				},
			},
			wantErr: nil,
		},
		{
			name: "fail, columns has an invalid element",
			args: args{
				cfg: &Config{
					Config: config.Config{
						Address:  "localhost:15999",
						Table:    "users",
						Keyspace: "test",
					},
					OrderingColumn: "id",
					KeyColumn:      "id",
					Columns:        []string{"id", "verylongnameinordertofailthetestverylongnameinordertofailthetesta"},
				},
			},
			wantErr: fmt.Errorf("%s value must be less than or equal to %d", ConfigOrderingColumn, 64),
		},
		{
			name: "fail, columns doesn't contain ordering column",
			args: args{
				cfg: &Config{
					Config: config.Config{
						Address:  "localhost:15999",
						Table:    "users",
						Keyspace: "test",
					},
					OrderingColumn: "created_at",
					KeyColumn:      "id",
					Columns:        []string{"id", "name"},
				},
			},
			wantErr: fmt.Errorf("%v value must contains values of these fields: %v", ConfigColumns, ConfigOrderingColumn),
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
