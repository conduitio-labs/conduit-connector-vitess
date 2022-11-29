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
	"reflect"
	"testing"

	"github.com/conduitio-labs/conduit-connector-vitess/config"
)

func TestParseConfig(t *testing.T) {
	t.Parallel()

	type args struct {
		cfg map[string]string
	}
	tests := []struct {
		name    string
		args    args
		want    Config
		wantErr bool
	}{
		{
			name: "success, all fields",
			args: args{
				cfg: map[string]string{
					config.KeyAddress:       "localhost:15999",
					config.KeyTable:         "users",
					config.KeyKeyspace:      "test",
					ConfigKeyOrderingColumn: "id",
					ConfigKeyKeyColumn:      "id",
					ConfigKeyColumns:        "id,name,age",
					ConfigKeyBatchSize:      "200",
				},
			},
			want: Config{
				Config: config.Config{
					Address:    "localhost:15999",
					Table:      "users",
					Keyspace:   "test",
					TabletType: "primary",
				},
				OrderingColumn: "id",
				KeyColumn:      "id",
				Columns:        []string{"id", "name", "age"},
				BatchSize:      200,
				Snapshot:       defaultSnapshot,
			},
			wantErr: false,
		},
		{
			name: "success, only required fields",
			args: args{
				cfg: map[string]string{
					config.KeyAddress:       "localhost:15999",
					config.KeyTable:         "users",
					config.KeyKeyspace:      "test",
					ConfigKeyOrderingColumn: "id",
					ConfigKeyKeyColumn:      "id",
				},
			},
			want: Config{
				Config: config.Config{
					Address:    "localhost:15999",
					Table:      "users",
					Keyspace:   "test",
					TabletType: "primary",
				},
				OrderingColumn: "id",
				KeyColumn:      "id",
				BatchSize:      defaultBatchSize,
				Snapshot:       defaultSnapshot,
			},
			wantErr: false,
		},
		{
			name: "fail, ordering column is invalid",
			args: args{
				cfg: map[string]string{
					config.KeyAddress:       "localhost:15999",
					config.KeyTable:         "users",
					config.KeyKeyspace:      "test",
					ConfigKeyOrderingColumn: "verylongnameinordertofailthetestverylongnameinordertofailthetesta",
					ConfigKeyKeyColumn:      "id",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, ordering column is missing",
			args: args{
				cfg: map[string]string{
					config.KeyAddress:  "localhost:15999",
					config.KeyTable:    "users",
					config.KeyKeyspace: "test",
					ConfigKeyKeyColumn: "id",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, columns has an invalid element",
			args: args{
				cfg: map[string]string{
					config.KeyAddress:       "localhost:15999",
					config.KeyTable:         "users",
					config.KeyKeyspace:      "test",
					ConfigKeyOrderingColumn: "id",
					ConfigKeyKeyColumn:      "id",
					ConfigKeyColumns:        "id,verylongnameinordertofailthetestverylongnameinordertofailthetesta",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, batchSize is invalid, gte",
			args: args{
				cfg: map[string]string{
					config.KeyAddress:       "localhost:15999",
					config.KeyTable:         "users",
					config.KeyKeyspace:      "test",
					ConfigKeyOrderingColumn: "id",
					ConfigKeyKeyColumn:      "id",
					ConfigKeyBatchSize:      "-1",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, batchSize is invalid, lte",
			args: args{
				cfg: map[string]string{
					config.KeyAddress:       "localhost:15999",
					config.KeyTable:         "users",
					config.KeyKeyspace:      "test",
					ConfigKeyOrderingColumn: "id",
					ConfigKeyKeyColumn:      "id",
					ConfigKeyBatchSize:      "1000000",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, batchSize is invalid, not a number",
			args: args{
				cfg: map[string]string{
					config.KeyAddress:       "localhost:15999",
					config.KeyTable:         "users",
					config.KeyKeyspace:      "test",
					ConfigKeyOrderingColumn: "id",
					ConfigKeyKeyColumn:      "id",
					ConfigKeyBatchSize:      "one hundred",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, columns doesn't contain ordering column",
			args: args{
				cfg: map[string]string{
					config.KeyAddress:       "localhost:15999",
					config.KeyTable:         "users",
					config.KeyKeyspace:      "test",
					ConfigKeyOrderingColumn: "created_at",
					ConfigKeyKeyColumn:      "id",
					ConfigKeyColumns:        "id,name",
				},
			},
			want:    Config{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseConfig(tt.args.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseConfig() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}
