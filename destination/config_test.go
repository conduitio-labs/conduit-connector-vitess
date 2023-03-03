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
			name: "success, all required fields",
			args: args{
				cfg: map[string]string{
					config.KeyAddress:  "localhost:15999",
					config.KeyTable:    "users",
					config.KeyKeyspace: "test",
					ConfigKeyKeyColumn: "id",
				},
			},
			want: Config{
				Config: config.Config{
					Address:      "localhost:15999",
					Table:        "users",
					Keyspace:     "test",
					TabletType:   "primary",
					RetryTimeout: config.DefaultRetryTimeout,
					MaxRetries:   config.DefaultMaxRetries,
				},
				KeyColumn: "id",
			},
			wantErr: false,
		},
		{
			name: "fail, missing keyColumn",
			args: args{
				cfg: map[string]string{
					config.KeyAddress:  "localhost:15999",
					config.KeyTable:    "users",
					config.KeyKeyspace: "test",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, invalid keyColumn, length is greater than 64",
			args: args{
				cfg: map[string]string{
					config.KeyAddress:  "localhost:15999",
					config.KeyTable:    "users",
					config.KeyKeyspace: "test",
					ConfigKeyKeyColumn: "ABRATQkOlvPWqfTgUssUuGYCVkQJd4YlkQ1BEe51cctLMqCzjLanlwARrlXZVmd4vbJLne",
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
