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
	"reflect"
	"testing"
)

func TestParse(t *testing.T) {
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
			name: "success, required and default fields",
			args: args{
				cfg: map[string]string{
					KeyAddress:  "localhost:15991",
					KeyTable:    "users",
					KeyKeyspace: "test",
				},
			},
			want: Config{
				Address:      "localhost:15991",
				Table:        "users",
				Keyspace:     "test",
				TabletType:   defaultTabletType,
				RetryTimeout: DefaultRetryTimeout,
				MaxRetries:   DefaultMaxRetries,
			},
			wantErr: false,
		},
		{
			name: "success, required, default and auth fields",
			args: args{
				cfg: map[string]string{
					KeyAddress:  "localhost:15991",
					KeyTable:    "users",
					KeyKeyspace: "test",
					KeyUsername: "admin",
					KeyPassword: "super_secret",
				},
			},
			want: Config{
				Address:      "localhost:15991",
				Table:        "users",
				Username:     "admin",
				Password:     "super_secret",
				Keyspace:     "test",
				TabletType:   defaultTabletType,
				RetryTimeout: DefaultRetryTimeout,
				MaxRetries:   DefaultMaxRetries,
			},
			wantErr: false,
		},
		{
			name: "success, required, auth and custom target fields",
			args: args{
				cfg: map[string]string{
					KeyAddress:    "localhost:15991",
					KeyTable:      "users",
					KeyKeyspace:   "test",
					KeyUsername:   "admin",
					KeyPassword:   "super_secret",
					KeyTabletType: "replica",
				},
			},
			want: Config{
				Address:      "localhost:15991",
				Table:        "users",
				Username:     "admin",
				Password:     "super_secret",
				Keyspace:     "test",
				TabletType:   "replica",
				RetryTimeout: DefaultRetryTimeout,
				MaxRetries:   DefaultMaxRetries,
			},
			wantErr: false,
		},
		{
			name: "fail, invalid address, redundant scheme",
			args: args{
				cfg: map[string]string{
					KeyAddress:  "http://localhost:15991",
					KeyTable:    "users",
					KeyKeyspace: "test",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, invalid address, port is missing",
			args: args{
				cfg: map[string]string{
					KeyAddress:  "localhost:",
					KeyTable:    "users",
					KeyKeyspace: "test",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, invalid address, only host",
			args: args{
				cfg: map[string]string{
					KeyAddress:  "localhost:",
					KeyTable:    "users",
					KeyKeyspace: "test",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, invalid table name, length is greater than 64",
			args: args{
				cfg: map[string]string{
					KeyAddress:  "localhost:15991",
					KeyTable:    "ABRATQkOlvPWqfTgUssUuGYCVkQJd4YlkQ1BEe51cctLMqCzjLanlwARrlXZVmd4vbJLne",
					KeyKeyspace: "test",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, invalid tabletType",
			args: args{
				cfg: map[string]string{
					KeyAddress:    "localhost:15991",
					KeyTable:      "users",
					KeyTabletType: "invalid",
					KeyKeyspace:   "test",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, missing keyspace",
			args: args{
				cfg: map[string]string{
					KeyAddress: "localhost:15991",
					KeyTable:   "users",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, username is provided, but password is not",
			args: args{
				cfg: map[string]string{
					KeyAddress:  "localhost:15991",
					KeyTable:    "users",
					KeyKeyspace: "test",
					KeyUsername: "admin",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, password is provided, but username is not",
			args: args{
				cfg: map[string]string{
					KeyAddress:  "localhost:15991",
					KeyTable:    "users",
					KeyKeyspace: "test",
					KeyPassword: "secret",
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

			got, err := Parse(tt.args.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse() = %v, want %v", got, tt.want)
			}
		})
	}
}
