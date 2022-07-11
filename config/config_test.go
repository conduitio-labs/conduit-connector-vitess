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
					ConfigKeyAddress:   "localhost:15991",
					ConfigKeyTable:     "users",
					ConfigKeyKeyColumn: "id",
				},
			},
			want: Config{
				Address:   "localhost:15991",
				Table:     "users",
				KeyColumn: "id",
				Target:    defaultTarget,
			},
			wantErr: false,
		},
		{
			name: "success, required, default and auth fields",
			args: args{
				cfg: map[string]string{
					ConfigKeyAddress:   "localhost:15991",
					ConfigKeyTable:     "users",
					ConfigKeyKeyColumn: "id",
					ConfigKeyUsername:  "admin",
					ConfigKeyPassword:  "super_secret",
				},
			},
			want: Config{
				Address:   "localhost:15991",
				Table:     "users",
				KeyColumn: "id",
				Username:  "admin",
				Password:  "super_secret",
				Target:    defaultTarget,
			},
			wantErr: false,
		},
		{
			name: "success, required, auth and custom target fields",
			args: args{
				cfg: map[string]string{
					ConfigKeyAddress:   "localhost:15991",
					ConfigKeyTable:     "users",
					ConfigKeyKeyColumn: "id",
					ConfigKeyUsername:  "admin",
					ConfigKeyPassword:  "super_secret",
					ConfigKeyTarget:    "@replica",
				},
			},
			want: Config{
				Address:   "localhost:15991",
				Table:     "users",
				KeyColumn: "id",
				Username:  "admin",
				Password:  "super_secret",
				Target:    "@replica",
			},
			wantErr: false,
		},
		{
			name: "fail, invalid address, redundant scheme",
			args: args{
				cfg: map[string]string{
					ConfigKeyAddress:   "http://localhost:15991",
					ConfigKeyTable:     "users",
					ConfigKeyKeyColumn: "id",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, invalid address, port is missing",
			args: args{
				cfg: map[string]string{
					ConfigKeyAddress:   "localhost:",
					ConfigKeyTable:     "users",
					ConfigKeyKeyColumn: "id",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, invalid address, only host",
			args: args{
				cfg: map[string]string{
					ConfigKeyAddress:   "localhost:",
					ConfigKeyTable:     "users",
					ConfigKeyKeyColumn: "id",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, invalid table name, length is greater than 64",
			args: args{
				cfg: map[string]string{
					ConfigKeyAddress:   "localhost:15991",
					ConfigKeyTable:     "ABRATQkOlvPWqfTgUssUuGYCVkQJd4YlkQ1BEe51cctLMqCzjLanlwARrlXZVmd4vbJLne",
					ConfigKeyKeyColumn: "id",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, invalid keyColumn name, length is greater than 64",
			args: args{
				cfg: map[string]string{
					ConfigKeyAddress:   "localhost:15991",
					ConfigKeyTable:     "users",
					ConfigKeyKeyColumn: "RgRGxUoqE2py3swvXKkuR4d88OFs4hDwReY77sltzmPo6KS8aXDqk1ZN7AR5YgW5nV9OAe",
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
