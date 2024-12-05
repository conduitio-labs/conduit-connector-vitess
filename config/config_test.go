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
	"testing"
)

func TestValidate(t *testing.T) {
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
			name: "success",
			args: args{
				cfg: &Config{
					Address:    "localhost:15991",
					Table:      "users",
					Keyspace:   "test",
					Username:   "user",
					Password:   "pass",
					TabletType: "primary",
				},
			},
			wantErr: nil,
		},
		{
			name: "missing password",
			args: args{
				cfg: &Config{
					Address:    "localhost:15991",
					Table:      "users",
					Keyspace:   "test",
					Username:   "user",
					TabletType: "primary",
				},
			},
			wantErr: fmt.Errorf("%q value is required if %q is provided", KeyPassword, KeyUsername),
		},
		{
			name: "missing username",
			args: args{
				cfg: &Config{
					Address:    "localhost:15991",
					Table:      "users",
					Keyspace:   "test",
					Password:   "pass",
					TabletType: "primary",
				},
			},
			wantErr: fmt.Errorf("%q value is required if %q is provided", KeyUsername, KeyPassword),
		},
		{
			name: "unknown tablet type",
			args: args{
				cfg: &Config{
					Address:    "localhost:15991",
					Table:      "users",
					Keyspace:   "test",
					TabletType: "type",
				},
			},
			wantErr: errors.New("unknown tablet type"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.args.cfg.Validate()
			if err != nil && err.Error() != tt.wantErr.Error() {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
