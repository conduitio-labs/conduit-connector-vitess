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

package writer

import (
	"testing"
)

func TestWriter_buildInsertQuery(t *testing.T) {
	t.Parallel()

	type args struct {
		table   string
		columns []string
		values  []any
	}

	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "success, one field",
			args: args{
				table:   "users",
				columns: []string{"name"},
				values:  []any{"Void"},
			},
			want:    "INSERT INTO users (name) VALUES ('Void')",
			wantErr: false,
		},
		{
			name: "success, many fields",
			args: args{
				table:   "users",
				columns: []string{"id", "name"},
				values:  []any{1, "Void"},
			},
			want:    "INSERT INTO users (id, name) VALUES (1, 'Void')",
			wantErr: false,
		},
		{
			name: "fail, columns and values length mismatch",
			args: args{
				table:   "users",
				columns: []string{"id", "name"},
				values:  []any{1},
			},
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := &Writer{}

			got, err := w.buildInsertQuery(tt.args.table, tt.args.columns, tt.args.values)
			if (err != nil) != tt.wantErr {
				t.Errorf("Writer.buildInsertQuery() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if got != tt.want {
				t.Errorf("Writer.buildInsertQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}
