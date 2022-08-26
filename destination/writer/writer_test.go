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
	"time"
)

func TestWriter_buildUpsertQuery(t *testing.T) {
	t.Parallel()

	type args struct {
		table     string
		keyColumn string
		columns   []string
		values    []any
	}

	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "success, without keyColumn",
			args: args{
				table:     "users",
				keyColumn: "",
				columns:   []string{"name", "age"},
				values:    []any{"Void", 23},
			},
			want: "INSERT  INTO `users` (`name`, `age`) VALUES ('Void', 23) ON DUPLICATE KEY UPDATE `age`=23,`name`='Void'",
		},
		{
			name: "success, with keyColumn",
			args: args{
				table:     "users",
				keyColumn: "customer_id",
				columns:   []string{"customer_id", "name", "age"},
				values:    []any{1, "Void", 23},
			},
			want: "INSERT  INTO `users` (`customer_id`, `name`, `age`) VALUES (1, 'Void', 23) " +
				"ON DUPLICATE KEY UPDATE `age`=23,`name`='Void'",
		},
		{
			name: "fail, columns and values length mismatch",
			args: args{
				table:   "users",
				columns: []string{"name"},
				values:  []any{"Void", 23},
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
			got, err := w.buildUpsertQuery(tt.args.table, tt.args.keyColumn, tt.args.columns, tt.args.values)
			if (err != nil) != tt.wantErr {
				t.Errorf("Writer.buildUpsertQuery() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if got != tt.want {
				t.Errorf("Writer.buildUpsertQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func BenchmarkWriter_buildUpsertQuery(b *testing.B) {
	w := &Writer{}

	table := "users"
	keyColumn := "customer_id"
	columns := []string{"customer_id", "email", "age", "created_at"}
	values := []any{1, "example@gmail.com", 53, time.Now()}

	for n := 0; n < b.N; n++ {
		_, err := w.buildUpsertQuery(table, keyColumn, columns, values)
		if err != nil {
			b.Errorf("build upsert query: %v", err)
		}
	}
}

func TestWriter_buildDeleteQuery(t *testing.T) {
	t.Parallel()

	type args struct {
		table     string
		keyColumn string
		keyValue  any
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "success",
			args: args{
				table:     "users",
				keyColumn: "id",
				keyValue:  1,
			},
			want:    "DELETE `users` FROM `users` WHERE (`id` = 1)",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := &Writer{}
			got, err := w.buildDeleteQuery(tt.args.table, tt.args.keyColumn, tt.args.keyValue)
			if (err != nil) != tt.wantErr {
				t.Errorf("Writer.buildDeleteQuery() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if got != tt.want {
				t.Errorf("Writer.buildDeleteQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func BenchmarkWriter_buildDeleteQuery(b *testing.B) {
	w := &Writer{}

	table := "users"
	keyColumn := "customer_id"
	keyValue := 1

	for n := 0; n < b.N; n++ {
		_, err := w.buildDeleteQuery(table, keyColumn, keyValue)
		if err != nil {
			b.Errorf("build delete query: %v", err)
		}
	}
}
