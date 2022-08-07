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

package iterator

import (
	"testing"
)

func TestIterator_constructRuleFilter(t *testing.T) {
	t.Parallel()

	type args struct {
		table          string
		orderingColumn string
		columns        []string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "success, with columns specified",
			args: args{
				table:          "users",
				orderingColumn: "id",
				columns:        []string{"id", "name", "created_at"},
			},
			want:    "SELECT `id`, `name`, `created_at` FROM `users` ORDER BY `id` ASC",
			wantErr: false,
		},
		{
			name: "success, all columns",
			args: args{
				table:          "users",
				orderingColumn: "id",
			},
			want:    "SELECT * FROM `users` ORDER BY `id` ASC",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cdc := &CDC{}
			got, err := cdc.constructRuleFilter(tt.args.table, tt.args.orderingColumn, tt.args.columns)
			if (err != nil) != tt.wantErr {
				t.Errorf("Iterator.constructRuleFilter() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if got != tt.want {
				t.Errorf("Iterator.constructRuleFilter() = %v, want %v", got, tt.want)
			}
		})
	}
}
