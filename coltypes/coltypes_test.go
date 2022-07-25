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

package coltypes

import (
	"context"
	"reflect"
	"testing"
)

var (
	testRow = map[string]any{
		"bigint_column":   8437348,
		"bool_column":     int64(1),
		"char_column":     []uint8{0x63}, // Yw==
		"date_column":     "2000-09-19T00:00:00Z",
		"datetime_column": "2000-02-12T12:38:56Z",
		"decimal_column":  []uint8{0x31, 0x32, 0x2e, 0x32, 0x30}, // MTIuMjA=
		"double_column":   2.3,
		"enum_column":     []uint8{0x31}, // MQ==
		"float_column":    2.3,
		"int_column":      1,
		"json_column": []uint8{0x7b, 0x22, 0x6b, 0x65, 0x79, 0x31, 0x22, 0x3a,
			0x20, 0x22, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x31, 0x22, 0x2c, 0x20, 0x22,
			0x6b, 0x65, 0x79, 0x32, 0x22, 0x3a, 0x20, 0x22, 0x76, 0x61, 0x6c, 0x75,
			0x65, 0x32, 0x22, 0x7d}, // eyJrZXkxIjogInZhbHVlMSIsICJrZXkyIjogInZhbHVlMiJ9
		"json_column_null": nil,
		"longtext_column": []uint8{0x6c, 0x6f, 0x6e, 0x67,
			0x74, 0x65, 0x78, 0x74}, // bG9uZ3RleHQ=
		"mediumint_column": 1897,
		"mediumtext_column": []uint8{0x6d, 0x65, 0x64, 0x69, 0x75,
			0x6d, 0x74, 0x65, 0x78, 0x74}, // bWVkaXVtdGV4dA==
		"set_column":      []uint8{0x32}, // Mg==
		"smallint_column": 23,
		"text_column": []uint8{0x54, 0x65, 0x78, 0x74, 0x5f,
			0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e}, // VGV4dF9jb2x1bW4=
		"time_column": []uint8{0x31, 0x31, 0x3a, 0x31, 0x32,
			0x3a, 0x30, 0x30}, // MTE6MTI6MDA=
		"timestamp_column": "2000-01-01T00:00:01Z",
		"tinyint_column":   1,
		"tinytext_column": []uint8{0x74, 0x69, 0x6e, 0x79,
			0x74, 0x65, 0x78, 0x74}, // dGlueXRleHQ=
		"varchar_column": []uint8{0x76, 0x61, 0x72, 0x63, 0x68, 0x61, 0x72,
			0x5f, 0x73, 0x75, 0x70, 0x65, 0x72}, // dmFyY2hhcl9zdXBlcg==
		"year_column": 2012,
	}

	testRowColumnTypes = map[string]string{
		"bigint_column":     "bigint",
		"bool_column":       "bool",
		"char_column":       "char",
		"date_column":       "date",
		"datetime_column":   "datetime",
		"decimal_column":    "decimal",
		"double_column":     "double",
		"enum_column":       "enum",
		"float_column":      "float",
		"int_column":        "int",
		"json_column":       "json",
		"json_column_null":  "json",
		"longtext_column":   "longtext",
		"mediumint_column":  "mediumint",
		"mediumtext_column": "mediumtext",
		"set_column":        "set",
		"smallint_column":   "smallint",
		"text_column":       "text",
		"time_column":       "time",
		"timestamp_column":  "timestamp",
		"tinyint_column":    "tinyint",
		"tinytext_column":   "tinytext",
		"varchar_column":    "varchar",
		"year_column":       "year",
	}
)

func TestTransformRow(t *testing.T) {
	t.Parallel()

	type args struct {
		row         map[string]any
		columnTypes map[string]string
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]any
		wantErr bool
	}{
		{
			name: "success",
			args: args{
				row:         testRow,
				columnTypes: testRowColumnTypes,
			},
			want: map[string]any{
				"bigint_column":   8437348,
				"bool_column":     true,
				"char_column":     "c",
				"date_column":     "2000-09-19T00:00:00Z",
				"datetime_column": "2000-02-12T12:38:56Z",
				"decimal_column":  "12.20",
				"double_column":   2.3,
				"enum_column":     "1",
				"float_column":    2.3,
				"int_column":      1,
				"json_column": map[string]any{
					"key1": "value1",
					"key2": "value2",
				},
				"json_column_null":  nil,
				"longtext_column":   "longtext",
				"mediumint_column":  1897,
				"mediumtext_column": "mediumtext",
				"set_column":        "2",
				"smallint_column":   23,
				"text_column":       "Text_column",
				"time_column":       "11:12:00",
				"timestamp_column":  "2000-01-01T00:00:01Z",
				"tinyint_column":    1,
				"tinytext_column":   "tinytext",
				"varchar_column":    "varchar_super",
				"year_column":       2012,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := TransformRow(context.Background(), tt.args.row, tt.args.columnTypes)
			if (err != nil) != tt.wantErr {
				t.Errorf("TransformRow() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse() = %v, want %v", got, tt.want)
			}
		})
	}
}

func BenchmarkTransformRow(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := TransformRow(context.Background(), testRow, testRowColumnTypes)
		if err != nil {
			b.Fatalf("transform row: %v", err)

			return
		}
	}
}
