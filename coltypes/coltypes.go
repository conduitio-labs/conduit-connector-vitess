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

// Package coltypes implements functions for converting Vitess/MySQL column types to appropriate Go types.
package coltypes

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

const (
	jsonType = "json"

	// boolAliasType is our custom alias to the tinyint(1).
	boolAliasType = "bool"
	boolType      = "tinyint(1)"

	// Types that can be represented as Go strings.
	charType       = "char"
	textType       = "text"
	longTextType   = "longtext"
	mediumTextType = "mediumtext"
	tinyTextType   = "tinytext"
	varcharType    = "varchar"
	timeType       = "time" // format is 15:04:34
	enumType       = "enum"
	setType        = "set"
	decimalType    = "decimal"
)

var (
	// querySchemaColumnTypes is a query that selects column names and
	// their data and column types from the information_schema.
	querySchemaColumnTypes = "select column_name, data_type, column_type " +
		"from information_schema.columns where table_name = ?;"
)

// Querier is a database querier interface needed for the GetColumnTypes function.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// TransformRow converts row map values to appropriate Go types, based on the columnTypes.
// This is necessary because Vitess driver doesn't scan some types into a map[string]any correctly.
// Some types become byte slices instead of strings or maps (JSON), etc.
func TransformRow(ctx context.Context, row map[string]any, columnTypes map[string]string) (map[string]any, error) {
	result := make(map[string]any, len(row))

	for key, value := range row {
		if value == nil {
			result[key] = value

			continue
		}

		switch columnTypes[key] {
		case jsonType:
			valueBytes, ok := value.([]byte)
			if !ok {
				return nil, convertValueToBytesErr(key)
			}

			parsed := make(map[string]any)
			if err := json.Unmarshal(valueBytes, &parsed); err != nil {
				return nil, fmt.Errorf("unmarshal value to map: %w", err)
			}

			result[key] = parsed

		case boolAliasType:
			valueInt, ok := value.(int64)
			if !ok {
				return nil, convertValueToIntErr(key)
			}

			result[key] = valueInt > 0

		case charType, textType, longTextType, mediumTextType, tinyTextType,
			varcharType, timeType, enumType, setType, decimalType:
			valueBytes, ok := value.([]byte)
			if !ok {
				return nil, convertValueToBytesErr(key)
			}

			result[key] = string(valueBytes)

		default:
			result[key] = value
		}
	}

	return result, nil
}

// GetColumnTypes returns a map containing all table's columns and their database types.
func GetColumnTypes(ctx context.Context, querier Querier, tableName string) (map[string]string, error) {
	rows, err := querier.QueryContext(ctx, querySchemaColumnTypes, tableName)
	if err != nil {
		return nil, fmt.Errorf("query column types: %w", err)
	}

	columnTypes := make(map[string]string)
	for rows.Next() {
		var columnName, dataType, columnType string
		if err := rows.Scan(&columnName, &dataType, &columnType); err != nil {
			return nil, fmt.Errorf("scan rows: %w", err)
		}

		// tinyint(1) is what Vitess/MySQL use to represent the boolean type.
		// We'll just use an explicit convertion here in order to properly recognize booleans.
		if columnType == boolType {
			dataType = boolAliasType
		}

		columnTypes[columnName] = dataType
	}

	return columnTypes, nil
}
