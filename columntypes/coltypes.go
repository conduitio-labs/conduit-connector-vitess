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

// Package columntypes implements functions for converting Vitess/MySQL column types to appropriate Go types.
package columntypes

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
)

const (
	// timeTypeFormat defines a format for the MySQL's TIME type.
	timeTypeFormat = "15:04:05"
)

// querySchemaColumnTypes is a query that selects column names and
// their data and column types from the information_schema.
var querySchemaColumnTypes = "select column_name, data_type " +
	"from information_schema.columns where table_name = ?;"

// Querier is a database querier interface needed for the GetColumnTypes function.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// TransformValuesToNative converts the values of type sqltypes.Value
// to appropriate Go types, based on the fields parameter.
// This is necessary because the underlying raw values are just byte slices.
func TransformValuesToNative(fields []*query.Field, values []sqltypes.Value) (map[string]any, error) {
	if len(fields) != len(values) {
		return nil, ErrFieldsValuesLenMissmatch
	}

	result := make(map[string]any, len(fields))

	for i, field := range fields {
		// seperate check for null type, since the field.Type may not match the values[i].Type()
		if field.Type == query.Type_NULL_TYPE || values[i].Type() == query.Type_NULL_TYPE {
			result[field.Name] = nil

			continue
		}

		switch field.Type {
		case query.Type_INT8, query.Type_INT16, query.Type_INT24,
			query.Type_INT32, query.Type_INT64, query.Type_YEAR, query.Type_BIT:

			// if the column length is 1,
			// and the column type is the integral type of INT8 - the column is boolean
			if field.ColumnLength == 1 && field.Type == query.Type_INT8 {
				boolValue, err := values[i].ToBool()
				if err != nil {
					return nil, fmt.Errorf("convert value to bool: %w", err)
				}

				result[field.Name] = boolValue

				continue
			}

			int64Value, err := values[i].ToInt64()
			if err != nil {
				return nil, fmt.Errorf("convert value to int64: %w", err)
			}

			result[field.Name] = int64Value

		case query.Type_UINT8, query.Type_UINT16, query.Type_UINT24,
			query.Type_UINT32, query.Type_UINT64:

			uint64Value, err := values[i].ToUint64()
			if err != nil {
				return nil, fmt.Errorf("convert value to uint64: %w", err)
			}

			result[field.Name] = uint64Value

		case query.Type_FLOAT32, query.Type_FLOAT64:
			float64Value, err := values[i].ToFloat64()
			if err != nil {
				return nil, fmt.Errorf("convert value to float64: %w", err)
			}

			result[field.Name] = float64Value

		case query.Type_TIMESTAMP, query.Type_DATETIME:
			timeValue, err := datetimeToNative(values[i], time.UTC)
			if err != nil {
				return nil, fmt.Errorf("convert datetime/timestamp value to time.Time: %w", err)
			}

			result[field.Name] = timeValue

		case query.Type_DATE:
			timeValue, err := dateToNative(values[i], time.UTC)
			if err != nil {
				return nil, fmt.Errorf("convert date value to time.Time: %w", err)
			}

			result[field.Name] = timeValue

		case query.Type_TIME:
			timeValue, err := time.Parse(timeTypeFormat, values[i].RawStr())
			if err != nil {
				return nil, fmt.Errorf("convert time value to time.Time: %w", err)
			}

			result[field.Name] = timeValue

		case query.Type_DECIMAL, query.Type_TEXT, query.Type_VARCHAR,
			query.Type_CHAR, query.Type_ENUM, query.Type_SET,
			query.Type_HEXNUM, query.Type_HEXVAL:

			result[field.Name] = values[i].ToString()

		case query.Type_BLOB, query.Type_VARBINARY, query.Type_BINARY:
			result[field.Name] = values[i].Raw()

		case query.Type_JSON:
			var rawValue map[string]any
			if err := json.Unmarshal(values[i].Raw(), &rawValue); err != nil {
				return nil, fmt.Errorf("unmashal json value: %w", err)
			}

			result[field.Name] = rawValue

		default:
			result[field.Name] = values[i].Raw()
		}
	}

	return result, nil
}

// ConvertStructureData converts an sdk.StructureData values to a proper database types.
// For now it's just converts TIMESTAMP, DATETIME, DATE and TIME values.
func ConvertStructureData(columnTypes map[string]string, data opencdc.StructuredData) (opencdc.StructuredData, error) {
	result := make(opencdc.StructuredData, len(data))

	for key, value := range data {
		if value == nil {
			result[key] = value

			continue
		}

		switch columnTypes[key] {
		case query.Type_name[int32(query.Type_TIMESTAMP)], query.Type_name[int32(query.Type_DATETIME)],
			query.Type_name[int32(query.Type_DATE)], query.Type_name[int32(query.Type_TIME)]:

			valueStr, ok := value.(string)
			if !ok {
				return nil, ErrValueIsNotAString
			}

			timeValue, err := time.Parse(time.RFC3339, valueStr)
			if err != nil {
				return nil, fmt.Errorf("convert value to time.Time: %w", err)
			}

			result[key] = timeValue

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
	if rows.Err() != nil {
		return nil, fmt.Errorf("query column types: %w", err)
	}
	defer rows.Close()

	columnTypes := make(map[string]string)
	for rows.Next() {
		var columnName, dataType string
		if err := rows.Scan(&columnName, &dataType); err != nil {
			return nil, fmt.Errorf("scan rows: %w", err)
		}

		columnTypes[columnName] = strings.ToUpper(dataType)
	}

	return columnTypes, nil
}
