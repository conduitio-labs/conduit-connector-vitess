// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/ConduitIO/conduit-commons/tree/main/paramgen

package source

import (
	"github.com/conduitio/conduit-commons/config"
)

const (
	ConfigAddress        = "address"
	ConfigBatchSize      = "batchSize"
	ConfigColumns        = "columns"
	ConfigKeyColumn      = "keyColumn"
	ConfigKeyspace       = "keyspace"
	ConfigMaxRetries     = "maxRetries"
	ConfigOrderingColumn = "orderingColumn"
	ConfigPassword       = "password"
	ConfigRetryTimeout   = "retryTimeout"
	ConfigSnapshot       = "snapshot"
	ConfigTable          = "table"
	ConfigTabletType     = "tabletType"
	ConfigUsername       = "username"
)

func (Config) Parameters() map[string]config.Parameter {
	return map[string]config.Parameter{
		ConfigAddress: {
			Default:     "",
			Description: "Address is an address pointed to a VTGate instance.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		ConfigBatchSize: {
			Default:     "1000",
			Description: "BatchSize is a size of rows batch.",
			Type:        config.ParameterTypeInt,
			Validations: []config.Validation{
				config.ValidationGreaterThan{V: 0},
				config.ValidationLessThan{V: 100001},
			},
		},
		ConfigColumns: {
			Default:     "",
			Description: "Columns is a comma separated list of column names that should be included in each Record's payload.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigKeyColumn: {
			Default:     "",
			Description: "KeyColumn is a column name that records should use for their Key fields.\nMax length is 64, see [MySQL Identifier Length Limits].\n\n[MySQL Identifier Length Limits]: https://dev.mysql.com/doc/refman/8.0/en/identifier-length.html",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigKeyspace: {
			Default:     "",
			Description: "Keyspace specifies a VTGate keyspace.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		ConfigMaxRetries: {
			Default:     "3",
			Description: "MaxRetries is the number of reconnect retries the connector will make before giving up if a connection goes down.",
			Type:        config.ParameterTypeInt,
			Validations: []config.Validation{},
		},
		ConfigOrderingColumn: {
			Default:     "",
			Description: "OrderingColumn is a name of a column that the connector will use for ordering rows.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		ConfigPassword: {
			Default:     "",
			Description: "Password is a password of a VTGate user.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigRetryTimeout: {
			Default:     "1s",
			Description: "RetryTimeout is the time period that will be waited between retries.",
			Type:        config.ParameterTypeDuration,
			Validations: []config.Validation{},
		},
		ConfigSnapshot: {
			Default:     "true",
			Description: "Snapshot determines whether or not the connector will take a snapshot\nof the entire collection before starting CDC mode.",
			Type:        config.ParameterTypeBool,
			Validations: []config.Validation{},
		},
		ConfigTable: {
			Default:     "",
			Description: "Table is a name of the table that the connector should write to or read from.\nMax length is 64, see Identifier Length Limits\nhttps://dev.mysql.com/doc/refman/8.0/en/identifier-length.html",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		ConfigTabletType: {
			Default:     "",
			Description: "TabletType is a tabletType.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigUsername: {
			Default:     "",
			Description: "Username is a username of a VTGate user.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
	}
}
