# Conduit Connector Vitess

## General

The [Vitess](https://vitess.io/) connector is one of [Conduit](https://github.com/ConduitIO/conduit) plugins. It provides both, a source and a destination Vitess connector.

### Prerequisites

- [Go](https://go.dev/) 1.18
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.48.0
- [MySQL](https://www.mysql.com/) versions 5.7 to 8.0 with binlog enabled
- [Vitess](https://vitess.io/docs/14.0/get-started/) v14.0
- [Docker](https://www.docker.com/) and [docker-compose](https://docs.docker.com/compose/)

### How to build it

Run `make build`.

### Testing

Run `make test` to run all the unit and integration tests, which require Docker and Compose V2 to be installed and running. The command will handle starting and stopping docker containers for you.

## Source

The Vitess Source Connector connects to a VTGate with the provided `address`, `keyspace` and `tabletType` and starts creating records for each change detected in a table.

Upon starting, the source takes a snapshot of a given table in the database, then switches into CDC mode. In CDC mode, the plugin reads events from a VStream. In order for this to work correctly, binlog must be enabled on MySQL instances you use with Vitess.

### Snapshot Capture

When the connector first starts, snapshot mode is enabled. The connector reads all rows of the table in batches (this is configurable via the `batchSize` parameter). Once all rows in that initial snapshot are read the connector switches into CDC mode.

### Change Data Capture

This connector implements CDC features for Vitess by connecting to a VStream that listens to changes in the configured table. Every detected change is converted into a record and returned in the call to `Read`. If there is no record available at the moment `Read` is called, it blocks until a record is available or the connector receives a stop signal.

The connector in the CDC mode retrieves all the available shards from Vitess and tracks changes from all of them. If a reshard occurs, the connector will see the change and will listen for events from the new shards.

### Position handling

The connector goes through two modes.

- Snapshot mode. The position contains `keyspace` and a value of the last processed element of an ordering column you chose. This means that the ordering column must contain unique values.

- CDC mode. The position in this mode contains the same fields as in the Snapshot mode plus a list shards (and their unique shard transaction identifiers, gtid) of a `keyspace` you chose. The connector retrieves all the available shards at startup and watches them for changes.

### Configuration Options

| name             | description                                                                                                                                                                                  | required | default       |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- | ------------- |
| `address`        | The address pointed to a VTGate instance.<br />Format: `hostname:port`                                                                                                                       | **true** |               |
| `table`          | The name of the table that the connector should write to.                                                                                                                                    | **true** |               |
| `keyColumn`      | Column name used to detect if the target table already contains the record.                                                                                                                  | **true** |               |
| `keyspace`       | The keyspace specifies a VTGate keyspace.                                                                                                                                                    | **true** |               |
| `orderingColumn` | The name of a column that the connector will use for ordering rows.                                                                                                                          | **true** |               |
| `username`       | Username of a VTGate user.<br />Required if your VTGate instance has a static authentication enabled.                                                                                        | false    |               |
| `password`       | Password of a VTGate user.<br />Required if your VTGate instance has a static authentication enabled.                                                                                        | false    |               |
| `tabletType`     | Specifies a VTGate tablet type.                                                                                                                                                              | false    | `primary`     |
| `columns`        | Comma separated list of column names that should be included in each Record's payload.<br />If the field is not empty it must contain values of the `keyColumn` and `orderingColumn` fields. | false    | `all columns` |
| `batchSize`      | Size of rows batch. Min is `1` and max is `100000`.                                                                                                                                          | false    | `1000`        |

### Columns

If no column names are provided in the config, then the connector will assume that all columns in the table should be returned.

## Destination

The Vitess Destination takes a `record.Record` and parses it into a valid SQL query. The Destination is designed to handle different payloads and keys. Because of this, each record is individually parsed and upserted.

### Table name

If a record contains a `table` property in its metadata it will be inserted in that table, otherwise it will fall back
to use the table configured in the connector. This way the Destination can support multiple tables in the same
connector, provided the user has proper access to those tables.

### Upsert Behavior

If the target table already contains a record with the same key, the Destination will upsert with its current received
values. Because Keys must be unique, this can overwrite and thus potentially lose data, so keys should be assigned
correctly from the Source.

If there is no key, the record will be simply appended.

### Configuration Options

| name         | description                                                                                           | required | default   |
| ------------ | ----------------------------------------------------------------------------------------------------- | -------- | --------- |
| `address`    | The address pointed to a VTGate instance.<br />Format: `hostname:port`                                | **true** |           |
| `table`      | The name of the table that the connector should write to.                                             | **true** |           |
| `keyColumn`  | Column name used to detect if the target table already contains the record.                           | **true** |           |
| `keyspace`   | Specifies a VTGate keyspace.                                                                          | **true** |           |
| `username`   | Username of a VTGate user.<br />Required if your VTGate instance has a static authentication enabled. | false    |           |
| `password`   | Password of a VTGate user.<br />Required if your VTGate instance has a static authentication enabled. | false    |           |
| `tabletType` | Specifies a VTGate tablet type.                                                                       | false    | `primary` |
