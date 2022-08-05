# Conduit Connector Vitess

## General

The [Vitess](https://vitess.io/) connector is one of [Conduit](https://github.com/ConduitIO/conduit) plugins. It provides both, a source and a destination Vitess connector.

### Prerequisites

- [Go](https://go.dev/) 1.18
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.48.0
- [Vitess](https://vitess.io/docs/14.0/get-started/) v14.0

### How to build it

Run `make build`.

### Testing

Run `make test` to run all the unit and integration tests, which require Docker and Compose V2 to be installed and running. The command will handle starting and stopping docker containers for you.

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

## Configuration Options

| name        | description                                                                                               | required | default    |
| ----------- | --------------------------------------------------------------------------------------------------------- | -------- | ---------- |
| `address`   | The address pointed to a VTGate instance.<br />Format: `hostname:port`                                    | **true** |            |
| `table`     | The name of the table that the connector should write to.                                                 | **true** |            |
| `keyColumn` | Column name used to detect if the target table already contains the record.                               | **true** |            |
| `username`  | The username of a VTGate user.<br />Required if your VTGate instance has a static authentication enabled. | false    |            |
| `password`  | The password of a VTGate user.<br />Required if your VTGate instance has a static authentication enabled. | false    |            |
| `target`    | The target specifies the default target.                                                                  | false    | `@primary` |
