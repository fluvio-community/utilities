# Summary

`sql2fluvio` takes a SQL file and query as input, converts each query result
into json, and produces fluvio records to the named topic.

See `./tests` for sample sqlite3 and queries

## Versions

### v0.2.0

Usage: sql2fluvio <DB_PATH> <SQL_FILE_PATH> <TOPIC_NAME>

Arguments:
  <DB_PATH>        Path to the SQL database file
  <SQL_FILE_PATH>  Path to a generic SQL query
  <TOPIC_NAME>     topic to produce to

Sample usage:
`sql2fluvio  dbfile.sqlite3 test.sql ingest-topic`

Sample SQL query:

```sql
select  * from timetest
```
## Features
* Any select SQL statement should be supported.
* The response will be converted to json with all fields mapped.
* Produce to fluvio topic `topic-name`
* Nice to have - show status as it runs.

## Building

`cargo build --release`

## Running in directory

`cargo run -- <DB_PATH> <SQL_FILE_PATH> <TOPIC_NAME>`

## Installing

Install the binary to a cargo bin path

`cargo install --path .`

