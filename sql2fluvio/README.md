# Summary

`sql2fluvio` can be used to import of sqlite3 data into a fluvio topic.
The utility takes an argument of a Sqlite database file, runs a given query,
then converts each query result into json, and produces records to the named
Fluvio topic.

See `./tests` for sample sqlite3 databases and queries

## Sample usage:
`sql2fluvio  dbfile.sqlite3 query.sql ingest-topic`

## Versions

### v0.2.0

```
Usage: sql2fluvio [OPTIONS] <DB_PATH> <SQL_FILE_PATH> <TOPIC_NAME>

Arguments:
  <DB_PATH>        Path to the SQL database file
  <SQL_FILE_PATH>  Path to a generic SQL query
  <TOPIC_NAME>     topic to produce to

Options:
      --no-create  do not create the topic
  -h, --help       Print help
  -V, --version    Print version
```

#### Sample SQL query:

```sql
select  * from timetest
```

## Features
* Any select SQL statement should be supported.
* The response will be converted to json with all fields mapped.
* Produce to fluvio topic `topic-name`
* Shows status as it runs.

## Building

`cargo build --release`

## Installing

Install the binary to the local cargo bin path

`cargo install --path . --locked`

## Uninstalling

`cargo uninstall sql2fluvio`

## Running in directory

This does not install the binary

`cargo run -- <DB_PATH> <SQL_FILE_PATH> <TOPIC_NAME>`

