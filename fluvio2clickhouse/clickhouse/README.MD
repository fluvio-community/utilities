## Run Clickhouse

Run the server:

```bash
clickhouse server
```

Run the client:

```bash
clickhouse client
```

Create a table:

```sql
CREATE TABLE events
(
    id String,
    timestamp String,
    payload String
)
ENGINE = MergeTree
PRIMARY KEY (id, timestamp)
```