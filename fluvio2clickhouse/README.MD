# Fluvio to Clickhouse

Simple proxy that connects fluvio with Clickhouse.

## Run the proxy

```bash
cargo run
```

### Run Clickhouse

Navigate to the `clickhouse` directory and follow the instructions in the [README.md](./clickhouse/README.md) file.

### Run fluvio

Create a topic:

```bash
fluvio topic create events
```

Write events:

```bash
fluvio produce events
```

```bash
{"id": "1", "timestamp": "2020-01-01T00:00:00.000Z", "payload": "foo"}
{"id": "2", "timestamp": "2020-01-01T00:00:00.000Z", "payload": "bar"}
{"id": "3", "timestamp": "2020-01-01T00:00:00.000Z", "payload": "baz"}
```

