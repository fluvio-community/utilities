
# Intro
`flvpipe` can read a fluvio cluster/topic and send to another
fluvio cluster/topic or other custom destinations

`flvpipe` is oriented to one time transfers, for recurring continuous transfers
[connectors](https://www.fluvio.io/docs/latest/connectors/overview) are recommended.


# Use

## Cluster A/topic A to cluster B/topic B

This transfers 1000000 records from the fluvio cluster in the 'source' profile
to the fluvio cluster to the 'local' cluster with the respective topics "metrics"
and "metrics-dev".

```
PROFILE_IN=source
PROFILE_OUT=local
TOPIC_IN=metrics
TOPIC_OUT=metrics-dev

flvpipe \
    --num-records 100000 \
    $TOPIC_IN  --in-profile=$PROFILE_IN \
    $TOPIC_OUT --out-profile=$PROFILE_OUT
```

# Cluster A/topic A to an Opentelemetry metrics collector port

This transfers 500000 records from the end fluvio cluster in the 'source' profile
in topic "metrics-dev" to an http otel metrics collector port.

Note: in this case the assumption is that each topic record is an individual
binary otel resource metric.

Any standard Opentelemetry collector should receive this, but for diagnostics:
[otel-tui](https://github.com/ymtdzzz/otel-tui) will open a local collector
which can be sent to.

```
TOPIC_IN=metrics-dev
PROFILE_IN=local
TOPIC_OUT=otelm:
# default otel is : http://localhost:4318/v1/metrics

cargo run --bin flvpipe -- \
    --end 500000 \
    $TOPIC_IN  --in-profile=$PROFILE_IN \
    $TOPIC_OUT
```

