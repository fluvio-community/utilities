#!/bin/sh

TOPIC_IN=metrics-dev
PROFILE_IN=local
TOPIC_OUT=otelm:
# default otel is : http://localhost:4318/v1/metrics

cargo run --bin flvpipe -- \
    --num-records 100000 \
    $TOPIC_IN  --in-profile=$PROFILE_IN \
    $TOPIC_OUT
