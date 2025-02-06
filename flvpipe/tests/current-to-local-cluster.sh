#!/bin/sh

PROFILE_IN=-
PROFILE_OUT=local
TOPIC_IN=metrics
TOPIC_OUT=metrics-dev

cargo run --bin flvpipe -- \
    --num-records 100000 \
    $TOPIC_IN  --in-profile=$PROFILE_IN \
    $TOPIC_OUT --out-profile=$PROFILE_OUT
