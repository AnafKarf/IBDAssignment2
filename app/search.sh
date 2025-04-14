#!/bin/bash

source .venv/bin/activate

if [ $# -eq 0 ]; then
    echo "Usage: $0 \"query text\""
    exit 1
fi

QUERY="$*"

spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 \
    --conf spark.cassandra.connection.host=cassandra \
    query.py "$QUERY"
