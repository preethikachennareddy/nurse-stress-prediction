#!/bin/bash
# Wait for Flink cluster to be ready
sleep 15

# Pick the shaded JAR (ignore original-*.jar)
JAR_FILE=$(ls target/*SNAPSHOT.jar | grep -v original | head -n 1)

echo "Submitting JAR: $JAR_FILE to Flink cluster at $FLINK_HOST:$FLINK_PORT"

flink run -m $FLINK_HOST:$FLINK_PORT $JAR_FILE
 ss