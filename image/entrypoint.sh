#!/bin/bash
set -ex


exec $SPARK_HOME/bin/spark-submit \
    --master=local[4] \
    --driver-memory 2G \
    --executor-memory 2G \
    --class TransformApp \
    --driver-java-options "-Dconfig.file=/opt/transform/config.json" \
    /opt/transform/transform-streaming-assembly.jar
