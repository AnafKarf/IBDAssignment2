#!/bin/bash
INPUT_PATH="/index/data"

# Check if user provided a different path
if [ $# -eq 1 ]; then
    INPUT_PATH=$1
fi

echo "Checking Cassandra schema..."
cqlsh -e "DESCRIBE KEYSPACE search_engine" > /dev/null 2>&1
if [ $? -ne 0 ]; then
    bash set_cassandra.sh
fi

# Check if input is local file or HDFS path
if [[ $INPUT_PATH == hdfs://* ]]; then
    # HDFS path
    hdfs dfs -ls $INPUT_PATH > /dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo "HDFS path $INPUT_PATH does not exist"
        exit 1
    fi
else
    # Local file
    if [ ! -f $INPUT_PATH ] && [ ! -d $INPUT_PATH ]; then
        echo "Local path $INPUT_PATH does not exist"
        exit 1
    fi

    # If it's a local file/directory, copy to HDFS
    TEMP_HDFS_PATH="/tmp/local_index_data_$(date +%s)"
    hdfs dfs -mkdir -p $TEMP_HDFS_PATH
    hdfs dfs -put $INPUT_PATH $TEMP_HDFS_PATH/
    INPUT_PATH=$TEMP_HDFS_PATH/$(basename $INPUT_PATH)
fi

# Create temporary HDFS directory for intermediate output
TMP_OUTPUT="/tmp/index/$(date +%s)"
hdfs dfs -mkdir -p $TMP_OUTPUT

# Run MapReduce job
echo "Starting indexing process..."
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -input $INPUT_PATH \
    -output $TMP_OUTPUT/stage1 \
    -mapper mapper1.py \
    -reducer reducer1.py \
    -file mapper1.py \
    -file reducer1.py

echo "Indexing completed. Cleaning up..."
hdfs dfs -rm -r $TMP_OUTPUT

if [ ! -z "$TEMP_HDFS_PATH" ]; then
    hdfs dfs -rm -r $TEMP_HDFS_PATH
fi

echo "Index is ready in Cassandra database."