#!/bin/bash
INPUT_PATH="/index/data"

source .venv/bin/activate

# Check if different path is provided
if [ $# -eq 1 ]; then
    echo "User input detected. Processing..."
    if [ -d $1 ]; then
      hdfs dfs -put $1 /
    else
      hdfs dfs -put -f $1 /
    fi
    .venv/bin/python process_input.py $1 1
    INPUT_PATH="/processed"
    echo "User input prepared for mapreduce!"
fi

TMP_OUTPUT="/tmp/index/$(date +%s)"
hdfs dfs -mkdir -p $TMP_OUTPUT

echo "Starting indexing process..."
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
    -archives /app/.venv.tar.gz#.venv \
    -input $INPUT_PATH \
    -output $TMP_OUTPUT/stage1 \
    -mapper '.venv/bin/python mapper1.py' \
    -reducer '.venv/bin/python reducer1.py' \
    -numReduceTasks 1

echo "Indexing completed. Cleaning up..."
hdfs dfs -rm -r $TMP_OUTPUT

if [ ! -z "$TEMP_HDFS_PATH" ]; then
    hdfs dfs -rm -r $TEMP_HDFS_PATH
fi

echo "Index is ready in Cassandra database."
