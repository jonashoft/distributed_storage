#!/bin/bash

# start_datanodes.sh

BASE_PORT=5556
NUMBER_OF_NODES=$1
PID_FILE="datanode_pids.txt"
LOG_DIR="logs"

mkdir -p $LOG_DIR
> $PID_FILE

for (( i=0; i<$NUMBER_OF_NODES; i++ ))
do
    PORT=$(($BASE_PORT + $i))
    LOG_FILE="$LOG_DIR/datanode_$i.log"
    python datanode.py $i $PORT $LOG_FILE &
    echo $! >> $PID_FILE
    echo "Started data node $i on port $PORT with PID $!, logging to $LOG_FILE"
done

echo "$NUMBER_OF_NODES data nodes started."