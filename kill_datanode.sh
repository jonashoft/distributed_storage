#!/bin/bash
NUM_NODES_TO_STOP=$1
PID_FILE="datanode_pids.txt"

if [ -z "$NUM_NODES_TO_STOP" ]; then
    echo "Please specify the number of nodes to stop."
    exit 1
fi

if [ ! -f "$PID_FILE" ]; then
    echo "PID file not found."
    exit 1
fi

# Read PIDs into an array
mapfile -t PIDS < $PID_FILE

# Shuffle the array and select the first NUM_NODES_TO_STOP PIDs to terminate
# This can be changed to select the last NUM_NODES_TO_STOP PIDs instead
shuf -n $NUM_NODES_TO_STOP -e "${PIDS[@]}" | while read PID; do
    if kill -0 $PID > /dev/null 2>&1; then
        echo "Stopping node with PID $PID"
        kill $PID
    else
        echo "Node with PID $PID not found"
    fi
done

# Update the PID file
# This step is optional and depends on whether you want to maintain the PID file after stopping nodes
grep -vFf <(printf "%s\n" "${PIDS[@]:0:$NUM_NODES_TO_STOP}") $PID_FILE > temp && mv temp $PID_FILE