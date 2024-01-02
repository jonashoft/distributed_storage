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
PIDS=()
while read -r line; do
    PIDS+=("$line")
done < "$PID_FILE"

# Function to shuffle an array
shuffle() {
    local i tmp size max rand
    size=${#PIDS[*]}
    max=$(( 32768 / size * size ))

    for ((i=size-1; i>0; i--)); do
        while (( (rand=RANDOM) >= max )); do :; done
        rand=$(( rand % (i+1) ))
        tmp=${PIDS[i]} PIDS[i]=${PIDS[rand]} PIDS[rand]=$tmp
    done
}

# Shuffle the array
shuffle

# Select the first NUM_NODES_TO_STOP PIDs to terminate
for ((i=0; i<NUM_NODES_TO_STOP; i++)); do
    PID=${PIDS[i]}
    if kill -0 "$PID" > /dev/null 2>&1; then
        echo "Stopping node with PID $PID"
        kill "$PID"
    else
        echo "Node with PID $PID not found"
    fi
done

# Update the PID file
for ((i=0; i<NUM_NODES_TO_STOP; i++)); do
    grep -v "^${PIDS[i]}$" "$PID_FILE" > temp && mv temp "$PID_FILE"
done
