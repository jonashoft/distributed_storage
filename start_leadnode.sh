#!/bin/bash

# start_datanodes.sh

K=$1
N=$2
PID_FILE="leadnode_pid.txt"

> $PID_FILE

python3 leadnode.py $i $K $N &
echo $! >> $PID_FILE

echo "lead node started"