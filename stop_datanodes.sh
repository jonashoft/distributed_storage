#!/bin/bash

PID_FILE="datanode_pids.txt"

if [ ! -f $PID_FILE ]; then
    echo "No PID file found. Are the data nodes running?"
    exit 1
fi

while read PID; do
    kill $PID
    echo "Killed data node with PID $PID"
done < $PID_FILE

# Remove the PID file
rm $PID_FILE

echo "All data nodes stopped."
