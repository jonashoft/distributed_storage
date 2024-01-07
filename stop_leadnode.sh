#!/bin/bash

PID_FILE="leadnode_pid.txt"

if [ ! -f $PID_FILE ]; then
    echo "No PID file found. Are the data nodes running?"
    exit 1
fi

while read PID; do
    kill $PID
    echo "Killed lead node with PID $PID"
done < $PID_FILE

rm $PID_FILE

echo "Lead node stopped."
