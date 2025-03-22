#!/bin/bash
#
# Run all replica servers.

# Check if exactly one argument is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <number of replica>"
    exit 1
fi

CONF_DIR=../gen-conf/
CONF=${CONF:-${CONF_DIR}/conf-localhost.json}

go build

if [ "$?" -ne 0 ]; then
	echo "go build failed"
	exit 1
fi

for i in $(seq 1 $((n - 1))); do
    ./tulip-node $CONF $i &
done
