#!/bin/bash
#
# Start the server that hosts replica X for all groups. Note that to serve on
# multiple IPs of the same port, run ./scripts/listen.sh first.

# Check if exactly one argument is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <replica (node) ID>"
    exit 1
fi

CONF_DIR=../gen-conf/
CONF=${CONF:-${CONF_DIR}/conf-localhost.json}

go build && ./tulip-node $CONF $1
