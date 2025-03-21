#!/bin/bash
#
# Execute the YCSB benchmark once.

# Check if exactly one argument is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <replica (node) ID>"
    exit 1
fi

CONF_DIR=../gen-conf/
CONF=${CONF:-${CONF_DIR}/conf-localhost.json}

NTHRDS=${${NTHRDS}:-1}

DURATION=${${DURATION}:-3}

RDRATIO=${${RDRATIO}:-100}

NKEYS=${${NKEYS}:-4}

RKEYS=${${RKEYS}:-1000000}

THETA=${${THETA}:-1}

go build && ./tulip-ycsb -conf     $CONF \
						 -nthrds   $NTHRDS \
						 -duration $DURATION \
						 -rdratio  $RDRATIO \
						 -nkeys    $NKEYS \
						 -rkeys    $RKEYS \
						 -theta    $THETA
