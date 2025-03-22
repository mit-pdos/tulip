#!/bin/bash
#
# Run all replica servers.

# Check if exactly one argument is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <number of replica>"
    exit 1
fi

git pull

if [ "$?" -ne 0 ]; then
	echo "git pull failed"
	exit 1
fi

CONF_DIR=../gen-conf/
CONF=${CONF:-${CONF_DIR}/conf-localhost.json}

go build

if [ "$?" -ne 0 ]; then
	echo "go build failed"
	exit 1
fi

n=$1
for i in $(seq 0 $((n - 1))); do
	echo $i
    ./tulip-node $CONF $i &
done

sleep 5

pushd ../tulip-client && \
	go build && \
	echo "reelect 0 0" | ./run.sh 1>/dev/null && \
	popd

wait
