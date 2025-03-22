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

CONF_DIR=../gen-conf
CONF=${CONF:-${CONF_DIR}/conf-localhost.json}

go build

if [ "$?" -ne 0 ]; then
	echo "go build failed"
	exit 1
fi

# Function to clean up background jobs
cleanup() {
	if [ -z "$CLEANED_UP" ]; then
		CLEANED_UP=1  # Mark cleanup as done
		kill 0  # Sends SIGTERM to all background processes in the same process group
	fi
}

# Trap script exit (SIGINT, SIGTERM, EXIT) to call cleanup
trap cleanup INT TERM EXIT

n=$1
for i in $(seq 0 $((n - 1))); do
    ./tulip-node $CONF $i &
done

sleep 5

pushd ../tulip-client && \
	go build && \
	echo "reelect 0 0" | ./run.sh 1>/dev/null && \
	popd

wait
