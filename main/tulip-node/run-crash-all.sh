#!/bin/bash
#
# Run all replica servers and kill/restart some of them. Fix the number of
# replica to 3.

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

./tulip-node $CONF 0 &
pid0=$!
./tulip-node $CONF 1 &
pid1=$!
./tulip-node $CONF 2 &
pid2=$!

sleep 5

# Force the first replica to start an election
pushd ../tulip-client && \
	go build && \
	echo "reelect 0 0" | ./run.sh 1>/dev/null && \
	popd

read -p "wait on the client to start"

echo "client started, 5 seconds without failure"
sleep 5

echo "kill the second replica (PID = $pid1), another 5 seconds"
kill $pid1
sleep 5

echo "bring the second replica back, another 5 seconds"
./tulip-node $CONF 1 &
pid1=$!
sleep 5
