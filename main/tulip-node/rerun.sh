#!/bin/bash

# Check if exactly one argument is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <replica (node) ID>"
    exit 1
fi

git reset --hard && git pull && pushd ../../ && ./scripts/listen.sh && popd && go build && \
	./tulip-node conf.json $1
