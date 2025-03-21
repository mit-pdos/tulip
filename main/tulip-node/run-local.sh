#!/bin/bash

# Check if exactly one argument is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <replica (node) ID>"
    exit 1
fi

cp ../conf-localhost.json ./ && ./tulip-node conf-localhost.json $1
