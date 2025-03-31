#!/bin/bash
#
# Execute the Retwis benchmark multiple times.

if [ -z "$1" ]; then
	nruns=1
else
	nruns=$1
fi

go build

if [ "$?" -ne 0 ]; then
	echo "go build failed"
	exit 1
fi

# Set up the result file path
RESULT_DIR=./exp
RESULT_FILE=${RESULT_DIR}/$(date +"%Y%m%d_%H%M%S").csv

mkdir -p $RESULT_DIR

# Set the config file path
CONF_DIR=../gen-conf
CONF_FILE=${CONF:-${CONF_DIR}/conf-datacenter.json}

DURATION=${DURATION:-10}

RKEYS=${RKEYS:-1000000}

SZKEY=${SZKEY:-64}

THETA=${THETA:--1}

for i in $(seq $nruns)
do
	for nthrds in $(seq 24)
	do
		stdbuf -o 0 ./tulip-retwis \
			   -conf     $CONF_FILE \
			   -nthrds   $nthrds \
			   -duration $DURATION \
			   -rkeys    $RKEYS \
			   -szkey    $SZKEY \
			   -theta    $THETA \
			   -exp
			| tee -a $RESULT_FILE
	done
done
