#!/bin/bash
#
# Execute the continuous profiling benchmark.

CONF_DIR=../gen-conf/
CONF=${CONF:-${CONF_DIR}/conf-localhost.json}

DURATION=${DURATION:-5}

STEP=${STEP:-1000}

go build && ./tulip-cont -conf     $CONF \
						 -duration $DURATION \
						 -step     $STEP
