#!/bin/bash
#
# Populate the YCSB database.

CONF_DIR=../gen-conf/
CONF=${CONF:-${CONF_DIR}/conf-datacenter.json}

RKEYS=${RKEYS:-1000000}

go build && ./tulip-retwis -conf  $CONF \
						   -rkeys $RKEYS \
						   -populate
