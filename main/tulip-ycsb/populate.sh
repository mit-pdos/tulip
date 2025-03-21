#!/bin/bash
#
# Populate the YCSB database.

CONF_DIR=../gen-conf/
CONF=${CONF:-${CONF_DIR}/conf-localhost.json}

RKEYS=${${RKEYS}:-1000000}

go build && ./tulip-ycsb -conf     $CONF \
						 -rkeys    $RKEYS
