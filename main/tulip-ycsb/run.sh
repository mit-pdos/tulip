#!/bin/bash
#
# Execute the YCSB benchmark once.

CONF_DIR=../gen-conf/
CONF=${CONF:-${CONF_DIR}/conf-wide.json}

NTHRDS=${NTHRDS:-1}

DURATION=${DURATION:-10}

RDRATIO=${RDRATIO:-100}

NKEYS=${NKEYS:-1}

RKEYS=${RKEYS:-1000000}

SZKEY=${SZKEY:-64}

THETA=${THETA:--1}

go build && ./tulip-ycsb -conf     $CONF \
						 -nthrds   $NTHRDS \
						 -duration $DURATION \
						 -rdratio  $RDRATIO \
						 -nkeys    $NKEYS \
						 -rkeys    $RKEYS \
						 -szkey    $SZKEY \
						 -theta    $THETA
