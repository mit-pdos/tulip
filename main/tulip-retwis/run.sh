#!/bin/bash
#
# Execute the Retwis benchmark once.

CONF_DIR=../gen-conf/
CONF=${CONF:-${CONF_DIR}/conf-datacenter.json}

NTHRDS=${NTHRDS:-1}

DURATION=${DURATION:-3}

RKEYS=${RKEYS:-1000000}

SZKEY=${SZKEY:-64}

THETA=${THETA:--1}

go build && ./tulip-retwis -conf     $CONF \
						   -nthrds   $NTHRDS \
						   -duration $DURATION \
						   -rkeys    $RKEYS \
						   -szkey    $SZKEY \
						   -theta    $THETA
