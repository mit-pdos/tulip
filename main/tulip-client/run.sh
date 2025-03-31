#!/bin/bash
#
# Terminal program to issue user/mod requests.

CONF_DIR=../gen-conf/
CONF=${CONF:-${CONF_DIR}/conf-datacenter.json}

go build && ./tulip-client $CONF 0
