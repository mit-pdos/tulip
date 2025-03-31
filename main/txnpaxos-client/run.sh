#!/bin/bash
#
# Terminal program to issue user/mod requests.

CONF_DIR=../gen-conf/
CONF=${CONF:-${CONF_DIR}/conf-wide.json}

go build && ./txnpaxos-client $CONF 0
