#!/bin/bash

CLOC="cloc --include-lang=Go --quiet"

$CLOC --exclude-dir="main,txnpaxos,netsim" .

echo "Transaction:"
$CLOC ./txn ./gcoord

echo "Backup coordinator:"
$CLOC ./backup

echo "Replica:"
$CLOC ./replica

echo "Transaction log:"
$CLOC ./txnlog

echo "Paxos:"
$CLOC ./paxos

echo "Tuple:"
$CLOC ./tuple ./index

echo "Message:"
$CLOC ./message

echo "Misc:"
$CLOC ./util ./quorum ./params ./tulip ./trusted_proph ./trusted_time
