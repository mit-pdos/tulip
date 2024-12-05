#!/bin/bash

sed -i -E 's/Listen\(addrme\)/Listen\(addrme \& 0xffff00000000\)/' ./paxos/paxos.go
sed -i -E 's/Listen\(rp.addr\)/Listen\(rp.addr \& 0xffff00000000\)/' ./replica/replica.go
