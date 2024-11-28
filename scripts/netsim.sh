#!/bin/bash

gsed -i -E -f- $(find . -name "*.go" ! -name "netsim.go") <<EOF
s/gokv\/grove_ffi/tulip\/netsim/
s/grove_ffi/netsim/
s/gaddrm\[0\]\[0\] = netsim.MakeAddress\("127.0.0.1:49000"\)/gaddrm\[0\]\[0\] = 0x000/
s/gaddrm\[0\]\[1\] = netsim.MakeAddress\("127.0.0.1:49010"\)/gaddrm\[0\]\[1\] = 0x010/
s/gaddrm\[0\]\[2\] = netsim.MakeAddress\("127.0.0.1:49020"\)/gaddrm\[0\]\[2\] = 0x020/
s/gaddrm\[1\]\[0\] = netsim.MakeAddress\("127.0.0.1:49100"\)/gaddrm\[1\]\[0\] = 0x100/
s/gaddrm\[1\]\[1\] = netsim.MakeAddress\("127.0.0.1:49110"\)/gaddrm\[1\]\[1\] = 0x110/
s/gaddrm\[1\]\[2\] = netsim.MakeAddress\("127.0.0.1:49120"\)/gaddrm\[1\]\[2\] = 0x120/
s/gaddrmpx\[0\]\[0\] = netsim.MakeAddress\("127.0.0.1:50000"\)/gaddrmpx\[0\]\[0\] = 0x001/
s/gaddrmpx\[0\]\[1\] = netsim.MakeAddress\("127.0.0.1:50010"\)/gaddrmpx\[0\]\[1\] = 0x011/
s/gaddrmpx\[0\]\[2\] = netsim.MakeAddress\("127.0.0.1:50020"\)/gaddrmpx\[0\]\[2\] = 0x021/
s/gaddrmpx\[1\]\[0\] = netsim.MakeAddress\("127.0.0.1:50100"\)/gaddrmpx\[1\]\[0\] = 0x101/
s/gaddrmpx\[1\]\[1\] = netsim.MakeAddress\("127.0.0.1:50110"\)/gaddrmpx\[1\]\[1\] = 0x111/
s/gaddrmpx\[1\]\[2\] = netsim.MakeAddress\("127.0.0.1:50120"\)/gaddrmpx\[1\]\[2\] = 0x121/
EOF
