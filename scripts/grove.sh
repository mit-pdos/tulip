#!/bin/bash

gsed -i -E -f- $(find . -name "*.go" ! -name "netsim.go") <<EOF
s/tulip\/netsim/gokv\/grove_ffi/
s/netsim/grove_ffi/
s/gaddrm\[0\]\[0\] = 0x000/gaddrm\[0\]\[0\] = grove_ffi.MakeAddress\("127.0.0.1:49000"\)/
s/gaddrm\[0\]\[1\] = 0x010/gaddrm\[0\]\[1\] = grove_ffi.MakeAddress\("127.0.0.1:49010"\)/
s/gaddrm\[0\]\[2\] = 0x020/gaddrm\[0\]\[2\] = grove_ffi.MakeAddress\("127.0.0.1:49020"\)/
s/gaddrm\[1\]\[0\] = 0x100/gaddrm\[1\]\[0\] = grove_ffi.MakeAddress\("127.0.0.1:49100"\)/
s/gaddrm\[1\]\[1\] = 0x110/gaddrm\[1\]\[1\] = grove_ffi.MakeAddress\("127.0.0.1:49110"\)/
s/gaddrm\[1\]\[2\] = 0x120/gaddrm\[1\]\[2\] = grove_ffi.MakeAddress\("127.0.0.1:49120"\)/
s/gaddrmpx\[0\]\[0\] = 0x001/gaddrmpx\[0\]\[0\] = grove_ffi.MakeAddress\("127.0.0.1:50000"\)/
s/gaddrmpx\[0\]\[1\] = 0x011/gaddrmpx\[0\]\[1\] = grove_ffi.MakeAddress\("127.0.0.1:50010"\)/
s/gaddrmpx\[0\]\[2\] = 0x021/gaddrmpx\[0\]\[2\] = grove_ffi.MakeAddress\("127.0.0.1:50020"\)/
s/gaddrmpx\[1\]\[0\] = 0x101/gaddrmpx\[1\]\[0\] = grove_ffi.MakeAddress\("127.0.0.1:50100"\)/
s/gaddrmpx\[1\]\[1\] = 0x111/gaddrmpx\[1\]\[1\] = grove_ffi.MakeAddress\("127.0.0.1:50110"\)/
s/gaddrmpx\[1\]\[2\] = 0x121/gaddrmpx\[1\]\[2\] = grove_ffi.MakeAddress\("127.0.0.1:50120"\)/
EOF
