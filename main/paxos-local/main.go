package main

import (
	"os"
	"fmt"
	"bufio"
	"github.com/mit-pdos/gokv/grove_ffi"
	"github.com/mit-pdos/tulip/paxos"
)

func main() {
	addrm := make(map[uint64]grove_ffi.Address)

	addrm[0] = grove_ffi.MakeAddress("127.0.0.1:49978")
	addrm[1] = grove_ffi.MakeAddress("127.0.0.1:49979")
	addrm[2] = grove_ffi.MakeAddress("127.0.0.1:49980")

	pxs := make(map[uint64]*paxos.Paxos)
	for nid := range(addrm) {
		pxs[nid] = paxos.Start(nid, addrm)
		fmt.Printf("[main] Paxos node %d started.\n", nid)
	}

	var leader uint64 = 0

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Printf("[main] Enter command: submit <value> | lookup <idx>\n> ")

		if !scanner.Scan() {
			break
		}

		input := scanner.Text()

		px := pxs[leader]

		var value string
		_, err := fmt.Sscanf(input, "submit %s", &value)
		if err == nil {
			lsn, term := px.Submit(value)
			if term == 0 {
				fmt.Printf("[main] Fail to submit to %d. Change leader.\n", leader)
				leader = (leader + 1) % 3
				continue
			}

			fmt.Printf("[main] Submit value \"%s\" to %d in term %d @ %d.\n",
				value, leader, term, lsn)
			continue
		}

		var idx uint64
		_, err = fmt.Sscanf(input, "lookup %d", &idx)
		if err == nil {
			for nid, px := range(pxs) {
				v, ok := px.Lookup(idx)
				if ok {
					fmt.Printf("[main] Value @ %d = \"%s\" (on node %d).\n", idx, v, nid)
				} else {
					fmt.Printf("[main] Value @ %d not chosen (on node %d).\n", idx, nid)
				}
			}
			continue
		}

		fmt.Printf("[main] Command not recognized.\n")
	}
}
