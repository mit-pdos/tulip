package main

import (
	"os"
	"fmt"
	"bufio"
	"github.com/mit-pdos/gokv/grove_ffi"
	"github.com/mit-pdos/tulip/paxos"
	"encoding/json"
)

type PaxosConf struct {
	NodeID     uint64
	AddressMap map[uint64]string
}

func main() {
	if len(os.Args[1]) < 2 {
		fmt.Printf("usage: ./paxos-node <conf.json>")
		os.Exit(1)
	}

	bs, err := os.ReadFile(os.Args[1])
    if err != nil {
        fmt.Println("File read error:", err)
		os.Exit(1)
    }

	var conf PaxosConf
	errjson := json.Unmarshal(bs, &conf)
	if errjson != nil {
		fmt.Println("JSON decoding error:", err)
		os.Exit(2)
	}

	addrm := make(map[uint64]grove_ffi.Address)

	for nid, ipaddr := range(conf.AddressMap) {
		addrm[nid] = grove_ffi.MakeAddress(ipaddr)
	}

	px := paxos.Start(conf.NodeID, addrm)

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Printf("[main] Enter command: submit <value> | lookup <idx>\n> ")

		if !scanner.Scan() {
			break
		}

		input := scanner.Text()

		var value string
		_, err := fmt.Sscanf(input, "submit %s", &value)
		if err == nil {
			lsn, term := px.Submit(value)
			if term == 0 {
				fmt.Printf("[main] Fail to submit the command (not a leader).\n")
				continue
			}

			fmt.Printf("[main] Submit value \"%s\" in term %d @ %d.\n",
				value, term, lsn)
			continue
		}

		var idx uint64
		_, err = fmt.Sscanf(input, "lookup %d", &idx)
		if err == nil {
			v, ok := px.Lookup(idx)
			if ok {
				fmt.Printf("[main] Value @ %d = \"%s\".\n", idx, v)
			} else {
				fmt.Printf("[main] Value @ %d not chosen.\n", idx)
			}
			continue
		}

		fmt.Printf("[main] Command not recognized.\n")
	}
}
