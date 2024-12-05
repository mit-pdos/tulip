package main

import (
	"os"
	"fmt"
	"strconv"
	// "github.com/mit-pdos/gokv/grove_ffi"
	"github.com/mit-pdos/tulip/replica"
	"strings"
	"encoding/json"
)

type TulipConf struct {
	ReplicaAddressMap map[uint64]map[uint64]string
	PaxosAddressMap map[uint64]map[uint64]string
}

func MakeAddress(ipStr string) uint64 {
	// XXX: manually parsing is pretty silly; couldn't figure out how to make
	// this work cleanly net.IP
	ipPort := strings.Split(ipStr, ":")
	if len(ipPort) != 2 {
		panic(fmt.Sprintf("Not ipv4:port %s", ipStr))
	}
	port, err := strconv.ParseUint(ipPort[1], 10, 16)
	if err != nil {
		panic(err)
	}

	ss := strings.Split(ipPort[0], ".")
	if len(ss) != 4 {
		panic(fmt.Sprintf("Not ipv4:port %s", ipStr))
	}
	ip := make([]byte, 4)
	for i, s := range ss {
		a, err := strconv.ParseUint(s, 10, 8)
		if err != nil {
			panic(err)
		}
		ip[i] = byte(a)
	}
	return (uint64(ip[0]) | uint64(ip[1])<<8 | uint64(ip[2])<<16 | uint64(ip[3])<<24 | uint64(port)<<32)
}

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("usage: ./tulip-node <conf.json> <replica id>\n")
		os.Exit(1)
	}

	bs, err := os.ReadFile(os.Args[1])
    if err != nil {
        fmt.Println("File read error:", err)
		os.Exit(1)
    }

	var conf TulipConf
	errjson := json.Unmarshal(bs, &conf)
	if errjson != nil {
		fmt.Println("JSON decoding error:", err)
		os.Exit(1)
	}

	ridint, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Printf("Unable to parse the replica ID field\n")
		os.Exit(1)
	}
	rid := uint64(ridint)

	gaddrmrp := conf.ReplicaAddressMap
	gaddrmpx := conf.PaxosAddressMap

	for gid := range(gaddrmrp) {
		addrraw := gaddrmrp[gid][rid]
		addr := MakeAddress(addrraw)

		addrmpxraw := gaddrmpx[gid]
		addrmpx := make(map[uint64]uint64)
		for ridpeer, addrpx := range(addrmpxraw) {
			addrmpx[ridpeer] = MakeAddress(addrpx)
		}

		fname := fmt.Sprintf("wal-rp-%d-%d", gid, rid)
		fnamepx := fmt.Sprintf("wal-px-%d-%d", gid, rid)
		
		fmt.Printf(
			"Starting G = %d / R = %d @ %s (replica) %s (paxos)\npaxos map = %v\n",
			gid, rid, addrraw, addrmpxraw[rid], addrmpxraw,
		)

		go replica.Start(rid, addr, fname, addrmpx, fnamepx)
	}
}
