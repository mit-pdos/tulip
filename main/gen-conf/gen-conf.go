package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

type TulipConf struct {
	ReplicaAddressMap map[uint64]map[uint64]string
	PaxosAddressMap   map[uint64]map[uint64]string
}

func ipport2str(ip string, port int) string {
	return fmt.Sprintf("%s:%d", ip, port)
}

func parsePort(arg string, ngroups int) (int, error) {
	port, err := strconv.Atoi(arg)
	// Every group needs a usable port, including the offset from the base port.
	if err != nil || port < 1 || port > 65535 {
		return 0, fmt.Errorf("invalid port %q: expected an integer between 1 and 65535", arg)
	}
	if ngroups-1 > (65535-port)/10 {
		return 0, fmt.Errorf("port %d with %d groups exceeds port 65535", port, ngroups)
	}
	return port, nil
}

func main() {
	// Require at least one replica, with both ports supplied for every address.
	if len(os.Args) < 5 || (len(os.Args)-2)%3 != 0 {
		fmt.Fprintln(os.Stderr,
			"usage: gen-conf <ngroups> <ip> <replica-port> <paxos-port>",
			"[<ip> <replica-port> <paxos-port> ...]")
		os.Exit(1)
	}

	ngroups, err := strconv.Atoi(os.Args[1])
	// An empty group map cannot be used by the servers or clients.
	if err != nil || ngroups < 1 {
		fmt.Fprintln(os.Stderr, "ngroups must be a positive integer")
		os.Exit(1)
	}

	conf := TulipConf{
		ReplicaAddressMap: make(map[uint64]map[uint64]string),
		PaxosAddressMap:   make(map[uint64]map[uint64]string),
	}

	args := os.Args[2:]
	for r := 0; r < len(args)/3; r++ {
		ip := args[3*r]
		portrp, err := parsePort(args[3*r+1], ngroups)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		portpx, err := parsePort(args[3*r+2], ngroups)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		for g := 0; g < ngroups; g++ {
			gid, rid := uint64(g), uint64(r)
			if r == 0 {
				conf.ReplicaAddressMap[gid] = make(map[uint64]string)
				conf.PaxosAddressMap[gid] = make(map[uint64]string)
			}
			conf.ReplicaAddressMap[gid][rid] = ipport2str(ip, portrp+g*10)
			conf.PaxosAddressMap[gid][rid] = ipport2str(ip, portpx+g*10)
		}
	}

	data, err := json.MarshalIndent(conf, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, "JSON encoding error:", err)
		os.Exit(2)
	}

	fmt.Println(string(data))
}
