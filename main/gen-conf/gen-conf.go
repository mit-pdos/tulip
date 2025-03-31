package main

import (
	"os"
	"fmt"
	"encoding/json"
	"strconv"
)

type TulipConf struct {
	ReplicaAddressMap map[uint64]map[uint64]string
	PaxosAddressMap map[uint64]map[uint64]string
}

func ipport2str(ip string, port int) string {
	return fmt.Sprintf("%s:%d", ip, port)
}

func main() {
	// TODO: Parametrize the number of replicas
	if len(os.Args) < 7 {
		fmt.Printf("usage: ./gen-conf <ngroups> <address 0> <address 1> <address 2> <address 3> <address 4>\n")
		os.Exit(1)
	}

	ngroups, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Printf("Unable to parse the ngroups fields\n")
		os.Exit(1)
	}

	addr0 := os.Args[2]
	addr1 := os.Args[3]
	addr2 := os.Args[4]
	addr3 := os.Args[5]
	addr4 := os.Args[6]

	conf := TulipConf{
		ReplicaAddressMap : make(map[uint64]map[uint64]string),
		PaxosAddressMap : make(map[uint64]map[uint64]string),
	}

	portrp := 49800
	for g := 0; g < ngroups; g++ {
		addrmrp := make(map[uint64]string)
		addrmrp[0] = ipport2str(addr0, portrp + g * 10)
		addrmrp[1] = ipport2str(addr1, portrp + g * 10)
		addrmrp[2] = ipport2str(addr2, portrp + g * 10)
		addrmrp[3] = ipport2str(addr3, portrp + g * 10)
		addrmrp[4] = ipport2str(addr4, portrp + g * 10)
		conf.ReplicaAddressMap[uint64(g)] = addrmrp
	}

	portpx := 49900
	for g := 0; g < ngroups; g++ {
		addrmpx := make(map[uint64]string)
		addrmpx[0] = ipport2str(addr0, portpx + g * 10)
		addrmpx[1] = ipport2str(addr1, portpx + g * 10)
		addrmpx[2] = ipport2str(addr2, portpx + g * 10)
		addrmpx[3] = ipport2str(addr3, portpx + g * 10)
		addrmpx[4] = ipport2str(addr4, portpx + g * 10)
		conf.PaxosAddressMap[uint64(g)] = addrmpx
	}

	data, errjson := json.MarshalIndent(conf, "", "  ")
	if errjson != nil {
		fmt.Println("JSON encoding error:", err)
		os.Exit(2)
	}

	fmt.Println(string(data))
}
