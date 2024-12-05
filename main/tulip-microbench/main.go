package main

import (
	"os"
	"fmt"
	"strconv"
	"time"
	"github.com/mit-pdos/gokv/grove_ffi"
	"github.com/mit-pdos/tulip/txn"
	// "github.com/mit-pdos/tulip/tulip"
	"github.com/mit-pdos/tulip/params"
	"strings"
	"encoding/json"
	"math/rand"
)

var done bool = false

type Result struct {
	sid uint64
	n   uint64
	nc  uint64
}

var rchannel = make(chan Result)

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

func RandomString(lengthmin, lengthmax int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

	length := rand.Int() % (lengthmax - lengthmin) + lengthmin
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func singleWriteLoop(sid uint64, gaddrm map[uint64]map[uint64]grove_ffi.Address) {
	txno := txn.MkTxn(sid, gaddrm)

	var n uint64 = 0
	var nc uint64 = 0

	for !done {
		key := RandomString(16, 32)
		value := RandomString(16, 32)
		
		body := func(txni *txn.Txn) bool {
			txni.Write(key, value)
			return true
		}

		// begin := time.Now()
		ok := txno.Run(body)
		if ok {
			// d := time.Since(begin).Microseconds()
			nc++
		}
		n++
	}

	res := Result{
		sid : sid,
		n   : n,
		nc  : nc,
	}
	rchannel <- res
}

func scaleWriteClients(gaddrm map[uint64]map[uint64]grove_ffi.Address) {
	var duration uint64 = 10
	var nclis uint64 = 1

	for nclis <= params.N_TXN_SITES {
		done = false

		var sid uint64 = 0
		for sid < nclis {
			go singleWriteLoop(sid, gaddrm)
			sid++
		}

		time.Sleep(time.Duration(duration) * time.Second)

		done = true

		var n uint64 = 0
		var nc uint64 = 0
		for i := uint64(0); i < nclis; i++ {
			res := <-rchannel
			// fmt.Printf("[client %d] %d / %d\n", res.sid, res.nc, res.n)
			n += res.n
			nc += res.nc
		}
		fmt.Printf("# clients = %d ops / s / client : %f (%d / %d)\n",
			nclis, float64(nc) / float64(duration) / float64(nclis), nc, n)

		nclis++
	}
}

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("Usage: ./tulip-microbench <conf.json> <benchmark type>\n")
		fmt.Printf("Benchmark type\n")
		fmt.Printf("0 : scale single-write clients\n")
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

	benchmark, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Printf("Unable to parse the benchmark type\n")
		os.Exit(1)
	}

	gaddrmraw := conf.ReplicaAddressMap

	gaddrm := make(map[uint64]map[uint64]grove_ffi.Address)

	for gid, addrmraw := range(gaddrmraw) {
		addrm := make(map[uint64]grove_ffi.Address)
		for rid, addrraw := range(addrmraw) {
			addr := MakeAddress(addrraw)
			addrm[rid] = addr
		}

		gaddrm[gid] = addrm
	}

	if benchmark == 0 {
		scaleWriteClients(gaddrm)
		return
	}

	fmt.Printf("Unable to recognize benchmark type %d.\n", benchmark)
}
