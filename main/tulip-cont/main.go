package main

import (
	"flag"
	"os"
	"fmt"
	"strconv"
	"time"
	"sync"
	"github.com/mit-pdos/gokv/grove_ffi"
	"github.com/mit-pdos/tulip/txn"
	"strings"
	"encoding/json"
)

var szrec int = 100
var done, warmup bool

type TulipConf struct {
	ReplicaAddressMap map[uint64]map[uint64]string
	PaxosAddressMap map[uint64]map[uint64]string
}

type Result struct {
	n  uint64
	nc uint64
	l  uint64
}

var mu sync.Mutex
var res Result
var counter uint64

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

func workerBody(txn *txn.Txn) bool {
	// txn.Write("hello", "world")
	txn.Write(fmt.Sprintf("%d", counter), "world")
	counter++
	return true
}

func worker(txno *txn.Txn) {
	for !done {
		body := func(txn *txn.Txn) bool {
			return workerBody(txn)
		}
		begin := time.Now()
		ok := txno.Run(body)
		if !warmup {
			continue
		}
		mu.Lock()
		if ok {
			res.l += uint64(time.Since(begin).Microseconds())
			res.nc++
		}
		res.n++
		mu.Unlock()
	}
}


func main() {
	var conffile string
	var duration, step uint64
	flag.StringVar(&conffile, "conf", "conf.json", "location of configuration file")
	flag.Uint64Var(&duration, "duration", 3, "benchmark duration (s)")
	flag.Uint64Var(&step, "step", 1000, "time interval to report result (ms)")
	flag.Parse()

	// Read and decode the configuration file.
	bs, err := os.ReadFile(conffile)
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

	// Prepare for the address map.
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

	done = false
	warmup = false
	txno := txn.MkTxn(uint64(0), gaddrm)
	go worker(txno)
	time.Sleep(time.Duration(1) * time.Second)
	warmup = true

	nsteps := duration * 1000 / step + 1
	fmt.Printf("Time,Latency\n")
	for i := uint64(0); i < nsteps; i++ {
		time.Sleep(time.Duration(step) * time.Millisecond)
		mu.Lock()
		// Compute the average latency.
		var avgl float64
		if res.nc == 0 {
			avgl = -1.0
		} else {
			avgl = float64(res.l) / float64(res.nc)
		}

		// Reset the results.
		res = Result{}
		mu.Unlock()
		fmt.Printf("%.2f,%f\n", float64(i * step) / 1000.0, avgl)
	}
	done = true

	// Wait for txn finalizing their work.
	time.Sleep(time.Duration(5) * time.Second)
}
