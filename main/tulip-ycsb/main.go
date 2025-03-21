package main

import (
	"flag"
	"os"
	"fmt"
	"strconv"
	"time"
	"github.com/mit-pdos/gokv/grove_ffi"
	"github.com/mit-pdos/tulip/txn"
	"github.com/mit-pdos/tulip/main/ycsb"
	// "github.com/mit-pdos/tulip/tulip"
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

var rchannel = make(chan Result)

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

func populateData(txno *txn.Txn, rkeys uint64) bool {
	var szblk uint64 = 10000
	var k uint64 = 0
	for k < rkeys {
		body := func(txni *txn.Txn) bool {
			var i uint64 = 0
			for k < rkeys && i < szblk {
				s := string(make([]byte, szrec))
				txni.Write(fmt.Sprintf("%d", k), s)
				k++
				i++
			}
			return true
		}
		ok := txno.Run(body)
		if !ok {
			return false
		}
	}
	return true
}

func longReaderBody(txn *txn.Txn, gen *ycsb.Generator) bool {
	for i := 0; i < 10000; i++ {
		key := gen.PickKey()
		txn.Read(fmt.Sprintf("%d", key))
	}
	return true
}

func longReader(txno *txn.Txn, gen *ycsb.Generator) {
	for !done {
		body := func(txni *txn.Txn) bool {
			return longReaderBody(txni, gen)
		}
		txno.Run(body)
	}
}

func workerRWBody(txn *txn.Txn, keys []string, ops []int, buf []byte) bool {
	for i, k := range keys {
		if ops[i] == ycsb.OP_RD {
			txn.Read(k)
		} else if ops[i] == ycsb.OP_WR {
			for j := range buf {
				buf[j] = 'b'
			}
			s := string(buf)
			txn.Write(k, s)
		}
	}
	return true
}

func workerRW(txno *txn.Txn, gen *ycsb.Generator) {
	var nc uint64 = 0
	var n uint64 = 0
	var l uint64 = 0
	nKeys := gen.NKeys()

	keys := make([]string, nKeys)
	ops := make([]int, nKeys)

	buf := make([]byte, szrec)
	for !done {
		for i := 0; i < nKeys; i++ {
			keys[i] = fmt.Sprintf("%d", gen.PickKey())
			ops[i] = gen.PickOp()
		}
		body := func(txn *txn.Txn) bool {
			return workerRWBody(txn, keys, ops, buf)
		}
		begin := time.Now()
		ok := txno.Run(body)
		if !warmup {
			continue
		}
		if ok {
			l += uint64(time.Since(begin).Microseconds())
			nc++
		}
		n++
	}

	r := Result{
		nc : nc,
		n  : n,
		l  : l,
	}
	rchannel <-r
}


func main() {
	var conffile string
	var nthrds int
	var nkeys int
	var rkeys uint64
	var rdratio uint64
	var theta float64
	var long bool
	var duration uint64
	var populate bool
	var exp bool
	flag.StringVar(&conffile, "conf", "conf.json", "location of configuration file")
	flag.IntVar(&nthrds, "nthrds", 1, "number of threads")
	flag.IntVar(&nkeys, "nkeys", 1, "number of keys accessed per txn")
	flag.Uint64Var(&rkeys, "rkeys", 1000, "access keys within [0:rkeys)")
	flag.Uint64Var(&rdratio, "rdratio", 80, "read ratio (200 for scan)")
	flag.Float64Var(&theta, "theta", 0.8, "zipfian theta (the higher the more contended; -1 for uniform)")
	flag.BoolVar(&long, "long", false, "background long-running RO transactions")
	flag.Uint64Var(&duration, "duration", 3, "benchmark duration (seconds)")
	flag.BoolVar(&populate, "populate", false, "populate database")
	flag.BoolVar(&exp, "exp", false, "print only experimental data")
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

	// Prepare for the workload generator.
	var nthrdsro int = 8
	gens := make([]*ycsb.Generator, nthrds + nthrdsro)
	for i := 0; i < nthrds; i++ {
		gens[i] = ycsb.NewGenerator(i, nkeys, rkeys, rdratio, theta)
	}
	for i := 0; i < nthrdsro; i++ {
		gens[i+nthrds] = ycsb.NewGenerator(i+nthrds, nkeys, rkeys, rdratio, theta)
	}

	// Populate the database.
	if populate {
		txno := txn.MkTxn(0, gaddrm)
		populated := populateData(txno, rkeys)
		if !populated {
			fmt.Printf("Unable to populate the database.\n")
			os.Exit(1)
		}
		if !exp {
			fmt.Printf("Database populated.\n")
		}
	}

	time.Sleep(time.Duration(3) * time.Second)

	// Start a long-running reader.
	if long {
		for i := 0; i < nthrdsro; i++ {
			txno := txn.MkTxn(uint64(i), gaddrm)
			go longReader(txno, gens[nthrds + i])
		}
	}

	done = false
	warmup = false
	for i := 0; i < nthrds; i++ {
		txno := txn.MkTxn(uint64(i), gaddrm)
		go workerRW(txno, gens[i])
	}
	// time.Sleep(time.Duration(60) * time.Second)
	warmup = true
	time.Sleep(time.Duration(duration) * time.Second)
	done = true

	var nc uint64 = 0
	var n uint64 = 0
	var l uint64
	for i := 0; i < nthrds; i++ {
		r := <-rchannel
		nc += r.nc
		n += r.n
		l += r.l
	}
	avgl := float64(l) / float64(nc)
	rate := float64(nc) / float64(n)
	tp := float64(nc) / float64(duration) / 1000.0

	if !exp {
		fmt.Printf("average latency = %f (us).\n", avgl)
		fmt.Printf("committed / total = %d / %d (%f).\n", nc, n, rate)
		fmt.Printf("tp = %f (K txns/s).\n", tp)
	}
	fmt.Printf("%d, %d, %d, %d, %.2f, %v, %d, %f, %f, %f\n",
			nthrds, nkeys, rkeys, rdratio, theta, long, duration, avgl, tp, rate)

	// Wait until txn finalizing their work.
	time.Sleep(time.Duration(5) * time.Second)
}
