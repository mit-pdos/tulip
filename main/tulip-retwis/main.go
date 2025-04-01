package main

import (
	"flag"
	"os"
	"fmt"
	"strconv"
	"time"
	"github.com/mit-pdos/gokv/grove_ffi"
	"github.com/mit-pdos/tulip/txn"
	"github.com/mit-pdos/tulip/main/workload/retwis"
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

func populateData(txno *txn.Txn, gen *retwis.Generator) bool {
	var szblk uint64 = 10000
	for gen.HasNextKey() {
		body := func(txni *txn.Txn) bool {
			var i uint64 = 0
			for gen.HasNextKey() && i < szblk {
				k := gen.NextKey()
				v := gen.PickValue()
				txni.Write(k, v)
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

func addUserTxn(txn *txn.Txn, gen *retwis.Generator) bool {
	keys := make([]string, 3)
	for i := 0; i < 3; i++ {
		keys[i] = gen.PickKey()
	}

	_, ok := txn.Read(keys[0])
	if !ok {
		return false
	}

	for i := 0; i < 3; i++ {
		txn.Write(keys[i], keys[i])
	}
	return true
}

func followTxn(txn *txn.Txn, gen *retwis.Generator) bool {
	for i := 0; i < 2; i++ {
		key := gen.PickKey()
		_, ok := txn.Read(key)
		if !ok {
			return false
		}

		txn.Write(key, key)
	}

	return true
}

func postTweetTxn(txn *txn.Txn, gen *retwis.Generator) bool {
	for i := 0; i < 3; i++ {
		key := gen.PickKey()
		_, ok := txn.Read(key)
		if !ok {
			return false
		}

		txn.Write(key, key)
	}

	for i := 0; i < 2; i++ {
		key := gen.PickKey()
		txn.Write(key, key)
	}

	return true
}

func getTimelineTxn(txn *txn.Txn, gen *retwis.Generator) bool {
	n := 1 + gen.RandomInt() % 10

	for i := 0; i < n; i++ {
		key := gen.PickKey()
		_, ok := txn.Read(key)
		if !ok {
			return false
		}
	}

	return true
}

func workerBody(txn *txn.Txn, gen *retwis.Generator) bool {
	txntype := gen.PickTxn()

	if txntype == retwis.TXN_ADD_USER {
		return addUserTxn(txn, gen)
	}

	if txntype == retwis.TXN_FOLLOW {
		return followTxn(txn, gen)
	}

	if txntype == retwis.TXN_POST_TWEET {
		return postTweetTxn(txn, gen)
	}

	if txntype == retwis.TXN_GET_TIMELINE {
		return getTimelineTxn(txn, gen)
	}

	panic("wrong txntype")
}

func worker(txno *txn.Txn, gen *retwis.Generator) {
	var nc uint64 = 0
	var n uint64 = 0
	var l uint64 = 0

	for !done {
		body := func(txn *txn.Txn) bool {
			return workerBody(txn, gen)
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
	var rkeys uint64
	var szkey uint64
	var szvalue uint64
	var theta float64
	var duration uint64
	var populate bool
	var exp bool
	flag.StringVar(&conffile, "conf", "conf.json", "location of configuration file")
	flag.IntVar(&nthrds, "nthrds", 1, "number of threads")
	flag.Uint64Var(&rkeys, "rkeys", 1000, "access keys within [0:rkeys)")
	flag.Uint64Var(&szkey, "szkey", 64, "key size (bytes)")
	flag.Uint64Var(&szvalue, "szvalue", 64, "value size (bytes)")
	flag.Float64Var(&theta, "theta", 0.8, "zipfian theta (the higher the more contended; -1 for uniform)")
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
	gens := make([]*retwis.Generator, nthrds)
	for i := 0; i < nthrds; i++ {
		gens[i] = retwis.NewGenerator(i, rkeys, szkey, szvalue, theta)
	}

	// Populate the database.
	if populate {
		txno := txn.MkTxn(0, gaddrm)
		gen := retwis.NewGenerator(0, rkeys, szkey, szvalue, theta)
		populated := populateData(txno, gen)
		if !populated {
			fmt.Printf("Unable to populate the database.\n")
			os.Exit(1)
		}
		if !exp {
			fmt.Printf("Database populated.\n")
		}
		// Wait for txn finalizing their work.
		time.Sleep(time.Duration(5) * time.Second)
		return
	}

	done = false
	warmup = false
	for i := 0; i < nthrds; i++ {
		txno := txn.MkTxn(uint64(i), gaddrm)
		go worker(txno, gens[i])
	}
	time.Sleep(time.Duration(5) * time.Second)
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
	fmt.Printf("%d, %d, %.2f, %d, %f, %f, %f\n",
		nthrds, rkeys, theta, duration, avgl, tp, rate)

	// Wait for txn finalizing their work.
	time.Sleep(time.Duration(5) * time.Second)
}
