package main

import (
	"fmt"
	"os"
	"time"
	"math/rand"
	"github.com/goose-lang/primitive"
	"github.com/mit-pdos/gokv/grove_ffi"
	"github.com/mit-pdos/tulip/replica"
	"github.com/mit-pdos/tulip/txn"
)

const N_KEYS uint64 = 1_000_000
const N_CLIENTS uint64 = 2

var done bool = false

type Result struct {
	sid uint64
	n   uint64
	nc  uint64
}

var rchannel = make(chan Result)

func populate(gaddrm map[uint64]map[uint64]grove_ffi.Address) {
	txno := txn.MkTxn(0, gaddrm, primitive.NewProph())
	txno.Run(func(txni *txn.Txn) bool {
		for k := uint64(0); k < N_KEYS; k++ {
			txni.Write(fmt.Sprintf("%d", k), "xxx")
		}
		return true
	})
}

func read1write1(sid uint64, gaddrm map[uint64]map[uint64]grove_ffi.Address) {
	txno := txn.MkTxn(sid, gaddrm, primitive.NewProph())
	rd := rand.New(rand.NewSource(int64(sid)))

	var n uint64 = 0
	var nc uint64 = 0

	for !done {
		ok := txno.Run(func(txni *txn.Txn) bool {
			k1 := fmt.Sprintf("%d", rd.Uint64() % N_KEYS)
			k2 := fmt.Sprintf("%d", rd.Uint64() % N_KEYS)
			_, readok := txni.Read(k1)
			txni.Write(k2, "yyy")
			// return true
			return readok
		})
		n++
		if ok {
			nc++
		}
	}
	fmt.Printf("hello")

	res := Result{
		sid : sid,
		n   : n,
		nc  : nc,
	}
	rchannel <- res
}

func main() {
	// Initialize group address map for replica.
	gaddrm := make(map[uint64]map[uint64]grove_ffi.Address)
	gaddrm[0] = make(map[uint64]grove_ffi.Address)
	gaddrm[1] = make(map[uint64]grove_ffi.Address)

	gaddrm[0][0] = grove_ffi.MakeAddress("127.0.0.1:49000")
	gaddrm[0][1] = grove_ffi.MakeAddress("127.0.0.1:49010")
	gaddrm[0][2] = grove_ffi.MakeAddress("127.0.0.1:49020")

	gaddrm[1][0] = grove_ffi.MakeAddress("127.0.0.1:49100")
	gaddrm[1][1] = grove_ffi.MakeAddress("127.0.0.1:49110")
	gaddrm[1][2] = grove_ffi.MakeAddress("127.0.0.1:49120")

	// Initialize group address map for paxos.
	gaddrmpx := make(map[uint64]map[uint64]grove_ffi.Address)
	gaddrmpx[0] = make(map[uint64]grove_ffi.Address)
	gaddrmpx[1] = make(map[uint64]grove_ffi.Address)

	gaddrmpx[0][0] = grove_ffi.MakeAddress("127.0.0.1:50000")
	gaddrmpx[0][1] = grove_ffi.MakeAddress("127.0.0.1:50010")
	gaddrmpx[0][2] = grove_ffi.MakeAddress("127.0.0.1:50020")

	gaddrmpx[1][0] = grove_ffi.MakeAddress("127.0.0.1:50100")
	gaddrmpx[1][1] = grove_ffi.MakeAddress("127.0.0.1:50110")
	gaddrmpx[1][2] = grove_ffi.MakeAddress("127.0.0.1:50120")

	// Create replica groups.
	for gid, addrm := range(gaddrm) {
		for rid, addr := range(addrm) {
			// if rid == 2 {
			// 	continue
			// }
			fname := fmt.Sprintf("wal-%d-%d", gid, rid)
			fnamepx := fmt.Sprintf("walpx-%d-%d", gid, rid)
			addrmpx := gaddrmpx[gid]
			replica.Start(rid, addr, fname, addrmpx, fnamepx)
			fmt.Printf("[main] G %d / R %d started.\n", gid, rid)
		}
	}

	// Wait for a few seconds until leaders are elected.
	time.Sleep(time.Duration(3) * time.Second)

	// Populate the database.
	fmt.Printf("Start populating the database.\n")
	pbegin := time.Now()
	populate(gaddrm)
	fmt.Printf("Populating %d keys take %v seconds.\n", N_KEYS, time.Since(pbegin).Seconds())

	// os.Exit(0)

	// Run some simple clients.
	fmt.Printf("Start running some benchmark clients.\n")
	for i := uint64(0); i < N_CLIENTS; i++ {
		go func(sid uint64) {
			read1write1(sid, gaddrm)
		}(i)
	}

	// Wait for 5 seconds.
	time.Sleep(time.Duration(5) * time.Second)
	done = true

	// Print out the results.
	for i := uint64(0); i < N_CLIENTS; i++ {
		res := <-rchannel
		fmt.Printf("[client %d] %d / %d\n", res.sid, res.nc, res.n)
	}

	os.Exit(0)
}
