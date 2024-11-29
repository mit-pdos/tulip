package main

import (
	"bufio"
	"fmt"
	"os"
	"time"
	"github.com/goose-lang/primitive"
	"github.com/mit-pdos/gokv/grove_ffi"
	"github.com/mit-pdos/tulip/tulip"
	"github.com/mit-pdos/tulip/replica"
	"github.com/mit-pdos/tulip/txn"
)

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

	// Create a txn client.
	txno := txn.MkTxn(0, gaddrm, primitive.NewProph())

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Printf("[main] Enter command: write <key> <value> | delete <key> | read <key>\n> ")

		if !scanner.Scan() {
			break
		}

		input := scanner.Text()

		var key string
		var value string
		var tvalue tulip.Value

		_, err := fmt.Sscanf(input, "write %s %s", &key, &value)
		if err == nil {
			body := func(txni *txn.Txn) bool {
				txni.Write(key, value)
				return true
			}

			begin := time.Now()
			ok := txno.Run(body)
			if ok {
				d := time.Since(begin).Microseconds()
				fmt.Printf("[main] Successfully write %s -> %s (%d us).\n", key, value, d)
			} else {
				fmt.Printf("[main] Fail to write %s.\n", key)
			}
			continue
		}

		_, err = fmt.Sscanf(input, "delete %s", &key)
		if err == nil {
			body := func(txni *txn.Txn) bool {
				txni.Delete(key)
				return true
			}

			begin := time.Now()
			ok := txno.Run(body)
			if ok {
				d := time.Since(begin).Microseconds()
				fmt.Printf("[main] Successfully delete %s (%d us).\n", key, d)
			} else {
				fmt.Printf("[main] Fail to delete %s.\n", key)
			}
			continue
		}

		_, err = fmt.Sscanf(input, "read %s", &key)
		if err == nil {
			body := func(txni *txn.Txn) bool {
				v, b := txni.Read(key)
				tvalue = v
				return b
			}

			begin := time.Now()
			ok := txno.Run(body)
			if ok {
				d := time.Since(begin).Microseconds()
				if tvalue.Present {
					fmt.Printf("[main] Successfully read %s -> %s (%d us).\n", key, tvalue.Content, d)
				} else {
					fmt.Printf("[main] Successfully read %s (deleted key) (%d us).\n", key, d)
				}
			} else {
				fmt.Printf("[main] Fail to read %s.\n", key)
			}
			continue
		}

		fmt.Printf("[main] Command not recognized.\n")
	}
}
