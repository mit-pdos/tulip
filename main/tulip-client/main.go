package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mit-pdos/gokv/grove_ffi"
	"github.com/mit-pdos/tulip/message"
	"github.com/mit-pdos/tulip/tulip"
	"github.com/mit-pdos/tulip/txn"
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

func RandomString(lengthmax int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

	length := rand.Int() % lengthmax + 1
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("usage: ./tulip-node <conf.json> <site id>\n")
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

	sidint, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Printf("Unable to parse the replica ID field\n")
		os.Exit(1)
	}
	sid := uint64(sidint)

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

	txno := txn.MkTxn(sid, gaddrm)

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Printf("[main] Enter command: write <key> <value> | delete <key> | read <key> | manywrite <duration> | manyread <duration> | delayread <key> | dump <gid> <rid>\n> ")

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

		_, err = fmt.Sscanf(input, "delayread %s", &key)
		if err == nil {
			body := func(txni *txn.Txn) bool {
				time.Sleep(time.Duration(5) * time.Second)
				begin := time.Now()
				v, b := txni.Read(key)
				d := time.Since(begin).Microseconds()
				tvalue = v
				if tvalue.Present {
					fmt.Printf("[main] Successfully read %s -> %s (%d us).\n", key, tvalue.Content, d)
				} else {
					fmt.Printf("[main] Successfully read %s (deleted key) (%d us).\n", key, d)
				}
				return b
			}

			ok := txno.Run(body)
			if ok {
				// Pass
			} else {
				fmt.Printf("[main] Fail to read %s.\n", key)
			}
			continue
		}

		var duration uint64
		_, err = fmt.Sscanf(input, "manywrite %d", &duration)
		if err == nil {
			begin := time.Now()
			end := begin.Add(time.Duration(duration) * time.Second)
			n := 0
			nc := 0

			for time.Now().Before(end) {
				key = RandomString(16)
				value = RandomString(16)

				body := func(txni *txn.Txn) bool {
					txni.Write(key, value)
					return true
				}

				ok := txno.Run(body)
				if ok {
					nc++
				}
				n++
			}

			fmt.Printf("[main] Executing writes for %d seconds. %f ops/s (%d / %d).\n",
				duration, float64(nc) / float64(duration), nc, n)
			continue
		}

		_, err = fmt.Sscanf(input, "manyread %d", &duration)
		if err == nil {
			begin := time.Now()
			end := begin.Add(time.Duration(duration) * time.Second)
			n := 0
			nc := 0

			for time.Now().Before(end) {
				key = RandomString(16)

				body := func(txni *txn.Txn) bool {
					txni.Read(key)
					return true
				}

				ok := txno.Run(body)
				if ok {
					nc++
				}
				n++
			}

			fmt.Printf("[main] Executing reads for %d seconds. %f ops/s (%d / %d).\n",
				duration, float64(nc) / float64(duration), nc, n)
			continue
		}

		var gid, rid uint64
		_, err = fmt.Sscanf(input, "dump %d %d", &gid, &rid)
		if err == nil {
			addr := gaddrm[gid][rid]
			ret := grove_ffi.Connect(addr)
			if ret.Err {
				fmt.Printf("[main] Fail to connect to G %d / R %d.\n", gid, rid)
			}
			conn := ret.Connection
			data := message.EncodeDumpStateRequest(gid)
			err := grove_ffi.Send(conn, data)
			if err {
				fmt.Printf("[main] Fail to send dump-state request to G %d / R %d.\n", gid, rid)
			}

			continue
		}

		_, err = fmt.Sscanf(input, "reelect %d %d", &gid, &rid)
		if err == nil {
			addr := gaddrm[gid][rid]
			ret := grove_ffi.Connect(addr)
			if ret.Err {
				fmt.Printf("[main] Fail to connect to G %d / R %d.\n", gid, rid)
			}
			conn := ret.Connection
			data := message.EncodeForceElectionRequest()
			err := grove_ffi.Send(conn, data)
			if err {
				fmt.Printf("[main] Fail to send force-election request to G %d / R %d.\n", gid, rid)
			}

			continue
		}

		fmt.Printf("[main] Command not recognized.\n")
	}
}
