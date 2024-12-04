package main

// Evalaute the performance of small synchronous append-only operations.

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// Adapted from gokv/grove_ffi/filesys.go

const DataDir = "."
const LenString uint64 = 40

var done bool = false

type Result struct {
	n   uint64
}

var rchannel = make(chan Result)

func panic_if_err(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func FileAppendOpened(f *os.File, data []byte, sync bool) {
	var err error
	f.Write(data)
	if sync {
		err = f.Sync()
	}
	panic_if_err(err)
	// time.Sleep(10 * time.Millisecond)
}

func FileAppend(filename string, data []byte, sync bool) {
	filename = filepath.Join(DataDir, filename)
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	f.Write(data)
	if sync {
		err = f.Sync()
	}
	panic_if_err(err)
	// time.Sleep(10 * time.Millisecond)
	err = f.Close()
	if err != nil {
		panic(err)
	}
}

func appendloop(sync bool) {
	bs := make([]byte, LenString)
	for i := 0; i < len(bs); i++ {
		bs[i] = byte(i)
	}

	var n uint64 = 0
	for !done {
		FileAppend("x.bin", bs, sync)
		n++
	}

	res := Result{ n : n }
	rchannel <- res
}

func appendloopOpened(sync bool) {
	filename := filepath.Join(DataDir, "x.bin")
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}

	bs := make([]byte, LenString)
	for i := 0; i < len(bs); i++ {
		bs[i] = byte(i)
	}

	var n uint64 = 0
	for !done {
		FileAppendOpened(f, bs, sync)
		n++
	}

	err = f.Close()
	if err != nil {
		panic(err)
	}

	res := Result{ n : n }
	rchannel <- res
}

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("Usage: ./eval-append <sync> <duration (s)>\n")
		os.Exit(1)
	}

	syncn, err := strconv.Atoi(os.Args[1])
	if err != nil || !(syncn == 0 || syncn == 1) {
		fmt.Printf("Unable to parse the sync field (please provide value 0 or 1)\n")
		os.Exit(1)
	}
	sync := syncn == 1

	duration, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Printf("Unable to parse the duration field\n")
		os.Exit(1)
	}

	go appendloopOpened(sync)

	time.Sleep(time.Duration(duration) * time.Second)
	done = true

	// Read and print out the results.
	res := <-rchannel
	tpinops := res.n / uint64(duration)
	tpinmb := res.n * LenString / uint64(duration) / 1000000
	fmt.Printf("Throughput (sync = %v): %d ops/s; %d MB/s\n", sync, tpinops, tpinmb)
}
