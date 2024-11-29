package netsim

import (
	// "fmt"
	"sync"
	"time"
	"github.com/mit-pdos/gokv/grove_ffi"
)

type Address = uint64

type NetSim struct {
	mu sync.Mutex
	lm map[Address]Listener
}

var net = NetSim{ lm : make(map[Address]Listener) }

const DELAY_G0_R0 uint64 = 1_000_000
const DELAY_G0_R1 uint64 = 10_000_000
const DELAY_G0_R2 uint64 = 50_000_000
const DELAY_G1_R0 uint64 = 1_000_000
const DELAY_G1_R1 uint64 = 100_000_000
const DELAY_G1_R2 uint64 = 150_000_000

// Address naming scheme: GID / RID / paxos (0) or replica (1)
// Simulated network delay in ns.
var delaym = map[uint64]uint64{
	// G0 R0
	0x000 : DELAY_G0_R0,
	0x001 : DELAY_G0_R0,
	// G0 R1
	0x010 : DELAY_G0_R1,
	0x011 : DELAY_G0_R1,
	// G0 R2
	0x020 : DELAY_G0_R2,
	0x021 : DELAY_G0_R2,
	// G1 R0
	0x100 : DELAY_G1_R0,
	0x101 : DELAY_G1_R0,
	// G1 R1
	0x110 : DELAY_G1_R1,
	0x111 : DELAY_G1_R1,
	// G1 R2
	0x120 : DELAY_G1_R2,
	0x121 : DELAY_G1_R2,
}

func MakeAddress(ip string) uint64 {
	return grove_ffi.MakeAddress(ip)
}

type Listener struct {
	ch chan Connection
}

type Connection struct {
	chsend chan []byte
	chrecv chan []byte
	delay  uint64
}

type ConnectRet struct {
	Err        bool
	Connection Connection
}

func Listen(host Address) Listener {
	l := Listener{ ch : make(chan Connection, 10) }

	net.mu.Lock()
	net.lm[host] = l
	net.mu.Unlock()

	return l
}

func Accept(l Listener) Connection {
	conn := <-l.ch

	return conn
}

func Connect(host Address) ConnectRet {
	net.mu.Lock()
	l, ok := net.lm[host]
	net.mu.Unlock()

	if !ok {
		return ConnectRet{ Err : true }
	}

	delay := delaym[host]

	chx := make(chan []byte, 1000)
	chy := make(chan []byte, 1000)

	connl := Connection{
		chsend : chx,
		chrecv : chy,
		delay : delay,
	}
	l.ch <- connl

	connc := Connection{
		chsend : chy,
		chrecv : chx,
		delay : delay,
	}

	return ConnectRet{ Err : false, Connection : connc }
}

func Send(c Connection, data []byte) bool {
	func() {
		time.Sleep(time.Duration(c.delay) * time.Nanosecond)
		c.chsend <- data
	}()

	return false
}

type ReceiveRet struct {
	Err  bool
	Data []byte
}

func Receive(c Connection) ReceiveRet {
	data := <-c.chrecv
	return ReceiveRet{ Err : false, Data : data }
}

///
/// File system interface.
//

func FileRead(filename string) []byte {
	return grove_ffi.FileRead(filename)
}

func FileAppend(filename string, data []byte) {
	grove_ffi.FileAppend(filename, data)
}
