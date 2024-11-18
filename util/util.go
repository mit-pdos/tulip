package util

import (
	"github.com/goose-lang/std"
	"github.com/tchajed/marshal"
)

func NextAligned(current, interval, low uint64) uint64 {
	var delta uint64

	rem := current % interval
	if rem < low {
		delta = low - rem
	} else {
		delta = interval + low - rem
	}

	return std.SumAssumeNoOverflow(current, delta)
}

func swap(ns []uint64, i, j uint64) {
	tmp := ns[i]
	ns[i] = ns[j]
	ns[j] = tmp
}

func Sort(ns []uint64) {
	// NB: Follow the proof of wrbuf.sortEntsByKey
	var i uint64 = 1
	for i < uint64(len(ns)) {
		var j uint64 = i
		for j > 0 {
			if ns[j - 1] <= ns[j] {
				break
			}
			swap(ns, j - 1, j)
			j--
		}
		i++
	}
}

func EncodeString(bs []byte, str string) []byte {
	var data = bs

	data = marshal.WriteInt(data, uint64(len(str)))
	data = marshal.WriteBytes(data, []byte(str))

	return data
}


func EncodeStrings(bs []byte, strs []string) []byte {
	var data = bs

	data = marshal.WriteInt(data, uint64(len(strs)))
	for _, s := range(strs) {
		data = EncodeString(data, s)
	}

	return data
}

func DecodeString(bs []byte) (string, []byte) {
	var data = bs

	sz, data := marshal.ReadInt(data)
	bsr, data := marshal.ReadBytes(data, sz)

	return string(bsr), data
}

func DecodeStrings(bs []byte) ([]string, []byte) {
	var data = bs

	n, data := marshal.ReadInt(data)
	var ents = make([]string, 0, n)

	var i uint64 = 0
	var s string
	for i < n {
		s, data = DecodeString(data)
		ents = append(ents, s)
		i++
	}

	return ents, data
}
