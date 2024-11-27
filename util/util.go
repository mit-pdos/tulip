package util

import (
	"github.com/goose-lang/std"
	"github.com/tchajed/marshal"
	"github.com/mit-pdos/tulip/tulip"
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

func CountBoolMap(m map[uint64]bool, b bool) uint64 {
	var n uint64 = 0
	for _, v := range(m) {
		if v == b {
			// TODO: We should be able to remove this by using the fact that @n
			// is always le the size of @m, and that the size of @m fits into
			// uint64. See the proof [wp_MapLen'] in map.v for reference on how
			// to prove the latter.
			n = std.SumAssumeNoOverflow(n, 1)
		}
	}

	return n
}

func EncodeString(bs []byte, str string) []byte {
	bs1 := marshal.WriteInt(bs, uint64(len(str)))
	data := marshal.WriteBytes(bs1, []byte(str))
	return data
}

func EncodeStrings(bs []byte, strs []string) []byte {
	var data = marshal.WriteInt(bs, uint64(len(strs)))

	for _, s := range(strs) {
		data = EncodeString(data, s)
	}

	return data
}

func DecodeString(bs []byte) (string, []byte) {
	sz, bs1 := marshal.ReadInt(bs)
	bsr, data := marshal.ReadBytes(bs1, sz)
	return string(bsr), data
}

func DecodeStrings(bs []byte) ([]string, []byte) {
	n, bs1 := marshal.ReadInt(bs)

	var data = bs1
	var ents = make([]string, 0, n)

	var i uint64 = 0
	for i < n {
		s, bsloop := DecodeString(data)
		ents = append(ents, s)
		data = bsloop
		i++
	}

	return ents, data
}

func EncodeValue(bs []byte, v tulip.Value) []byte {
	if !v.Present {
		data := marshal.WriteBool(bs, false)
		return data
	}

	bs1 := marshal.WriteBool(bs, true)
	data := EncodeString(bs1, v.Content)
	return data
}

func DecodeValue(bs []byte) (tulip.Value, []byte) {
	b, bs1 := marshal.ReadBool(bs)

	if !b {
		v := tulip.Value{
			Present : false,
			Content : "",
		}
		return v, bs1
	}

	s, data := DecodeString(bs1)
	v := tulip.Value{
		Present : true,
		Content : s,
	}
	return v, data
}

func EncodeVersion(bs []byte, x tulip.Version) []byte {
	bs1 := marshal.WriteInt(bs, x.Timestamp)
	data := EncodeValue(bs1, x.Value)
	return data
}

func DecodeVersion(bs []byte) (tulip.Version, []byte) {
	ts, bs1 := marshal.ReadInt(bs)
	v, data := DecodeValue(bs1)
	x := tulip.Version{
		Timestamp : ts,
		Value     : v,
	}
	return x, data
}

func EncodeWriteEntry(bs []byte, x tulip.WriteEntry) []byte {
	bs1 := EncodeString(bs, x.Key)
	data := EncodeValue(bs1, x.Value)
	return data
}

func DecodeWriteEntry(bs []byte) (tulip.WriteEntry, []byte) {
	k, bs1 := DecodeString(bs)
	v, data := DecodeValue(bs1)
	x := tulip.WriteEntry{
		Key   : k,
		Value : v,
	}
	return x, data
}

func EncodeKVMap(bs []byte, m tulip.KVMap) []byte {
	var data = marshal.WriteInt(bs, uint64(len(m)))

	for k, v := range(m) {
		x := tulip.WriteEntry{
			Key   : k,
			Value : v,
		}
		data = EncodeWriteEntry(data, x)
	}

	return data
}

func EncodeKVMapFromSlice(bs []byte, xs []tulip.WriteEntry) []byte {
	var data = marshal.WriteInt(bs, uint64(len(xs)))

	for _, x := range(xs) {
		data = EncodeWriteEntry(data, x)
	}

	return data
}

func DecodeKVMapIntoSlice(bs []byte) ([]tulip.WriteEntry, []byte) {
	n, bs1 := marshal.ReadInt(bs)

	var data = bs1
	var ents = make([]tulip.WriteEntry, 0, n)

	var i uint64 = 0
	for i < n {
		x, bsloop := DecodeWriteEntry(data)
		ents = append(ents, x)
		data = bsloop
		i++
	}

	return ents, data
}
