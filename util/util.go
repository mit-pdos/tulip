package util

import (
	"github.com/goose-lang/std"
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
