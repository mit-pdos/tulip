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
