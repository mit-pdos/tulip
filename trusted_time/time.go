package trusted_time

import "time"

// Assume 1-ms clock skew for @GetTimeSimple.
// const CLOCK_SKEW uint64 = 1_000_000
const CLOCK_SKEW uint64 = 0

func GetTimeSimple() uint64 {
	// Choose the latest possible time as the timestamp.
	ts := uint64(time.Now().UnixNano()) + CLOCK_SKEW

	for {
		// Compute the current earliest possible timestamp.
		earliest := uint64(time.Now().UnixNano()) - CLOCK_SKEW

		// Wait until the earliest possible timestamp surpasses the timestamp.
		if ts < earliest {
			break
		}
	}

	return ts
}

func GetTimePrecise() uint64 {
	// TODO: using AWS PCP
	return 0
}

func GetTime() uint64 {
	return GetTimeSimple()
}
