package retwis

import (
	"fmt"
	"testing"
)

func TestTxnProportion(t *testing.T) {
	gen := NewGenerator(0, 1000, 64, 64, -1)
	ns := make([]int, 4)

	niters := 1000000
	for i := 0; i < niters; i++ {
		n := gen.PickTxn()
		ns[n]++
	}

	fmt.Printf("ADD_USER: %f%%\n", float64(ns[TXN_ADD_USER]) / float64(niters) * 100)
	fmt.Printf("FOLLOW: %f%%\n", float64(ns[TXN_FOLLOW]) / float64(niters) * 100)
	fmt.Printf("POST_TWEET: %f%%\n", float64(ns[TXN_POST_TWEET]) / float64(niters) * 100)
	fmt.Printf("GET_TIMELINE: %f%%\n", float64(ns[TXN_GET_TIMELINE]) / float64(niters) * 100)
}
