package tuple

import (
	"sync"
	"github.com/mit-pdos/tulip/tulip"
)

// @owned
// Write lock of this tuple. Acquired before committing.
//
// @tslast
// Timestamp of the last reader or last writer + 1.
//
// @vers
// Versions.
type Tuple struct {
	latch  *sync.Mutex
	owned  bool
	tslast uint64
	vers   []tulip.Version
}

func (tuple *Tuple) Own(tid uint64) bool {
	// TODO
	return false
}

func (tuple *Tuple) ReadVersion(tid uint64) (tulip.Version, bool) {
	// TODO
	// Return false if the abstract history has no value at index @tid.
	return tulip.Version{}, false
}

func (tuple *Tuple) Extend(tid uint64) bool {
	// TODO
	return false
}

func (tuple *Tuple) AppendVersion(tid uint64, val string) {
	// TODO
}

func (tuple *Tuple) KillVersion(tid uint64) {
	// TODO
}

func (tuple *Tuple) Free() {
	// TODO
}
