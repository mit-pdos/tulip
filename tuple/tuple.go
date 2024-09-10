package tuple

import (
	"sync"
	"github.com/mit-pdos/tulip/tulip"
)

// Key invariants:
// (1) @vers[len(vers) - 1].Timestamp < @tslast.
// (2) @tslast <= @tsown, if @tsown != 0.
// (3) length of the abstract history is equal to @tslast.
type Tuple struct {
	// Mutex protecting the fields below.
	mu     *sync.Mutex
	// Timestamp of the current owner of this tuple.
	tsown  uint64
	// Timestamp of the last reader or the last writer + 1.
	tslast uint64
	// List of versions.
	vers   []tulip.Version
}

func (tuple *Tuple) Own(ts uint64) bool {
	tuple.mu.Lock()

	if ts < tuple.tslast {
		tuple.mu.Unlock()
		return false
	}

	if tuple.tsown != 0 {
		tuple.mu.Unlock()
		return false
	}

	tuple.tsown = ts
	tuple.mu.Unlock()
	return true
}

// A note on fast-path reads. With the current design fast-path reads won't be
// very effective, since it only applies when the read timestamp is less than or
// equal to the timestamp of the latest version. This means only readers that
// linearize before (i.e., get assigned a smaller timestamp) some writers would
// benefit from fast-path reads. This is especially bad in, for instance, the
// following scenario: a key is first written once (and only once) and then many
// reads follow; all the reads would have to go through the slow-path. Ideally,
// we want to enable also *readers* to bump the timestamp of the latest version.
//
// We can add such an interface and whenever a transaction successfully performs
// a slow-path read, it sends to those replicas that have the latest version an
// additional message to bump the latest version to the transaction timestamp.
//
// In terms of program and proof changes to support this feature, we might just
// need to add an additional method @Extend(tsver, tstxn) that modifies the
// timestamp of the latest version. The precondition of @Extend(tsver, tstxn)
// would essentially say that "the values of the committed history do not change
// between @tsver and @tstxn", which can be encoded with a lower bound of the
// replicated history, that of the committed history, and some [last_extend]
// relationship between them (obviously persistent resource). If a replica has
// extended its replicated history to @tsver, then it would also be safe to
// extend it to @tstxn (so that the replicated history continues to be a prefix
// of the committed history).

// Arguments:
// @ts: Index at which lookup of the abstract history is performed.
//
// Return values:
// @ver: If @ver.Timestamp = 0, then this is a fast-path read---the value at @ts
// has been determined to be @ver.Value. Otherwise, this is a slow-path read,
// the replica promises not to accept prepare requests from transactions that
// modifies this tuple and whose timestamp lies within @ver.Timestamp and @ts.
//
// @ok: @ver is meaningful iff @ok is true.
func (tuple *Tuple) ReadVersion(ts uint64) (tulip.Version, bool) {
	tuple.mu.Lock()

	// Trying to read a tuple that is locked by a prepared transaction that has
	// a smaller timestamp. This read has to fail because the value to be read
	// is undetermined---the prepared transaction might or might not commit.
	if tuple.tsown != 0 && tuple.tsown <= ts {
		return tulip.Version{}, false
	}

	ver, slow := findVersion(ts, tuple.vers)

	if !slow {
		// Fast-path read: the final value is determined.
		verfast := tulip.Version{
			Timestamp : 0,
			Value     : ver.Value,
		}
		tuple.mu.Unlock()
		return verfast, true
	}

	// Slow-path read: bump @tslast and return the latest version.
	tuple.tslast = ts
	tuple.mu.Unlock()
	return ver, true
}

// @findVersion starts from the end of @vers and return the first version whose
// timestamp is less than or equal to @ts, and whether the returned version is
// the latest one.
func findVersion(ts uint64, vers []tulip.Version) (tulip.Version, bool) {
	var ver tulip.Version
	length := uint64(len(vers))
	var idx uint64 = 0
	for idx < length {
		ver = vers[length - idx - 1]
		if ver.Timestamp <= ts {
			break
		}
		idx++
	}

	return ver, (idx == 0)
}

func (tuple *Tuple) AppendVersion(ts uint64, value string) {
	tuple.mu.Lock()

	// Create a new version and add it to the version chain.
	ver := tulip.Version{
		Timestamp : ts,
		Value : tulip.Value{
			Present : true,
			Content : value,
		},
	}
	tuple.vers = append(tuple.vers, ver)

	// Release the permission to update this tuple.
	tuple.tsown = 0

	tuple.tslast = ts + 1

	tuple.mu.Unlock()
}

func (tuple *Tuple) KillVersion(ts uint64) {
	tuple.mu.Lock()

	// Create a new version and add it to the version chain.
	ver := tulip.Version{
		Timestamp : ts,
		Value : tulip.Value{ Present : false },
	}
	tuple.vers = append(tuple.vers, ver)

	// Release the permission to update this tuple.
	tuple.tsown = 0

	tuple.tslast = ts + 1

	tuple.mu.Unlock()
}

func (tuple *Tuple) Free() {
	tuple.mu.Lock()
	tuple.tsown = 0
	tuple.mu.Unlock()
}

func MkTuple() *Tuple {
	tuple := new(Tuple)
	tuple.mu = new(sync.Mutex)
	tuple.tsown = 0
	tuple.tslast = 1
	tuple.vers = make([]tulip.Version, 1, 1)
	tuple.vers[0] = tulip.Version{
		Timestamp : 0,
		Value     : tulip.Value{ Present : false },
	}
	return tuple
}
