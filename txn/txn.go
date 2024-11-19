package txn

import (
	"sync"
	"github.com/goose-lang/primitive"
	"github.com/mit-pdos/tulip/tulip"
	"github.com/mit-pdos/tulip/gcoord"
	"github.com/mit-pdos/tulip/trusted_proph"
)

type Txn struct {
	// Timestamp of this transaction.
	ts      uint64
	// Buffered write set.
	wrs     map[uint64]map[string]tulip.Value
	// Participant group of this transaction. Initialized in prepare time.
	ptgs    []uint64
	// Group coordinators for performing reads, prepare, abort, and commit.
	gcoords map[uint64]*gcoord.GroupCoordinator
	// Global prophecy variable (for verification purpose).
	proph   primitive.ProphId
}

func GetTS() uint64 {
	// TODO
	return 0
}

func (txn *Txn) begin() {
	// TODO
	// Ghost action: Linearize.
	txn.ts = GetTS()
}

func (txn *Txn) resetwrs() {
	wrs := make(map[uint64]map[string]tulip.Value)
	for gid := range(txn.wrs) {
		wrs[gid] = make(map[string]tulip.Value)
	}
	txn.wrs = wrs
}

func (txn *Txn) setwrs(key string, value tulip.Value) {
	gid := KeyToGroup(key)
	pwrs := txn.wrs[gid]
	pwrs[key] = value
}

func (txn *Txn) getwrs(key string) (tulip.Value, bool) {
	gid := KeyToGroup(key)
	pwrs := txn.wrs[gid]
	v, ok := pwrs[key]
	return v, ok
}

func (txn *Txn) resetptgs() {
	txn.ptgs = txn.ptgs[:0]
}

func (txn *Txn) setptgs() {
	var ptgs = txn.ptgs
	for gid, pwrs := range(txn.wrs) {
		if uint64(len(pwrs)) != 0 {
			ptgs = append(ptgs, gid)
		}
	}
	txn.ptgs = ptgs
}

func (txn *Txn) reset() {
	txn.resetwrs()
	txn.resetptgs()
}

func (txn *Txn) prepare() uint64 {
	// Compute the participant groups.
	txn.setptgs()

	// TODO: init the group coordinator

	ts := txn.ts
	ptgs := txn.ptgs

	// An alternative (and more elegant) design would be using a wait-groups, but
	// the CV approach has the advantage of early abort: If the transaction
	// fails to prepare on one of the participant groups (e.g., due to conflict
	// with another transaction), then the CV approach can "short-circuiting" to
	// aborting the entire transaction, whereas the WaitGroup approach would
	// have to wait until all groups reach their own prepare decisions.
	mu := new(sync.Mutex)
	cv := sync.NewCond(mu)
	var np uint64 = 0
	var st uint64 = tulip.TXN_PREPARED

	// Some notes about the concurrency reasoning here:
	//
	// 1. Even though at any point the group coordinators are assigned
	// exclusively to @txn.ts, the fact that it is reused (for performance
	// reason: connection can be established only once for each @Txn object)
	// means that the associated timestamp is not exposed in the representation
	// predicate. Hence, we'll need a fractional RA to remember that the group
	// coordinators are assigned to @txn.ts during the course of @txn.prepare.
	//
	// 2. To establish sufficient proof that @txn.ts can finalize, we need to
	// maintain the following the lock invariant:
	// There exists a set G of group IDs:
	// (a) @st associated with the right txn tokens; for @st = TXN_PREPARED, in
	// particular, all groups in G must have prepared;
	// (b) size(G) = @np;
	// (c) exclusive tokens over G, allowing a coordinator to prove uniqueness
	// when adding its result, and thereby re-esbalish property (b).

	// Try to prepare transaction @tcoord.ts on each group.
	for _, gid := range(ptgs) {
		gcoord := txn.gcoords[gid]
		pwrs := txn.wrs[gid]

		go func() {
			stg, ok := gcoord.Prepare(ts, ptgs, pwrs)

			if ok {
				mu.Lock()
				if stg == tulip.TXN_PREPARED {
					np += 1
				} else {
					st = stg
				}
				mu.Unlock()
				cv.Signal()
			}

			// @ok = false means that the group coordinator has already been
			// assigned to a different transaction, implying nothing is waiting
			// on the CV.
		}()
	}

	mu.Lock()
	// Wait until either status is no longer TXN_PREPARED or all participant
	// groups have responded.
	for st == tulip.TXN_PREPARED && np != uint64(len(ptgs)) {
		cv.Wait()
	}

	status := st
	mu.Unlock()
	
	return status
}

func (txn *Txn) commit() {
	trusted_proph.ResolveCommit(txn.proph, txn.ts, txn.wrs)

	ts := txn.ts
	for _, gid := range(txn.ptgs) {
		gcoord := txn.gcoords[gid]
		pwrs := txn.wrs[gid]

		go func() {
			gcoord.Commit(ts, pwrs)
		}()
	}
}

func (txn *Txn) abort() {
	trusted_proph.ResolveAbort(txn.proph, txn.ts)

	ts := txn.ts
	for _, gid := range(txn.ptgs) {
		gcoord := txn.gcoords[gid]

		go func() {
			gcoord.Abort(ts)
		}()
	}
}

func (txn *Txn) cancel() {
	trusted_proph.ResolveAbort(txn.proph, txn.ts)

	txn.reset()
}

func KeyToGroup(key string) uint64 {
	// TODO
	return 0
}

func (txn *Txn) Read(key string) tulip.Value {
	vlocal, ok := txn.getwrs(key)
	if ok {
		return vlocal
	}

	gid := KeyToGroup(key)
	gcoord := txn.gcoords[gid]
	v := gcoord.Read(txn.ts, key)

	trusted_proph.ResolveRead(txn.proph, txn.ts, key)

	return v
}

func (txn *Txn) Write(key string, value string) {
	v := tulip.Value{
		Present : true,
		Content : value,
	}
	txn.setwrs(key, v)
}

func (txn *Txn) Delete(key string) {
	v := tulip.Value{
		Present : false,
	}
	txn.setwrs(key, v)
}

func (txn *Txn) Run(body func(txn *Txn) bool) bool {
	txn.begin()
	cmt := body(txn)
	if !cmt {
		// This transaction has not really requested to prepare yet, so no
		// cleanup tasks are required.
		txn.cancel()
		return false
	}

	status := txn.prepare()

	if status == tulip.TXN_COMMITTED {
		// A backup coordinator must have committed this transaction, so there's
		// nothing we need to do here.
		txn.reset()
		return true
	}
	
	if status == tulip.TXN_ABORTED {
		// Ghost action: Abort this transaction.
		txn.abort()
		return false
	}

	// Ghost action: Commit this transaction.
	txn.commit()
	return true
}
