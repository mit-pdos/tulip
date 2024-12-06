package txnpaxos

// NOTE: This file contains unverified code. It's merely used as a comparison
// point for the evaluation.

import (
	"fmt"
	"sync"
	// "time"
	"github.com/goose-lang/primitive"
	"github.com/goose-lang/std"
	"github.com/mit-pdos/gokv/grove_ffi"
	"github.com/tchajed/marshal"
	"github.com/mit-pdos/tulip/tulip"
	"github.com/mit-pdos/tulip/params"
	"github.com/mit-pdos/tulip/util"
	"github.com/mit-pdos/tulip/quorum"
	"github.com/mit-pdos/tulip/index"
	"github.com/mit-pdos/tulip/paxos"
	"github.com/mit-pdos/tulip/trusted_proph"
	"github.com/mit-pdos/tulip/trusted_time"
)

///
/// Txn.
///

type Txn struct {
	// Timestamp site ID.
	sid     uint64
	// Timestamp of this transaction.
	ts      uint64
	// Buffered write set.
	wrs     map[uint64]map[string]tulip.Value
	// Buffered write set for prophecy resolution. TODO: this exists to simplify
	// the proof, remove it after proving the stronger resolution spec.
	wrsp    map[string]tulip.Value
	// Participant group of this transaction. Initialized in prepare time.
	ptgs    []uint64
	// Group coordinators for performing reads, prepare, abort, and commit.
	gcoords map[uint64]*GroupCoordinator
	// Global prophecy variable (for verification purpose).
	proph   primitive.ProphId
}

func MkTxn(sid uint64, gaddrm map[uint64]map[uint64]grove_ffi.Address) *Txn {
	return mkTxn(sid, gaddrm, primitive.NewProph())
}

func mkTxn(sid uint64, gaddrm map[uint64]map[uint64]grove_ffi.Address, proph primitive.ProphId) *Txn {
	txn := &Txn{ sid : sid, proph : proph }

	wrs := make(map[uint64]map[string]tulip.Value)
	for gid := range(gaddrm) {
		wrs[gid] = make(map[string]tulip.Value)
	}
	txn.wrs = wrs
	txn.wrsp = make(map[string]tulip.Value)

	txn.ptgs = make([]uint64, 0)

	gcoords := make(map[uint64]*GroupCoordinator)
	for gid, addrm := range(gaddrm) {
		gcoords[gid] = GcoordStart(addrm)
	}
	txn.gcoords = gcoords

	return txn
}

func getTimestamp(sid uint64) uint64 {
	ts := trusted_time.GetTime()

	n := params.N_TXN_SITES
	tid := std.SumAssumeNoOverflow(ts, n) / n * n + sid

	for trusted_time.GetTime() <= tid {
	}

	return tid
}

func (txn *Txn) begin() {
	txn.ts = getTimestamp(txn.sid)
}

func (txn *Txn) attach() {
	for _, gcoord := range(txn.gcoords) {
		gcoord.Attach(txn.ts)
	}
}

func (txn *Txn) resetwrs() {
	// Creating a new @wrs is not really necessary, but currently it seems like
	// there's no easy way to reason about modifying a map while iterating over
	// it (which is a defined behavior in Go).
	wrs := make(map[uint64]map[string]tulip.Value)
	for gid := range(txn.wrs) {
		wrs[gid] = make(map[string]tulip.Value)
	}
	txn.wrs = wrs
	txn.wrsp = make(map[string]tulip.Value)
}

func (txn *Txn) setwrs(key string, value tulip.Value) {
	gid := txn.keyToGroup(key)
	pwrs := txn.wrs[gid]
	pwrs[key] = value
	txn.wrsp[key] = value
}

func (txn *Txn) getwrs(key string) (tulip.Value, bool) {
	gid := txn.keyToGroup(key)
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
	wrs := txn.wrs
	for _, gid := range(ptgs) {
		gcoord := txn.gcoords[gid]
		pwrs := wrs[gid]

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
	trusted_proph.ResolveCommit(txn.proph, txn.ts, txn.wrsp)

	ts := txn.ts
	for _, gid := range(txn.ptgs) {
		gcoord := txn.gcoords[gid]

		go func() {
			gcoord.Commit(ts)
		}()
	}

	txn.reset()
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

	txn.reset()
}

func (txn *Txn) cancel() {
	trusted_proph.ResolveAbort(txn.proph, txn.ts)

	txn.reset()
}

func (txn *Txn) keyToGroup(key string) uint64 {
	return uint64(len(key)) % uint64(len(txn.wrs))
}

func (txn *Txn) Read(key string) (tulip.Value, bool) {
	vlocal, hit := txn.getwrs(key)
	if hit {
		return vlocal, true
	}

	gid := txn.keyToGroup(key)
	gcoord := txn.gcoords[gid]
	v, ok := gcoord.Read(txn.ts, key)

	if !ok {
		return tulip.Value{}, false
	}

	trusted_proph.ResolveRead(txn.proph, txn.ts, key)

	return v, true
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
	txn.attach()
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

///
/// Group coordinator.
///

type GroupCoordinator struct {
	// Replica IDs in this group.
	rps       []uint64
	// Replica addresses. Read-only.
	addrm     map[uint64]grove_ffi.Address
	// Mutex protecting fields below.
	mu        *sync.Mutex
	// Condition variable used to notify arrival of responses.
	cv        *sync.Cond
	// Condition variable used to trigger resending message.
	cvrs      *sync.Cond
	// Timestamp of the currently active transaction.
	ts        uint64
	// Index of the replica believed to be the leader of this group.
	idxleader uint64
	// Group reader.
	grd       *GroupReader
	// Group preparer.
	gpp       *GroupPreparer
	// IDs of the finalizing transactions. Using unit as range would suffice.
	tsfinals  map[uint64]bool
	// Connections to replicas.
	conns     map[uint64]grove_ffi.Connection
}

func GcoordStart(addrm map[uint64]grove_ffi.Address) *GroupCoordinator {
	gcoord := mkGroupCoordinator(addrm)

	for ridloop := range(addrm) {
		rid := ridloop

		go func() {
			gcoord.ResponseSession(rid)
		}()
	}

	go func() {
		gcoord.ResendSession()
	}()

	return gcoord
}

func mkGroupCoordinator(addrm map[uint64]grove_ffi.Address) *GroupCoordinator {
	mu := new(sync.Mutex)
	cv := sync.NewCond(mu)
	cvrs := sync.NewCond(mu)
	nrps := uint64(len(addrm))

	var rps = make([]uint64, 0)
	for rid := range(addrm) {
		rps = append(rps, rid)
	}

	gcoord := &GroupCoordinator{
		rps       : rps,
		addrm     : addrm,
		mu        : mu,
		cv        : cv,
		cvrs      : cvrs,
		idxleader : 0,
		grd       : mkGroupReader(nrps),
		gpp       : mkGroupPreparer(nrps),
		tsfinals  : make(map[uint64]bool),
		conns     : make(map[uint64]grove_ffi.Connection),
	}

	return gcoord
}

// Arguments:
// @ts: Timestamp of the transaction performing this read.
// @key: Key to be read.
//
// Return value:
// @value: Value of @key.
//
// @gcoord.Read blocks until the value of @key is determined.
func (gcoord *GroupCoordinator) Read(ts uint64, key string) (tulip.Value, bool) {
	go func() {
		gcoord.ReadSession(ts, key)
	}()

	v, ok := gcoord.WaitUntilValueReady(ts, key)
	return v, ok
}

func (gcoord *GroupCoordinator) ReadSession(ts uint64, key string) {
	var leader = gcoord.GetLeader()
	gcoord.SendRead(leader, ts, key)
	primitive.Sleep(params.NS_RESEND_READ)
	for gcoord.AttachedWith(ts) && !gcoord.ValueReady(key) {
		// Retry with different leaders until success.
		// leader = gcoord.ChangeLeader()
		gcoord.SendRead(leader, ts, key)
		primitive.Sleep(params.NS_RESEND_READ)
	}
}

func (gcoord *GroupCoordinator) WaitUntilValueReady(ts uint64, key string) (tulip.Value, bool) {
	var value tulip.Value
	var valid bool

	gcoord.mu.Lock()

	for {
		if !gcoord.attachedWith(ts) {
			valid = false
			break
		}

		v, ok := gcoord.grd.read(key)
		if ok {
			value = v
			valid = true
			break
		}

		gcoord.cv.Wait()
	}

	gcoord.mu.Unlock()

	return value, valid
}

func (gcoord *GroupCoordinator) ValueReady(key string) bool {
	gcoord.mu.Lock()
	done := gcoord.grd.ready(key)
	gcoord.mu.Unlock()
	return done
}

// Arguments:
// @ts: Transaction timestamp.
//
// Return values:
// @status: Transaction status.
// @valid: If true, the group coordinator is assigned the same timestamp @ts
// throughout the course of @Prepare. @status is meaningful iff @valid is true.
//
// @Prepare blocks until the prepare decision (one of prepared, committed,
// aborted) is made, or the associated timestamp has changed.
func (gcoord *GroupCoordinator) Prepare(ts uint64, ptgs []uint64, pwrs tulip.KVMap) (uint64, bool) {
	go func() {
		gcoord.PrepareSession(ts, ptgs, pwrs)
	}()

	st, valid := gcoord.WaitUntilPrepareDone(ts)
	return st, valid
}

func (gcoord *GroupCoordinator) waitOnResendSignal() {
	gcoord.mu.Lock()
	gcoord.cvrs.Wait()
	gcoord.mu.Unlock()
}

func (gcoord *GroupCoordinator) PrepareSession(ts uint64, ptgs []uint64, pwrs map[string]tulip.Value) {
	var leader = gcoord.GetLeader()
	for {
		act, attached := gcoord.NextPrepareAction(ts)

		if !attached {
			break
		}

		if act == GPP_PREPARE {
			gcoord.SendPrepare(leader, ts, pwrs, ptgs)
		} else if act == GPP_QUERY {
			gcoord.SendQuery(leader, ts)
		}

		gcoord.waitOnResendSignal()
		leader = gcoord.GetLeader()
	}

	// The coordinator is no longer associated with @ts, this could happen only
	// after the prepare decision for @ts on @rid is made. Hence, this session
	// can terminate.
}

func (gcoord *GroupCoordinator) WaitUntilPrepareDone(ts uint64) (uint64, bool) {
	var phase uint64
	var valid bool

	gcoord.mu.Lock()

	for {
		if !gcoord.attachedWith(ts) {
			valid = false
			break
		}

		ready := gcoord.gpp.ready()
		if ready {
			phase = gcoord.gpp.getPhase()
			valid = true
			break
		}

		gcoord.cv.Wait()
	}

	gcoord.mu.Unlock()

	if !valid {
		// TXN_PREPARED here is just a placeholder.
		return tulip.TXN_PREPARED, false
	}

	if phase == GPP_COMMITTED {
		return tulip.TXN_COMMITTED, true
	}

	if phase == GPP_ABORTED {
		return tulip.TXN_ABORTED, true
	}

	return tulip.TXN_PREPARED, true
}

func (gcoord *GroupCoordinator) NextPrepareAction(ts uint64) (uint64, bool) {
	gcoord.mu.Lock()

	if !gcoord.attachedWith(ts) {
		gcoord.mu.Unlock()
		return 0, false
	}

	action := gcoord.gpp.action()

	gcoord.mu.Unlock()

	return action, true
}

func (gcoord *GroupCoordinator) Attach(ts uint64) {
	gcoord.mu.Lock()
	gcoord.ts = ts
	gcoord.grd.reset()
	gcoord.gpp.attach()
	gcoord.mu.Unlock()
}

func (gcoord *GroupCoordinator) attachedWith(ts uint64) bool {
	return gcoord.ts == ts
}

func (gcoord *GroupCoordinator) AttachedWith(ts uint64) bool {
	gcoord.mu.Lock()
	b := gcoord.attachedWith(ts)
	gcoord.mu.Unlock()
	return b
}

func (gcoord *GroupCoordinator) Commit(ts uint64) {
	gcoord.RegisterFinalization(ts)

	var leader = gcoord.GetLeader()
	gcoord.SendCommit(leader, ts)
	primitive.Sleep(params.NS_RESEND_COMMIT)
	for !gcoord.Finalized(ts) {
		// Retry with different leaders until success.
		// leader = gcoord.ChangeLeader()
		gcoord.SendCommit(leader, ts)
		primitive.Sleep(params.NS_RESEND_COMMIT)
	}
}

func (gcoord *GroupCoordinator) Abort(ts uint64) {
	gcoord.RegisterFinalization(ts)

	var leader = gcoord.GetLeader()
	gcoord.SendAbort(leader, ts)
	primitive.Sleep(params.NS_RESEND_ABORT)
	for !gcoord.Finalized(ts) {
		// Retry with different leaders until success.
		// leader = gcoord.ChangeLeader()
		gcoord.SendAbort(leader, ts)
		primitive.Sleep(params.NS_RESEND_ABORT)
	}
}

func (gcoord *GroupCoordinator) RegisterFinalization(ts uint64) {
	gcoord.mu.Lock()
	gcoord.tsfinals[ts] = true
	gcoord.mu.Unlock()
}

func (gcoord *GroupCoordinator) Finalized(ts uint64) bool {
	gcoord.mu.Lock()
	_, ok := gcoord.tsfinals[ts]
	gcoord.mu.Unlock()
	return !ok
}

func (gcoord *GroupCoordinator) processFinalizationResult(ts uint64, res uint64) {
	if res == tulip.REPLICA_WRONG_LEADER {
		return
	}
	delete(gcoord.tsfinals, ts)
}

func (gcoord *GroupCoordinator) changeLeader() uint64 {
	idxleader := (gcoord.idxleader + 1) % uint64(len(gcoord.rps))
	gcoord.idxleader = idxleader
	// fmt.Printf("change leader\n")
	return gcoord.rps[idxleader]
}

func (gcoord *GroupCoordinator) ChangeLeader() uint64 {
	gcoord.mu.Lock()
	leader := gcoord.changeLeader()
	gcoord.mu.Unlock()
	return leader
}

func (gcoord *GroupCoordinator) GetLeader() uint64 {
	gcoord.mu.Lock()
	idxleader := gcoord.idxleader
	gcoord.mu.Unlock()
	return gcoord.rps[idxleader]
}

func (gcoord *GroupCoordinator) ResendSession() {
	for {
		primitive.Sleep(params.NS_RESEND_PREPARE)

		gcoord.cvrs.Broadcast()
	}
}

func (gcoord *GroupCoordinator) ResponseSession(rid uint64) {
	for {
		data, ok := gcoord.Receive(rid)
		if !ok {
			// Try to re-establish a connection on failure.
			primitive.Sleep(params.NS_RECONNECT)
			continue
		}

		msg := DecodeTxnResponse(data)
		kind := msg.Kind
		// fmt.Printf("receive response %v\n", msg)

		gcoord.mu.Lock()

		// Handle commit and abort responses. Note that timestamp check should
		// happen after.
		if kind == MSG_TXN_COMMIT || kind == MSG_TXN_ABORT {
			gcoord.processFinalizationResult(msg.Timestamp, msg.Result)
			gcoord.mu.Unlock()
			continue
		}

		// Ignore this response message if it is not the currently active one.
		if !gcoord.attachedWith(msg.Timestamp) {
			gcoord.mu.Unlock()
			continue
		}

		if kind == MSG_TXN_READ {
			gcoord.grd.processReadResult(msg.Key, msg.Value)
		} else if kind == MSG_TXN_PREPARE {
			resend := gcoord.gpp.processPrepareResult(msg.Result)
			if resend {
				// gcoord.changeLeader()
				gcoord.cvrs.Broadcast()
			}
		} else if kind == MSG_TXN_QUERY {
			gcoord.gpp.processQueryResult(msg.Result)
		}

		// In the current design the coordinator will be notified whenever a new
		// response arrives, and then checks whether the final result (e.g.,
		// prepared, committed, or aborted in the case of preparing) is
		// ready. An optimization would be requiring those @process{X}Result
		// functions to return a bool indicating the final result is ready, and
		// call @gcoord.cv.Signal only on those occasions.
		gcoord.cv.Broadcast()

		gcoord.mu.Unlock()
	}
}

func (gcoord *GroupCoordinator) Send(rid uint64, data []byte) {
	conn, ok := gcoord.GetConnection(rid)
	if !ok {
		gcoord.Connect(rid)
		return
	}

	err := grove_ffi.Send(conn, data)
	if err {
		gcoord.Connect(rid)
	}
}

func (gcoord *GroupCoordinator) Receive(rid uint64) ([]byte, bool) {
	conn, ok := gcoord.GetConnection(rid)
	if !ok {
		gcoord.Connect(rid)
		return nil, false
	}

	ret := grove_ffi.Receive(conn)
	if ret.Err {
		gcoord.Connect(rid)
		return nil, false
	}

	return ret.Data, true
}

func (gcoord *GroupCoordinator) SendRead(rid, ts uint64, key string) {
	data := EncodeTxnReadRequest(ts, key)
	gcoord.Send(rid, data)
}

func (gcoord *GroupCoordinator) SendPrepare(rid, ts uint64, pwrs tulip.KVMap, ptgs []uint64) {
	data := EncodeTxnPrepareRequest(ts, pwrs, ptgs)
	gcoord.Send(rid, data)
}

func (gcoord *GroupCoordinator) SendQuery(rid, ts uint64) {
	data := EncodeTxnQueryRequest(ts)
	gcoord.Send(rid, data)
}

func (gcoord *GroupCoordinator) SendCommit(rid, ts uint64) {
	data := EncodeTxnCommitRequest(ts)
	gcoord.Send(rid, data)
}

func (gcoord *GroupCoordinator) SendAbort(rid, ts uint64) {
	data := EncodeTxnAbortRequest(ts)
	gcoord.Send(rid, data)
}

func (gcoord *GroupCoordinator) GetConnection(rid uint64) (grove_ffi.Connection, bool) {
	gcoord.mu.Lock()
	conn, ok := gcoord.conns[rid]
	gcoord.mu.Unlock()
	return conn, ok
}

func (gcoord *GroupCoordinator) Connect(rid uint64) bool {
	addr := gcoord.addrm[rid]
	ret := grove_ffi.Connect(addr)
	if !ret.Err {
		gcoord.mu.Lock()
		gcoord.conns[rid] = ret.Connection
		gcoord.mu.Unlock()
		return true
	}
	return false
}

///
/// Group reader. Used internally by group coordinator.
///
type GroupReader struct {
	// Number of replicas. Read-only.
	nrps   uint64
	// Cached read set. Exists for performance reason; could have an interface
	// to create a transaction that does not cache reads.
	valuem map[string]tulip.Value
}

func mkGroupReader(nrps uint64) *GroupReader {
	grd := &GroupReader{ nrps : nrps }
	grd.reset()

	return grd
}

func (grd *GroupReader) reset() {
	grd.valuem = make(map[string]tulip.Value)
}

func (grd *GroupReader) read(key string) (tulip.Value, bool) {
	v, ok := grd.valuem[key]
	return v, ok
}

func (grd *GroupReader) ready(key string) bool {
	_, ok := grd.valuem[key]
	return ok
}

func (grd *GroupReader) processReadResult(key string, value tulip.Value) {
	_, final := grd.valuem[key]
	if final {
		// The final value is already determined.
		return
	}

	grd.valuem[key] = value
}

///
/// Group preparer. Used internally by group coordinator.
///
type GroupPreparer struct {
	// Number of replicas. Read-only.
	nrps   uint64
	// Control phase.
	phase  uint64
}

func mkGroupPreparer(nrps uint64) *GroupPreparer {
	gpp := &GroupPreparer{ nrps : nrps }
	gpp.phase = GPP_WAITING

	return gpp
}

func (gpp *GroupPreparer) attach() {
	gpp.phase = GPP_PREPARING
}

// Control phases of group preparer.
const (
	GPP_PREPARING   uint64 = 1
	GPP_WAITING     uint64 = 3
	GPP_PREPARED    uint64 = 4
	GPP_COMMITTED   uint64 = 5
	GPP_ABORTED     uint64 = 6
)

// Actions of group preparer.
const (
	GPP_PREPARE      uint64 = 2
	GPP_QUERY        uint64 = 4
)

// Argument:
// @rid: ID of the replica to which a new action is performed.
//
// Return value:
// @action: Next action to perform.
func (gpp *GroupPreparer) action() uint64 {
	if gpp.in(GPP_PREPARING) {
		return GPP_PREPARE
	}

	return GPP_QUERY
}

func (gpp *GroupPreparer) cquorum(n uint64) bool {
	return quorum.ClassicQuorum(gpp.nrps) <= n
}

func (gpp *GroupPreparer) ready() bool {
	return GPP_PREPARED <= gpp.phase
}

func (gpp *GroupPreparer) tryResign(res uint64) bool {
	if gpp.ready() {
		return true
	}

	if res == tulip.REPLICA_COMMITTED_TXN {
		gpp.phase = GPP_COMMITTED
		return true
	}

	if res == tulip.REPLICA_ABORTED_TXN {
		gpp.phase = GPP_ABORTED
		return true
	}

	if res == tulip.REPLICA_STALE_COORDINATOR {
		// gpp.phase = GPP_WAITING
		return true
	}

	return false
}

func (gpp *GroupPreparer) in(phase uint64) bool {
	return gpp.phase == phase
}

func (gpp *GroupPreparer) processPrepareResult(res uint64) bool {
	// Result is ready or a backup coordinator has become live.
	if gpp.tryResign(res) {
		return false
	}

	// Prepare fails; abort the transaction.
	if res == tulip.REPLICA_FAILED_VALIDATION {
		gpp.phase = GPP_ABORTED
		return false
	}

	// Prepare succeeds; become prepared.
	if res == tulip.REPLICA_OK {
		gpp.phase = GPP_PREPARED
		return false
	}

	// Possibely wrong leader, resend a prepare request to a different leader.
	return true
}

func (gpp *GroupPreparer) processQueryResult(res uint64) {
	// Result is ready or a backup coordinator has become live.
	gpp.tryResign(res)
}

func (gpp *GroupPreparer) getPhase() uint64 {
	return gpp.phase
}

///
/// Transaction messages.
///

type TxnRequest struct {
	Kind              uint64
	Timestamp         uint64
	Key               string
	PartialWrites     []tulip.WriteEntry
	ParticipantGroups []uint64
}

type TxnResponse struct {
	Kind      uint64
	Timestamp uint64
	// ReplicaID uint64
	Result    uint64
	Key       string
	Value     tulip.Value
	PartialWrites tulip.KVMap
}

const (
	MSG_TXN_READ         uint64 = 100
	MSG_TXN_PREPARE      uint64 = 203
	MSG_TXN_QUERY        uint64 = 205
	MSG_TXN_COMMIT       uint64 = 300
	MSG_TXN_ABORT        uint64 = 301
	MSG_DUMP_STATE       uint64 = 10000
	MSG_FORCE_ELECTION   uint64 = 10001
)

func EncodeTxnReadRequest(ts uint64, key string) []byte {
	bs := make([]byte, 0, 32)
	bs1 := marshal.WriteInt(bs, MSG_TXN_READ)
	bs2 := marshal.WriteInt(bs1, ts)
	data := util.EncodeString(bs2, key)
	return data
}

func DecodeTxnReadRequest(bs []byte) TxnRequest {
	ts, bs1 := marshal.ReadInt(bs)
	key, _ := util.DecodeString(bs1)
	return TxnRequest{
		Kind      : MSG_TXN_READ,
		Timestamp : ts,
		Key       : key,
	}
}

func EncodeTxnReadResponse(ts uint64, key string, value tulip.Value) []byte {
	bs := make([]byte, 0, 32)
	bs1 := marshal.WriteInt(bs, MSG_TXN_READ)
	bs2 := marshal.WriteInt(bs1, ts)
	bs3 := util.EncodeString(bs2, key)
	data := util.EncodeValue(bs3, value)
	return data
}

func DecodeTxnReadResponse(bs []byte) TxnResponse {
	ts, bs1 := marshal.ReadInt(bs)
	key, bs2 := util.DecodeString(bs1)
	value, _ := util.DecodeValue(bs2)
	return TxnResponse{
		Kind      : MSG_TXN_READ,
		Timestamp : ts,
		Key       : key,
		Value     : value,
	}
}

func EncodeTxnPrepareRequest(ts uint64, pwrs tulip.KVMap, ptgs []uint64) []byte {
	bs := make([]byte, 0, 64)
	bs1 := marshal.WriteInt(bs, MSG_TXN_PREPARE)
	bs2 := marshal.WriteInt(bs1, ts)
	data := util.EncodeKVMap(bs2, pwrs)
	return data
}

func DecodeTxnPrepareRequest(bs []byte) TxnRequest {
	ts, bs1 := marshal.ReadInt(bs)
	pwrs, _ := util.DecodeKVMapIntoSlice(bs1)
	return TxnRequest{
		Kind          : MSG_TXN_PREPARE,
		Timestamp     : ts,
		PartialWrites : pwrs,
	}
}

func EncodeTxnPrepareResponse(ts, res uint64) []byte {
	bs := make([]byte, 0, 32)
	bs1 := marshal.WriteInt(bs, MSG_TXN_PREPARE)
	bs2 := marshal.WriteInt(bs1, ts)
	data := marshal.WriteInt(bs2, res)
	return data
}

func DecodeTxnPrepareResponse(bs []byte) TxnResponse {
	ts, bs1 := marshal.ReadInt(bs)
	res, _ := marshal.ReadInt(bs1)
	return TxnResponse{
		Kind      : MSG_TXN_PREPARE,
		Timestamp : ts,
		Result    : res,
	}
}

func EncodeTxnQueryRequest(ts uint64) []byte {
	bs := make([]byte, 0, 24)
	bs1 := marshal.WriteInt(bs, MSG_TXN_QUERY)
	data := marshal.WriteInt(bs1, ts)
	return data
}

func DecodeTxnQueryRequest(bs []byte) TxnRequest {
	ts, _ := marshal.ReadInt(bs)
	return TxnRequest{
		Kind          : MSG_TXN_QUERY,
		Timestamp     : ts,
	}
}

func EncodeTxnQueryResponse(ts, res uint64) []byte {
	bs := make([]byte, 0, 24)
	bs1 := marshal.WriteInt(bs, MSG_TXN_QUERY)
	bs2 := marshal.WriteInt(bs1, ts)
	data := marshal.WriteInt(bs2, res)
	return data
}

func DecodeTxnQueryResponse(bs []byte) TxnResponse {
	ts, bs1 := marshal.ReadInt(bs)
	res, _ := marshal.ReadInt(bs1)
	return TxnResponse{
		Kind      : MSG_TXN_QUERY,
		Timestamp : ts,
		Result    : res,
	}
}

func EncodeTxnCommitRequest(ts uint64) []byte {
	bs := make([]byte, 0, 64)
	bs1 := marshal.WriteInt(bs, MSG_TXN_COMMIT)
	data := marshal.WriteInt(bs1, ts)
	return data
}

func DecodeTxnCommitRequest(bs []byte) TxnRequest {
	ts, _ := marshal.ReadInt(bs)
	return TxnRequest{
		Kind          : MSG_TXN_COMMIT,
		Timestamp     : ts,
	}
}

func EncodeDumpStateRequest(gid uint64) []byte {
	bs := make([]byte, 0, 16)
	bs1 := marshal.WriteInt(bs, MSG_DUMP_STATE)
	data := marshal.WriteInt(bs1, gid)
	return data
}

func DecodeDumpStateRequest(bs []byte) TxnRequest {
	gid, _ := marshal.ReadInt(bs)
	return TxnRequest{
		Kind          : MSG_DUMP_STATE,
		Timestamp     : gid,
	}
}

func EncodeForceElectionRequest() []byte {
	bs := make([]byte, 0, 8)
	data := marshal.WriteInt(bs, MSG_FORCE_ELECTION)
	return data
}

func DecodeForceElectionRequest() TxnRequest {
	return TxnRequest{ Kind : MSG_FORCE_ELECTION }
}

func EncodeTxnCommitResponse(ts, res uint64) []byte {
	bs := make([]byte, 0, 24)
	bs1 := marshal.WriteInt(bs, MSG_TXN_COMMIT)
	bs2 := marshal.WriteInt(bs1, ts)
	data := marshal.WriteInt(bs2, res)
	return data
}

func DecodeTxnCommitResponse(bs []byte) TxnResponse {
	ts, bs1 := marshal.ReadInt(bs)
	res, _ := marshal.ReadInt(bs1)
	return TxnResponse{
		Kind      : MSG_TXN_COMMIT,
		Timestamp : ts,
		Result    : res,
	}
}

func EncodeTxnAbortRequest(ts uint64) []byte {
	bs := make([]byte, 0, 16)
	bs1 := marshal.WriteInt(bs, MSG_TXN_ABORT)
	data := marshal.WriteInt(bs1, ts)
	return data
}

func DecodeTxnAbortRequest(bs []byte) TxnRequest {
	ts, _ := marshal.ReadInt(bs)
	return TxnRequest{
		Kind          : MSG_TXN_ABORT,
		Timestamp     : ts,
	}
}

func EncodeTxnAbortResponse(ts, res uint64) []byte {
	bs := make([]byte, 0, 24)
	bs1 := marshal.WriteInt(bs, MSG_TXN_ABORT)
	bs2 := marshal.WriteInt(bs1, ts)
	data := marshal.WriteInt(bs2, res)
	return data
}

func DecodeTxnAbortResponse(bs []byte) TxnResponse {
	ts, bs1 := marshal.ReadInt(bs)
	res, _ := marshal.ReadInt(bs1)
	return TxnResponse{
		Kind      : MSG_TXN_ABORT,
		Timestamp : ts,
		Result    : res,
	}
}

func DecodeTxnRequest(bs []byte) TxnRequest {
	kind, bs1 := marshal.ReadInt(bs)

	if kind == MSG_TXN_READ {
		return DecodeTxnReadRequest(bs1)
	}
	if kind == MSG_TXN_PREPARE {
		return DecodeTxnPrepareRequest(bs1)
	}
	if kind == MSG_TXN_QUERY {
		return DecodeTxnQueryRequest(bs1)
	}
	if kind == MSG_TXN_COMMIT {
		return DecodeTxnCommitRequest(bs1)
	}
	if kind == MSG_TXN_ABORT {
		return DecodeTxnAbortRequest(bs1)
	}
	if kind == MSG_DUMP_STATE {
		return DecodeDumpStateRequest(bs1)
	}
	if kind == MSG_FORCE_ELECTION {
		return DecodeForceElectionRequest()
	}
	return TxnRequest{}
}

func DecodeTxnResponse(bs []byte) TxnResponse {
	kind, bs1 := marshal.ReadInt(bs)

	if kind == MSG_TXN_READ {
		return DecodeTxnReadResponse(bs1)
	}
	if kind == MSG_TXN_PREPARE {
		return DecodeTxnPrepareResponse(bs1)
	}
	if kind == MSG_TXN_QUERY {
		return DecodeTxnQueryResponse(bs1)
	}
	if kind == MSG_TXN_COMMIT {
		return DecodeTxnCommitResponse(bs1)
	}
	if kind == MSG_TXN_ABORT {
		return DecodeTxnAbortResponse(bs1)
	}
	return TxnResponse{}
}

///
/// Transaction log.
///
type Cmd struct {
	// Two kinds of command: commit and abort.
	Kind          uint64
	// Transaction timestamp (used in both).
	Timestamp     uint64
	// Transaction write-set in a certain group (used in prepare).
	PartialWrites []tulip.WriteEntry
	// Key (used in read).
	Key string
}

type TxnLog struct {
	px *paxos.Paxos
}

const (
	TXNLOG_READ    uint64 = 0
	TXNLOG_PREPARE uint64 = 1
	TXNLOG_ABORT   uint64 = 2
	TXNLOG_COMMIT  uint64 = 3
)

func (log *TxnLog) SubmitRead(ts uint64, key string) (uint64, uint64) {
	bs := make([]byte, 0, 64)
	bs1 := marshal.WriteInt(bs, TXNLOG_READ)
	bs2 := marshal.WriteInt(bs1, ts)
	data := util.EncodeString(bs2, key)

	lsn, term := log.px.Submit(string(data))

	return lsn, term
}

func (log *TxnLog) SubmitPrepare(ts uint64, pwrs []tulip.WriteEntry) (uint64, uint64) {
	bs := make([]byte, 0, 64)
	bs1 := marshal.WriteInt(bs, TXNLOG_PREPARE)
	bs2 := marshal.WriteInt(bs1, ts)
	data := util.EncodeKVMapFromSlice(bs2, pwrs)

	lsn, term := log.px.Submit(string(data))

	return lsn, term
}

// Arguments:
// @ts: Transaction timestamp.
//
// Return values:
// @lsn (uint64): @lsn = 0 indicates failure; otherwise, this indicates the
// logical index this command is supposed to be placed at.
//
// @term (uint64): The @term this command is supposed to have.
//
// Notes:
// 1) Passing @lsn and @term to @WaitUntilSafe allows us to determine whether
// the command we just submitted actually get safely replicated (and wait until
// that happens up to some timeout).
//
// 2) Although @term is redundant in that we can always detect failure by
// comparing the content of the command to some application state (e.g., a reply
// table), it can be seen as a performance optimization that would allow us to
// know earlier that our command has not been safely replicated (and hence
// resubmit). Another upside of having it would be that this allows the check to
// be done in a general way, without relying on the content.
func (log *TxnLog) SubmitCommit(ts uint64) (uint64, uint64) {
	bs := make([]byte, 0, 16)
	bs1 := marshal.WriteInt(bs, TXNLOG_COMMIT)
	data := marshal.WriteInt(bs1, ts)

	lsn, term := log.px.Submit(string(data))

	return lsn, term
}

// Arguments and return values: see description of @SubmitPrepare.
func (log *TxnLog) SubmitAbort(ts uint64) (uint64, uint64) {
	bs := make([]byte, 0, 16)
	bs1 := marshal.WriteInt(bs, TXNLOG_ABORT)
	data := marshal.WriteInt(bs1, ts)

	lsn, term := log.px.Submit(string(data))

	return lsn, term
}

// Arguments:
// @lsn: LSN of the command whose replication to wait for.
//
// @term: Expected term of the command.
//
// Return values:
// @replicated (bool): If @true, then the command at @lsn has term @term;
// otherwise, we know nothing, but the upper layer is suggested to resubmit the
// command.
//
// TODO: maybe this is a bad interface since now the users would have to make
// another call.
func (log *TxnLog) WaitUntilSafe(lsn uint64, term uint64) bool {
	return log.px.WaitUntilSafe(lsn, term)
}

// Argument:
// @lsn: Logical index of the queried command.
func (log *TxnLog) Lookup(lsn uint64) (Cmd, bool) {
	s, ok := log.px.Lookup(lsn)
	if !ok {
		return Cmd{}, false
	}

	bs := []byte(s)
	kind, bs1 := marshal.ReadInt(bs)

	if kind == TXNLOG_READ {
		ts, bs2 := marshal.ReadInt(bs1)
		key, _ := util.DecodeString(bs2)
		cmd := Cmd{
			Kind      : TXNLOG_READ,
			Timestamp : ts,
			Key       : key,
		}
		return cmd, true
	}
	if kind == TXNLOG_PREPARE {
		ts, bs2 := marshal.ReadInt(bs1)
		pwrs, _ := util.DecodeKVMapIntoSlice(bs2)
		cmd := Cmd{
			Kind          : TXNLOG_PREPARE,
			Timestamp     : ts,
			PartialWrites : pwrs,
		}
		return cmd, true
	}
	if kind == TXNLOG_COMMIT {
		ts, _ := marshal.ReadInt(bs1)
		cmd := Cmd{
			Kind      : TXNLOG_COMMIT,
			Timestamp : ts,
		}
		return cmd, true
	}
	if kind == TXNLOG_ABORT {
		ts, _ := marshal.ReadInt(bs1)
		cmd := Cmd{
			Kind      : TXNLOG_ABORT,
			Timestamp : ts,
		}
		return cmd, true
	}

	return Cmd{}, false
}

func (log *TxnLog) DumpState() {
	log.px.DumpState()
}

func (log *TxnLog) ForceElection() {
	log.px.ForceElection()
}

func TxnLogStart(nidme uint64, addrm map[uint64]grove_ffi.Address, fname string) *TxnLog {
	px := paxos.Start(nidme, addrm, fname)
	txnlog := &TxnLog{ px : px }
	return txnlog
}

///
/// Replica.
///
type Replica struct {
	// Mutex.
	mu     *sync.Mutex
	// Condition variable to notify that new commands are applied.
	cv     *sync.Cond
	// Replica ID.
	rid    uint64
	// Address of this replica.
	addr   grove_ffi.Address
	// Name of the write-ahead log file.
	fname  string
	// Replicated transaction log.
	txnlog *TxnLog
	//
	// Fields below are application states.
	//
	// LSN up to which all commands have been applied.
	lsna   uint64
	// Write sets of validated transactions.
	prepm  map[uint64][]tulip.WriteEntry
	// Participant groups of validated transactions.
	ptgsm  map[uint64][]uint64
	// Transaction status table; mapping from transaction timestamps to their
	// commit/abort status.
	txntbl map[uint64]bool
	// Mapping from keys to their prepare timestamps.
	ptsm   map[string]uint64
	// Mapping from keys to their smallest preparable timestamps.
	sptsm  map[string]uint64
	// Index.
	idx    *index.Index
	//
	// Fields below are group info initialized after creation of all replicas.
	//
	// Replicas in the same group. Read-only.
	rps    map[uint64]grove_ffi.Address
	// ID of the replica believed to be the leader of this group. Used to
	// initialize backup coordinators.
	leader uint64
}

// Arguments:
// @ts: Transaction timestamp.
//
// Return values:
// @terminated: Whether txn @ts has terminated (committed or aborted).
func (rp *Replica) terminated(ts uint64) bool {
	_, terminated := rp.txntbl[ts]
	return terminated
}

func (rp *Replica) Terminated(ts uint64) bool {
	rp.mu.Lock()
	terminated := rp.terminated(ts)
	rp.mu.Unlock()
	return terminated
}

// Arguments:
// @ts: Transaction timestamp.
//
// Return values:
// @ok: If @true, this transaction is committed.
func (rp *Replica) Commit(ts uint64) bool {
	// Query the transaction table. Note that if there's an entry for @ts in
	// @txntbl, then transaction @ts can only be committed. That's why we're not
	// even reading the value of entry.
	committed := rp.Terminated(ts)

	if committed {
		return true
	}
	
	lsn, term := rp.txnlog.SubmitCommit(ts)
	if term == 0 {
		return false
	}

	safe := rp.txnlog.WaitUntilSafe(lsn, term)
	if !safe {
		return false
	}

	// We don't really care about the result, since at this point (i.e., after
	// all the successful prepares), commit should never fail.
	return true
}

// Arguments:
// @ts: Transaction timestamp.
//
// Return values:
// @ok: If @true, this transaction is aborted.
func (rp *Replica) Abort(ts uint64) bool {
	// Query the transaction table. Note that if there's an entry for @ts in
	// @txntbl, then transaction @ts can only be aborted. That's why we're not
	// even reading the value of entry.
	aborted := rp.Terminated(ts)

	if aborted {
		return true
	}

	lsn, term := rp.txnlog.SubmitAbort(ts)
	if term == 0 {
		return false
	}

	safe := rp.txnlog.WaitUntilSafe(lsn, term)
	if !safe {
		return false
	}

	// We don't really care about the result, since at this point (i.e., after
	// at least one failed prepares), abort should never fail.
	return true
}

func (rp *Replica) Read(ts uint64, key string) (tulip.Value, bool) {
	terminated := rp.Terminated(ts)

	if terminated {
		return tulip.Value{}, false
	}

	lsn, term := rp.txnlog.SubmitRead(ts, key)
	if term == 0 {
		return tulip.Value{}, false
	}

	safe := rp.txnlog.WaitUntilSafe(lsn, term)
	if !safe {
		return tulip.Value{}, false
	}

	rp.WaitUntilApplied(lsn)

	spts := rp.sptsm[key]

	if spts + 1 < ts {
		return tulip.Value{}, false
	}
	
	// The key has been successfully bumped, so it's safe to read the tuple.
	tpl := rp.idx.GetTuple(key)
	v, _ := tpl.ReadVersion(ts)
	return v.Value, true
}

func (rp *Replica) applyRead(ts uint64, key string) {
	ok := rp.readableKey(ts, key)
	if !ok {
		// Trying to read a tuple that is locked by a lower-timestamp
		// transaction. This read has to fail because the value to be read is
		// undetermined---the prepared transaction might or might not commit.
		return
	}

	// Simply bump the key up to @ts - 1. The actually value will be read in the
	// handler @Read.
	rp.bumpKey(ts, key)
}

func (rp *Replica) acquire(ts uint64, pwrs []tulip.WriteEntry) bool {
	// Check if all keys are writable.
	var pos uint64 = 0
	for pos < uint64(len(pwrs)) {
		ent := pwrs[pos]
		writable := rp.writableKey(ts, ent.Key)
		if !writable {
			break
		}
		pos++
	}

	// Report error if some key cannot be locked.
	if pos < uint64(len(pwrs)) {
		return false
	}

	// Acquire locks for each key.
	for _, ent := range(pwrs) {
		rp.acquireKey(ts, ent.Key)
	}

	return true
}

// Arguments:
// @ts: Transaction timestamp.
// @pwrs: Write set of transaction @ts.
// @ptgs: Participant groups of transaction @ts.
//
// Return values:
// @error: Error code.
func (rp *Replica) applyPrepare(ts uint64, pwrs []tulip.WriteEntry) {
	// Check if the transaction has aborted or committed. If so, returns the
	// status immediately.
	_, final := rp.finalized(ts)
	if final {
		return
	}

	// Check if the replica has already validated this transaction.
	_, validated := rp.prepm[ts]
	if validated {
		return
	}

	// Validate timestamps.
	acquired := rp.acquire(ts, pwrs)
	if !acquired {
		// Abort the transaction immediately if prepare fails.
		rp.txntbl[ts] = false
		return
	}

	// Record the write set.
	rp.prepm[ts] = pwrs
}

func (rp *Replica) WaitUntilApplied(lsn uint64) {
	rp.mu.Lock()
	for {
		lsna := rp.lsna
		if lsn < lsna {
			rp.mu.Unlock()
			break
		}
		// Wait for 10 us.
		rp.mu.Unlock()
		primitive.Sleep(1 * 10000)
		rp.mu.Lock()
		// rp.cv.Wait()
	}
}

func (rp *Replica) Prepare(ts uint64, pwrs []tulip.WriteEntry) (uint64, bool) {
	res, final := rp.finalized(ts)
	if final {
		return res, true
	}
	
	lsn, term := rp.txnlog.SubmitPrepare(ts, pwrs)
	if term == 0 {
		// fmt.Printf("term == 0\n")
		return tulip.REPLICA_OK, false
	}

	safe := rp.txnlog.WaitUntilSafe(lsn, term)
	if !safe {
		// fmt.Printf("unsafe\n")
		return tulip.REPLICA_OK, false
	}

	rp.WaitUntilApplied(lsn)

	res, final = rp.finalized(ts)
	if final {
		return res, true
	}

	return tulip.REPLICA_OK, true
}

func (rp *Replica) query(ts uint64) uint64 {
	res, final := rp.finalized(ts)
	if final {
		return res
	}

	return tulip.REPLICA_OK
}

func (rp *Replica) Query(ts uint64) uint64 {
	rp.mu.Lock()
	res := rp.query(ts)
	rp.mu.Unlock()
	return res
}

// Keep alive coordinator for @ts at @rank.
func (rp *Replica) refresh(ts uint64, rank uint64) {
	// TODO
}

func (rp *Replica) Refresh(ts uint64, rank uint64) {
	rp.mu.Lock()
	rp.refresh(ts, rank)
	rp.mu.Unlock()
}

func (rp *Replica) multiwrite(ts uint64, pwrs []tulip.WriteEntry) {
	for _, ent := range pwrs {
		key := ent.Key
		value := ent.Value
		tpl := rp.idx.GetTuple(key)
		if value.Present {
			tpl.AppendVersion(ts, value.Content)
		} else {
			tpl.KillVersion(ts)
		}
	}
}

func (rp *Replica) applyCommit(ts uint64) {
	// Query the transaction table. Note that if there's an entry for @ts in
	// @txntbl, then transaction @ts can only be committed. That's why we're not
	// even reading the value of entry.
	committed := rp.terminated(ts)
	if committed {
		return
	}

	pwrs := rp.prepm[ts]
	rp.multiwrite(ts, pwrs)

	rp.txntbl[ts] = true

	rp.release(pwrs)
	delete(rp.prepm, ts)
}

func (rp *Replica) release(pwrs []tulip.WriteEntry) {
	for _, ent := range pwrs {
		key := ent.Key
		rp.releaseKey(key)
	}
}

func (rp *Replica) applyAbort(ts uint64) {
	// Query the transaction table. Note that if there's an entry for @ts in
	// @txntbl, then transaction @ts can only be aborted. That's why we're not
	// even reading the value of entry.
	aborted := rp.terminated(ts)
	if aborted {
		return
	}

	rp.txntbl[ts] = false

	// Tuples lock are held iff @prepm[ts] contains something (and so we should
	// release them by calling @abort).
	pwrs, prepared := rp.prepm[ts]

	if prepared {
		rp.release(pwrs)
		delete(rp.prepm, ts)
	}
}

func (rp *Replica) apply(cmd Cmd) {
	if cmd.Kind == TXNLOG_READ {
		rp.applyRead(cmd.Timestamp, cmd.Key)
	} else if cmd.Kind == TXNLOG_PREPARE {
		rp.applyPrepare(cmd.Timestamp, cmd.PartialWrites)
	} else if cmd.Kind == TXNLOG_COMMIT {
		// fmt.Printf("[replica %d] Apply commit(%d, %v).\n", rp.rid, cmd.Timestamp, cmd.PartialWrites)
		rp.applyCommit(cmd.Timestamp)
	} else if cmd.Kind == TXNLOG_ABORT {
		// fmt.Printf("[replica %d] Apply abort(%d).\n", rp.rid, cmd.Timestamp)
		rp.applyAbort(cmd.Timestamp)
	}
}

func (rp *Replica) Applier() {
	rp.mu.Lock()

	for {
		// TODO: a more efficient interface would return multiple safe commands
		// at once (so as to reduce the frequency of acquiring Paxos mutex).

		// Ghost action: Learn a list of new commands.
		cmd, ok := rp.txnlog.Lookup(rp.lsna)

		if !ok {
			// Sleep for 1 ms.
			rp.mu.Unlock()
			primitive.Sleep(1 * 10000)
			rp.mu.Lock()
			continue
		}

		rp.apply(cmd)

		rp.cv.Broadcast()

		rp.lsna= std.SumAssumeNoOverflow(rp.lsna, 1)
	}
}

func (rp *Replica) writableKey(ts uint64, key string) bool {
	// The default of prepare timestamps are 0, so no need to check existence.
	pts := rp.ptsm[key]
	if pts != 0 {
		return false
	}

	// The default of smallest preparable timestamps are also 0, so no need to
	// check existence.
	spts := rp.sptsm[key]
	if ts <= spts {
		return false
	}

	return true
}

func (rp *Replica) readableKey(ts uint64, key string) bool {
	pts := rp.ptsm[key]

	// Note that for correctness we only need @pts < @ts. However, @pts = @ts
	// implies that @ts has already prepared, and hence this read request must
	// be outdated.
	if pts != 0 && pts <= ts {
		return false
	}

	return true
}

func (rp *Replica) acquireKey(ts uint64, key string) {
	rp.ptsm[key]  = ts
	rp.sptsm[key] = ts
}

func (rp *Replica) releaseKey(key string) {
	delete(rp.ptsm, key)
}

func (rp *Replica) bumpKey(ts uint64, key string) bool {
	spts := rp.sptsm[key]
	if ts - 1 <= spts {
		return false
	}
	rp.sptsm[key] = ts - 1
	return true
}

func (rp *Replica) finalized(ts uint64) (uint64, bool) {
	cmted, done := rp.txntbl[ts]
	if done {
		if cmted {
			return tulip.REPLICA_COMMITTED_TXN, true
		} else {
			return tulip.REPLICA_ABORTED_TXN, true
		}
	}

	// @tulip.REPLICA_OK is a placeholder.
	return tulip.REPLICA_OK, false
}

//
// For debugging and evaluation purpose.
//
func (rp *Replica) DumpState(gid uint64) {
	rp.mu.Lock()
	fmt.Printf("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n")
	fmt.Printf("[G %d / R %d] Dumping replica state:\n", gid, rp.rid)
	fmt.Printf("-------------------------------------------------------------\n")
	fmt.Printf("Number of finalized txns: %d\n", uint64(len(rp.txntbl)))
	fmt.Printf("Number of prepared txns: %d\n", uint64(len(rp.prepm)))
	fmt.Printf("Number of acquired keys: %d\n", uint64(len(rp.ptsm)))
	fmt.Printf("Applied LSN: %d\n\n", rp.lsna)
	fmt.Printf("[G %d / R %d] Dumping paxos state:\n", gid, rp.rid)
	fmt.Printf("-------------------------------------------------------------\n")
	rp.txnlog.DumpState()
	fmt.Printf("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n")
	rp.mu.Unlock()
}

func (rp *Replica) ForceElection() {
	rp.txnlog.ForceElection()
}

///
/// Network.
///

func (rp *Replica) RequestSession(conn grove_ffi.Connection) {
	for {
		ret := grove_ffi.Receive(conn)
		if ret.Err {
			break
		}

		req  := DecodeTxnRequest(ret.Data)
		kind := req.Kind
		ts   := req.Timestamp

		if kind == MSG_TXN_READ {
			key := req.Key
			value, ok := rp.Read(ts, key)
			if !ok {
				continue
			}
			data := EncodeTxnReadResponse(ts, key, value)
			grove_ffi.Send(conn, data)
		} else if kind == MSG_TXN_PREPARE {
			pwrs := req.PartialWrites
			res, ok := rp.Prepare(ts, pwrs)
			if ok {
				data := EncodeTxnPrepareResponse(ts, res)
				grove_ffi.Send(conn, data)
			} else {
				data := EncodeTxnPrepareResponse(ts, tulip.REPLICA_WRONG_LEADER)
				grove_ffi.Send(conn, data)
			}
		} else if kind == MSG_TXN_QUERY {
			res := rp.Query(ts)
			data := EncodeTxnQueryResponse(ts, res)
			grove_ffi.Send(conn, data)
		} else if kind == MSG_TXN_COMMIT {
			ok := rp.Commit(ts)
			if ok {
				data := EncodeTxnCommitResponse(ts, tulip.REPLICA_COMMITTED_TXN)
				grove_ffi.Send(conn, data)
			} else {
				data := EncodeTxnCommitResponse(ts, tulip.REPLICA_WRONG_LEADER)
				grove_ffi.Send(conn, data)
			}
		} else if kind == MSG_TXN_ABORT {
			ok := rp.Abort(ts)
			if ok {
				data := EncodeTxnAbortResponse(ts, tulip.REPLICA_ABORTED_TXN)
				grove_ffi.Send(conn, data)
			} else {
				data := EncodeTxnAbortResponse(ts, tulip.REPLICA_WRONG_LEADER)
				grove_ffi.Send(conn, data)
			}
		} else if kind == MSG_DUMP_STATE {
			gid := req.Timestamp
			rp.DumpState(gid)
		} else if kind == MSG_FORCE_ELECTION {
			rp.ForceElection()
		}
	}
}

func (rp *Replica) Serve() {
	ls := grove_ffi.Listen(rp.addr)
	for {
		conn := grove_ffi.Accept(ls)
		go func() {
			rp.RequestSession(conn)
		}()
	}
}

func Start(rid uint64, addr grove_ffi.Address, fname string, addrmpx map[uint64]uint64, fnamepx string) *Replica {
	txnlog := TxnLogStart(rid, addrmpx, fnamepx)

	mu := new(sync.Mutex)
	cv := sync.NewCond(mu)
	rp := &Replica{
		mu     : mu,
		cv     : cv,
		rid    : rid,
		addr   : addr,
		fname  : fname,
		txnlog : txnlog,
		lsna   : 0,
		prepm  : make(map[uint64][]tulip.WriteEntry),
		ptgsm  : make(map[uint64][]uint64),
		txntbl : make(map[uint64]bool),
		ptsm   : make(map[string]uint64),
		sptsm  : make(map[string]uint64),
		idx    : index.MkIndex(),
	}

	go func() {
		rp.Serve()
	}()

	go func() {
		rp.Applier()
	}()

	go func() {
		// rp.Debug()
	}()

	return rp
}

func (rp *Replica) Debug() {
	for {
		primitive.Sleep(5 * 1000000000)

		rp.DumpState(0)
	}
}
