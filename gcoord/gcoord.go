package gcoord

import (
	"fmt"
	"sync"
	"github.com/goose-lang/primitive"
	"github.com/mit-pdos/gokv/grove_ffi"
	"github.com/mit-pdos/tulip/message"
	"github.com/mit-pdos/tulip/params"
	"github.com/mit-pdos/tulip/quorum"
	"github.com/mit-pdos/tulip/tulip"
	"github.com/mit-pdos/tulip/util"
)

///
/// Group coordinator.
///
/// Interface:
/// Read(ts, key) Value
/// Prepare(ts, ptgs, pwrs)
/// Commit(ts, pwrs)
/// Abort(ts)
/// ResultSession(rid uint64)
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
	cvresend  *sync.Cond
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

func Start(addrm map[uint64]grove_ffi.Address) *GroupCoordinator {
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
	cvresend := sync.NewCond(mu)
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
		cvresend  : cvresend,
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
	// Spawn a session with each replica in the group.
	for ridloop := range(gcoord.addrm) {
		rid := ridloop
		go func() {
			gcoord.ReadSession(rid, ts, key)
		}()
	}

	v, ok := gcoord.WaitUntilValueReady(ts, key)
	return v, ok
}

func (gcoord *GroupCoordinator) ReadSession(rid uint64, ts uint64, key string) {
	for !gcoord.ValueResponded(rid, key) && gcoord.AttachedWith(ts) {
		gcoord.SendRead(rid, ts, key)
		primitive.Sleep(params.NS_RESEND_READ)
	}

	// Either replica @rid has already responded with its value, or the value
	// for @key has already been determined. In either case, the corresponding
	// read session could terminate.
}

func (gcoord *GroupCoordinator) WaitUntilValueReady(ts uint64, key string) (tulip.Value, bool) {
	var value tulip.Value
	var valid bool

	gcoord.mu.Lock()

	for {
		if !gcoord.attachedWith(ts) {
			// fmt.Printf("[gcoord] %d no longer attached.\n", ts)
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

func (gcoord *GroupCoordinator) ValueResponded(rid uint64, key string) bool {
	gcoord.mu.Lock()
	done := gcoord.grd.responded(rid, key)
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
	// Spawn a prepare session with each replica.
	for ridloop := range(gcoord.addrm) {
		rid := ridloop
		go func() {
			gcoord.PrepareSession(rid, ts, ptgs, pwrs)
		}()
	}

	st, valid := gcoord.WaitUntilPrepareDone(ts)
	return st, valid
}

func (gcoord *GroupCoordinator) PrepareSession(rid uint64, ts uint64, ptgs []uint64, pwrs map[string]tulip.Value) {
	for {
		act, attached := gcoord.NextPrepareAction(rid, ts)

		if !attached {
			break
		}

		if act == GPP_FAST_PREPARE {
			fmt.Printf("[gcoord] Send fast prepare to R %d.\n", rid)
			gcoord.SendFastPrepare(rid, ts, pwrs, ptgs)
		} else if act == GPP_VALIDATE {
			gcoord.SendValidate(rid, ts, pwrs, ptgs)
		} else if act == GPP_PREPARE {
			fmt.Printf("[gcoord] Send prepare to R %d.\n", rid)
			gcoord.SendPrepare(rid, ts)
		} else if act == GPP_UNPREPARE {
			gcoord.SendUnprepare(rid, ts)
		} else if act == GPP_QUERY {
			gcoord.SendQuery(rid, ts)
		} else if act == GPP_REFRESH {
			// Keep sending keep-alive message until the transaction terminated.
			gcoord.SendRefresh(rid, ts)
		}

		if act == GPP_REFRESH {
			primitive.Sleep(params.NS_SEND_REFRESH)
		} else {
			// The optimal time to sleep is the time required to arrive at a
			// prepare decision. Waking up too frequently means sending
			// unnecessary messages, too infrequently means longer latency when
			// messages are lost.
			//
			// This might not be optimal for slow-path prepare. Consider
			// optimize this with CV wait and timeout.
			// primitive.Sleep(params.NS_RESEND_PREPARE)
			gcoord.mu.Lock()
			gcoord.cvresend.Wait()
			gcoord.mu.Unlock()
			fmt.Printf("[gcoord] Resend.\n")
		}
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

func (gcoord *GroupCoordinator) NextPrepareAction(rid uint64, ts uint64) (uint64, bool) {
	gcoord.mu.Lock()

	if !gcoord.attachedWith(ts) {
		gcoord.mu.Unlock()
		return 0, false
	}

	action := gcoord.gpp.action(rid)

	gcoord.mu.Unlock()

	return action, true
}

func (gcoord *GroupCoordinator) Attach(ts uint64) {
	gcoord.mu.Lock()
	gcoord.ts = ts
	gcoord.grd.reset()
	gcoord.gpp.reset()
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

func (gcoord *GroupCoordinator) Commit(ts uint64, pwrs tulip.KVMap) {
	gcoord.RegisterFinalization(ts)

	var leader = gcoord.GetLeader()
	for !gcoord.Finalized(ts) {
		gcoord.SendCommit(leader, ts, pwrs)
		primitive.Sleep(params.NS_RESEND_COMMIT)
		// Retry with different leaders until success.
		leader = gcoord.ChangeLeader()
	}
}

func (gcoord *GroupCoordinator) Abort(ts uint64) {
	gcoord.RegisterFinalization(ts)

	var leader = gcoord.GetLeader()
	for !gcoord.Finalized(ts) {
		gcoord.SendAbort(leader, ts)
		primitive.Sleep(params.NS_RESEND_ABORT)
		// Retry with different leaders until success.
		leader = gcoord.ChangeLeader()
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

func (gcoord *GroupCoordinator) ChangeLeader() uint64 {
	gcoord.mu.Lock()
	idxleader := (gcoord.idxleader + 1) % uint64(len(gcoord.rps))
	gcoord.idxleader = idxleader
	gcoord.mu.Unlock()
	return gcoord.rps[idxleader]
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

		gcoord.cvresend.Broadcast()
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

		msg := message.DecodeTxnResponse(data)
		kind := msg.Kind

		gcoord.mu.Lock()

		// Handle commit and abort responses. Note that timestamp check should
		// happen after.
		if kind == message.MSG_TXN_COMMIT || kind == message.MSG_TXN_ABORT {
			gcoord.processFinalizationResult(msg.Timestamp, msg.Result)
			gcoord.mu.Unlock()
			continue
		}

		// Ignore this response message if it is not the currently active one.
		if !gcoord.attachedWith(msg.Timestamp) {
			gcoord.mu.Unlock()
			continue
		}

		if kind == message.MSG_TXN_READ {
			gcoord.grd.processReadResult(msg.ReplicaID, msg.Key, msg.Version, msg.Slow)
		} else if kind == message.MSG_TXN_FAST_PREPARE {
			resend := gcoord.gpp.processFastPrepareResult(msg.ReplicaID, msg.Result)
			if resend {
				gcoord.cvresend.Broadcast()
			}
		} else if kind == message.MSG_TXN_VALIDATE {
			resend := gcoord.gpp.processValidateResult(msg.ReplicaID, msg.Result)
			if resend {
				gcoord.cvresend.Broadcast()
			}
		} else if kind == message.MSG_TXN_PREPARE {
			// This check also doesn't feel necessary, but seems to be due to
			// the fact that we cannot establish that "this session belongs to
			// rank 1".
			if msg.Rank == 1 {
				gcoord.gpp.processPrepareResult(msg.ReplicaID, msg.Result)
			}
		} else if kind == message.MSG_TXN_UNPREPARE {
			if msg.Rank == 1 {
				gcoord.gpp.processUnprepareResult(msg.ReplicaID, msg.Result)
			}
		} else if kind == message.MSG_TXN_QUERY {
			gcoord.gpp.processQueryResult(msg.Result)
		} else if kind == message.MSG_TXN_REFRESH {
			// No reponse message for REFRESH.
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
	data := message.EncodeTxnReadRequest(ts, key)
	gcoord.Send(rid, data)
}

func (gcoord *GroupCoordinator) SendFastPrepare(rid, ts uint64, pwrs tulip.KVMap, ptgs []uint64) {
	data := message.EncodeTxnFastPrepareRequest(ts, pwrs, ptgs)
	gcoord.Send(rid, data)
}

func (gcoord *GroupCoordinator) SendValidate(rid, ts uint64, pwrs tulip.KVMap, ptgs []uint64) {
	data := message.EncodeTxnValidateRequest(ts, 1, pwrs, ptgs)
	gcoord.Send(rid, data)
}

func (gcoord *GroupCoordinator) SendPrepare(rid, ts uint64) {
	data := message.EncodeTxnPrepareRequest(ts, 1)
	gcoord.Send(rid, data)
}

func (gcoord *GroupCoordinator) SendUnprepare(rid, ts uint64) {
	data := message.EncodeTxnUnprepareRequest(ts, 1)
	gcoord.Send(rid, data)
}

func (gcoord *GroupCoordinator) SendQuery(rid, ts uint64) {
	data := message.EncodeTxnQueryRequest(ts, 1)
	gcoord.Send(rid, data)
}

func (gcoord *GroupCoordinator) SendRefresh(rid, ts uint64) {
	data := message.EncodeTxnRefreshRequest(ts, 1)
	gcoord.Send(rid, data)
}

func (gcoord *GroupCoordinator) SendCommit(rid, ts uint64, pwrs tulip.KVMap) {
	data := message.EncodeTxnCommitRequest(ts, pwrs)
	gcoord.Send(rid, data)
}

func (gcoord *GroupCoordinator) SendAbort(rid, ts uint64) {
	data := message.EncodeTxnAbortRequest(ts)
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
	// Versions responded by each replica for each key. Instead of using a
	// single map[uint64]Version for the current key being read, this design allows
	// supporting more sophisticated "async-read" in future.
	qreadm map[string]map[uint64]tulip.Version
}

func mkGroupReader(nrps uint64) *GroupReader {
	grd := &GroupReader{ nrps : nrps }
	grd.reset()

	return grd
}

func (grd *GroupReader) reset() {
	grd.valuem = make(map[string]tulip.Value)
	grd.qreadm = make(map[string]map[uint64]tulip.Version)
}

func (grd *GroupReader) cquorum(n uint64) bool {
	return quorum.ClassicQuorum(grd.nrps) <= n
}

func (grd *GroupReader) pickLatestValue(key string) tulip.Value {
	var lts uint64
	var value tulip.Value

	verm := grd.qreadm[key]
	for _, ver := range(verm) {
		if lts <= ver.Timestamp {
			value = ver.Value
			lts = ver.Timestamp
		}
	}

	return value
}

func (grd *GroupReader) read(key string) (tulip.Value, bool) {
	v, ok := grd.valuem[key]
	return v, ok
}

func (grd *GroupReader) responded(rid uint64, key string) bool {
	_, final := grd.valuem[key]
	if final {
		// The final value is already determined.
		return true
	}

	qread, ok := grd.qreadm[key]
	if !ok {
		return false
	}

	_, responded := qread[rid]
	if responded {
		// The replica has already responded with its latest version.
		return true
	}

	return false
}

func (grd *GroupReader) clearVersions(key string) {
	delete(grd.qreadm, key)
}

func (grd *GroupReader) processReadResult(rid uint64, key string, ver tulip.Version, slow bool) {
	_, final := grd.valuem[key]
	if final {
		// The final value is already determined.
		return
	}

	if !slow {
		// Fast-path read: set the final value and clean up the read versions.
		grd.valuem[key] = ver.Value
		delete(grd.qreadm, key)
		return
	}

	qread, ok := grd.qreadm[key]
	if !ok {
		// The very first version arrives. Initialize a new map with the version
		// received.
		verm := make(map[uint64]tulip.Version)
		verm[rid] = ver
		grd.qreadm[key] = verm
		return
	}

	_, responded := qread[rid]
	if responded {
		// The replica has already responded with its latest version.
		return
	}

	// Record the version responded by the replica.
	qread[rid] = ver
	// This step seems unnecessary, but is required because of how Perennial
	// models Go map?
	grd.qreadm[key] = qread

	// Count the responses from replicas.
	n := uint64(len(qread))
	if !grd.cquorum(n) {
		// Cannot determine the final value without a classic quorum of
		// versions.
		return
	}

	// With enough versions, choose the latest one to be the final value.
	latest := grd.pickLatestValue(key)
	grd.valuem[key] = latest

	// The thread that determines the final value for @key also clears the
	// versions collected for @key.
	grd.clearVersions(key)
}

///
/// Group preparer. Used internally by group coordinator.
///
type GroupPreparer struct {
	// Number of replicas. Read-only.
	nrps   uint64
	// Control phase.
	phase  uint64
	// Fast-path replica responses.
	frespm map[uint64]bool
	// Replicas validated.
	vdm    map[uint64]bool
	// Slow-path replica responses.
	// NB: The range doesn't need to be bool, unit would suffice.
	srespm map[uint64]bool
	//
	// TODO: Merge @validated and @sresps
	// @phase = VALIDATING => records whether a certain replica is validated;
	// @phase = PREPARING / UNPREPARING => records prepared/unprepared.
	//
}

func mkGroupPreparer(nrps uint64) *GroupPreparer {
	gpp := &GroupPreparer{ nrps : nrps }
	gpp.reset()

	return gpp
}

func (gpp *GroupPreparer) reset() {
	gpp.phase = GPP_VALIDATING
	gpp.frespm = make(map[uint64]bool)
	gpp.vdm = make(map[uint64]bool)
	gpp.srespm = make(map[uint64]bool)
}

// Control phases of group preparer.
const (
	GPP_VALIDATING  uint64 = 0
	GPP_PREPARING   uint64 = 1
	GPP_UNPREPARING uint64 = 2
	GPP_WAITING     uint64 = 3
	GPP_PREPARED    uint64 = 4
	GPP_COMMITTED   uint64 = 5
	GPP_ABORTED     uint64 = 6
)

// Actions of group preparer.
const (
	GPP_FAST_PREPARE uint64 = 0
	GPP_VALIDATE     uint64 = 1
	GPP_PREPARE      uint64 = 2
	GPP_UNPREPARE    uint64 = 3
	GPP_QUERY        uint64 = 4
	GPP_REFRESH      uint64 = 5
)

// Argument:
// @rid: ID of the replica to which a new action is performed.
//
// Return value:
// @action: Next action to perform.
func (gpp *GroupPreparer) action(rid uint64) uint64 {
	// Validate the transaction through fast-path or slow-path.
	if gpp.in(GPP_VALIDATING) {
		// Check if the fast-path response for replica @rid is available.
		_, fresp := gpp.frespm[rid]
		if !fresp {
			// Have not received the fast-path response.
			return GPP_FAST_PREPARE
		}

		// Check if the validation response for replica @rid is available.
		_, validated := gpp.vdm[rid]
		if !validated {
			// Previous attemp of validation fails; retry.
			return GPP_VALIDATE
		}

		// Successfully validated (in either fast-path or slow-path).
		return GPP_QUERY
	}

	// Prepare the transaction through slow-path.
	if gpp.in(GPP_PREPARING) {
		_, prepared := gpp.srespm[rid]
		if !prepared {
			return GPP_PREPARE
		}
		return GPP_QUERY
	}

	// Unprepare the transaction through slow-path.
	if gpp.in(GPP_UNPREPARING) {
		_, unprepared := gpp.srespm[rid]
		if !unprepared {
			return GPP_UNPREPARE
		}
		return GPP_QUERY
	}

	// Backup coordinator exists, just wait for the result.
	if gpp.in(GPP_WAITING) {
		return GPP_QUERY
	}

	// The transaction has either prepared, committed, or aborted.
	return GPP_REFRESH
}

func (gpp *GroupPreparer) fquorum(n uint64) bool {
	return quorum.FastQuorum(gpp.nrps) <= n
}

func (gpp *GroupPreparer) cquorum(n uint64) bool {
	return quorum.ClassicQuorum(gpp.nrps) <= n
}

func (gpp *GroupPreparer) hcquorum(n uint64) bool {
	return quorum.Half(quorum.ClassicQuorum(gpp.nrps)) <= n
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

func (gpp *GroupPreparer) tryFastAbort() bool {
	// Count how many replicas have fast unprepared.
	n := util.CountBoolMap(gpp.frespm, false)

	// Move to the ABORTED phase if obtaining a fast quorum of fast unprepares.
	if gpp.fquorum(n) {
		gpp.phase = GPP_ABORTED
		return true
	}
	return false
}

func (gpp *GroupPreparer) tryFastPrepare() bool {
	// Count how many replicas have fast prepared.
	n := util.CountBoolMap(gpp.frespm, true)

	// Move to the PREPARED phase if obtaining a fast quorum of fast prepares.
	if gpp.fquorum(n) {
		gpp.phase = GPP_PREPARED
		return true
	}
	return false
}

func (gpp *GroupPreparer) tryBecomePreparing() bool {
	// Count how many replicas have validated.
	nvd := uint64(len(gpp.vdm))
	if !gpp.cquorum(nvd) {
		// Cannot move to the PREPARING phase unless some classic quorum of
		// replicas successfully validate.
		return false
	}

	// Count how many replicas have responded in the fast path.
	nresp := uint64(len(gpp.frespm))
	if !gpp.cquorum(nresp) {
		return false
	}

	// Count how many replicas have prepared.
	nfp := util.CountBoolMap(gpp.frespm, true)
	if !gpp.hcquorum(nfp) {
		// Cannot move to the PREPARING phase unless half (i.e., celing(n / 2))
		// of replicas in some classic quorum agrees to prepare.
		return false
	}

	gpp.srespm = make(map[uint64]bool)
	gpp.phase = GPP_PREPARING
	fmt.Printf("[gcoord] Become preparing\n")
	return true

	// Logical action: Propose.
}

func (gpp *GroupPreparer) tryBecomeUnpreparing() bool {
	// Count how many replicas have responded in the fast path.
	nresp := uint64(len(gpp.frespm))
	if !gpp.cquorum(nresp) {
		return false
	}

	// Count how many replicas have unprepared.
	nfu := util.CountBoolMap(gpp.frespm, false)
	if !gpp.hcquorum(nfu) {
		// Cannot move to the UNPREPARING phase unless half of replicas in some
		// classic quorum agrees to unprepare.
		return false
	}

	gpp.srespm = make(map[uint64]bool)
	gpp.phase = GPP_UNPREPARING
	return true

	// Logical action: Propose.
}

func (gpp *GroupPreparer) collectFastDecision(rid uint64, b bool) {
	gpp.frespm[rid] = b
}

func (gpp *GroupPreparer) collectValidation(rid uint64) {
	gpp.vdm[rid] = true
}

func (gpp *GroupPreparer) in(phase uint64) bool {
	return gpp.phase == phase
}

func (gpp *GroupPreparer) processFastPrepareResult(rid uint64, res uint64) bool {
	// Result is ready or a backup coordinator has become live.
	if gpp.tryResign(res) {
		return false
	}

	// Fast-prepare fails; fast abort if possible.
	if res == tulip.REPLICA_FAILED_VALIDATION {
		gpp.collectFastDecision(rid, false)

		aborted := gpp.tryFastAbort()
		if aborted {
			return false
		}

		if !gpp.in(GPP_VALIDATING) {
			return false
		}

		return gpp.tryBecomeUnpreparing()
	}

	// Fast-prepare succeeds; fast prepare if possible.
	gpp.collectFastDecision(rid, true)
	if gpp.tryFastPrepare() {
		return false
	}

	// Ignore the result if it's not in the validating phase. At this point, the
	// other possible phases are preparing and unpreparing.
	if !gpp.in(GPP_VALIDATING) {
		return false
	}

	// Record success of validation and try to move to the preparing phase.
	gpp.collectValidation(rid)
	return gpp.tryBecomePreparing()
}

func (gpp *GroupPreparer) processValidateResult(rid uint64, res uint64) bool {
	// Result is ready or a backup coordinator has become live.
	if gpp.tryResign(res) {
		return false
	}

	// Validation fails; nothing to record.
	if res == tulip.REPLICA_FAILED_VALIDATION {
		return false
	}

	// Skip if the coordiantor is not in the validating phase. At this point,
	// the other possible phases are preparing and unpreparing.
	if !gpp.in(GPP_VALIDATING) {
		return false
	}

	// Record success of validation and try to move to the preparing phase.
	gpp.collectValidation(rid)
	return gpp.tryBecomePreparing()
}

func (gpp *GroupPreparer) processPrepareResult(rid uint64, res uint64) {
	// Result is ready or a backup coordinator has become live.
	if gpp.tryResign(res) {
		return
	}

	// We might be able to prove this without an additional check.
	if !gpp.in(GPP_PREPARING) {
		return
	}

	// Record success of preparing the replica and try to move to prepared.
	gpp.srespm[rid] = true

	// Count how many replicas have prepared.
	n := uint64(len(gpp.srespm))

	// Go to prepared phase if successful prepares reaches a classic quorum.
	if gpp.cquorum(n) {
		gpp.phase = GPP_PREPARED
	}
}

func (gpp *GroupPreparer) processUnprepareResult(rid uint64, res uint64) {
	// Result is ready or a backup coordinator has become live.
	if gpp.tryResign(res) {
		return
	}

	// We might be able to prove this without an additional check.
	if !gpp.in(GPP_UNPREPARING) {
		return
	}

	// Record success of unpreparing the replica and try to move to aborted.
	gpp.srespm[rid] = true

	// Count how many replicas have unprepared.
	n := uint64(len(gpp.srespm))

	// Go to aborted phase if successful unprepares reaches a classic quorum.
	if gpp.cquorum(n) {
		gpp.phase = GPP_ABORTED
	}
}

func (gpp *GroupPreparer) processQueryResult(res uint64) {
	// Result is ready or a backup coordinator has become live.
	gpp.tryResign(res)
}

func (gpp *GroupPreparer) getPhase() uint64 {
	return gpp.phase
}
