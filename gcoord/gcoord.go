package gcoord

import (
	"sync"
	"github.com/goose-lang/primitive"
	"github.com/mit-pdos/gokv/grove_ffi"
	"github.com/mit-pdos/tulip/tulip"
	"github.com/mit-pdos/tulip/params"
	"github.com/mit-pdos/tulip/message"
	"github.com/mit-pdos/tulip/quorum"
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
	// Replica addresses. Read-only.
	rps     map[uint64]grove_ffi.Address
	// Mutex protecting fields below.
	mu      *sync.Mutex
	// Condition variable used to notify arrival of responses.
	cv      *sync.Cond
	// Timestamp of the currently active transaction.
	ts       uint64
	// ID of the replica believed to be the leader of this group.
	leader   uint64
	// Group reader.
	grd      *GroupReader
	// Group preparer.
	gpp      *GroupPreparer
	// IDs of the finalizing transactions. Using unit as range would suffice.
	tsfinals map[uint64]bool
	// Connections to replicas.
	conns    map[uint64]grove_ffi.Connection
}

// Arguments:
// @ts: Timestamp of the transaction performing this read.
// @key: Key to be read.
//
// Return value:
// @value: Value of @key.
//
// @gcoord.Read blocks until the value of @key is determined.
func (gcoord *GroupCoordinator) Read(ts uint64, key string) tulip.Value {
	// Spawn a session with each replica in the group.
	for ridloop := range(gcoord.rps) {
		rid := ridloop
		go func() {
			gcoord.ReadSession(rid, ts, key)
		}()
	}

	v := gcoord.WaitUntilValueReady(key)
	return v
}

func (gcoord *GroupCoordinator) ReadSession(rid uint64, ts uint64, key string) {
	for !gcoord.ValueResponded(rid, key) {
		gcoord.SendRead(rid, ts, key)
		primitive.Sleep(params.NS_RESEND_READ)
	}

	// Either replica @rid has already responded with its value, or the value
	// for @key has already been determined. In either case, the corresponding
	// read session could terminate.
}

func (gcoord *GroupCoordinator) WaitUntilValueReady(key string) tulip.Value {
	gcoord.mu.Lock()

	var value tulip.Value
	var ok bool
	value, ok = gcoord.grd.read(key)
	for !ok {
		gcoord.cv.Wait()
		value, ok = gcoord.grd.read(key)
	}

	gcoord.mu.Unlock()
	return value
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
	for ridloop := range(gcoord.rps) {
		rid := ridloop
		go func() {
			gcoord.PrepareSession(rid, ts, ptgs, pwrs)
		}()
	}

	st, valid := gcoord.WaitUntilPrepareDone(ts)
	return st, valid
}

func (gcoord *GroupCoordinator) PrepareSession(
	rid uint64, ts uint64, ptgs []uint64, pwrs map[string]tulip.Value,
) {
	var act uint64 = gcoord.NextPrepareAction(rid)
	for gcoord.AttachedWith(ts) {
		if act == GPP_FAST_PREPARE {
			gcoord.SendFastPrepare(rid, ts, pwrs, ptgs)
		} else if act == GPP_VALIDATE {
			gcoord.SendValidate(rid, ts, 1, pwrs, ptgs)
		} else if act == GPP_PREPARE {
			gcoord.SendPrepare(rid, ts, 1)
		} else if act == GPP_UNPREPARE {
			gcoord.SendUnprepare(rid, ts, 1)
		} else if act == GPP_QUERY {
			gcoord.SendQuery(rid, ts, 1)
		} else if act == GPP_REFRESH {
			// Keep sending keep-alive message until the transaction terminated.
			gcoord.SendRefresh(rid, ts, 1)
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
			primitive.Sleep(params.NS_RESEND_PREPARE)
		}

		act = gcoord.NextPrepareAction(rid)
	}

	// The coordinator is no longer associated with @ts, this could happen only
	// after the prepare decision for @ts on @rid is made. Hence, this session
	// can terminate.
}

func (gcoord *GroupCoordinator) WaitUntilPrepareDone(ts uint64) (uint64, bool) {
	gcoord.mu.Lock()

	if gcoord.ts != ts {
		gcoord.mu.Unlock()
		// TXN_PREPARED here is just a placeholder.
		return tulip.TXN_PREPARED, false
	}

	for !gcoord.gpp.ready() {
		gcoord.cv.Wait()
	}

	phase := gcoord.gpp.getPhase()

	gcoord.mu.Unlock()

	if phase == GPP_COMMITTED {
		return tulip.TXN_COMMITTED, true
	}

	if phase == GPP_ABORTED {
		return tulip.TXN_ABORTED, true
	}

	return tulip.TXN_PREPARED, true
}

func (gcoord *GroupCoordinator) NextPrepareAction(rid uint64) uint64 {
	gcoord.mu.Lock()
	a := gcoord.gpp.action(rid)
	gcoord.mu.Unlock()
	return a
}

func (gcoord *GroupCoordinator) AttachedWith(ts uint64) bool {
	gcoord.mu.Lock()
	b := gcoord.ts == ts
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
	leader := (gcoord.leader + 1) % uint64(len(gcoord.rps))
	gcoord.leader = leader
	gcoord.mu.Unlock()
	return leader
}

func (gcoord *GroupCoordinator) GetLeader() uint64 {
	gcoord.mu.Lock()
	leader := gcoord.leader
	gcoord.mu.Unlock()
	return leader
}

func (gcoord *GroupCoordinator) ResultSession(rid uint64) {
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
		if gcoord.ts != msg.Timestamp {
			gcoord.mu.Unlock()
			continue
		}

		gpp := gcoord.gpp
		grd := gcoord.grd

		if kind == message.MSG_TXN_READ {
			grd.processReadResult(rid, msg.Key, msg.Version)
		} else if kind == message.MSG_TXN_FAST_PREPARE {
			gpp.processFastPrepareResult(rid, msg.Result)
		} else if kind == message.MSG_TXN_VALIDATE {
			gpp.processValidateResult(rid, msg.Result)
		} else if kind == message.MSG_TXN_PREPARE {
			gpp.processPrepareResult(rid, msg.Result)
		} else if kind == message.MSG_TXN_UNPREPARE {
			gpp.processUnprepareResult(rid, msg.Result)
		} else if kind == message.MSG_TXN_QUERY {
			gpp.processQueryResult(rid, msg.Result)
		} else if kind == message.MSG_TXN_REFRESH {
			// No reponse message for REFRESH.
		}

		// In the current design the coordinator will be notified whenever a new
		// response arrives, and then checks whether the final result (e.g.,
		// prepared, committed, or aborted in the case of preparing) is
		// ready. An optimization would be requiring those @process{X}Result
		// functions to return a bool indicating the final result is ready, and
		// call @gcoord.cv.Signal only on those occasions.
		gcoord.cv.Signal()

		gcoord.mu.Unlock()
	}
}

func (gcoord *GroupCoordinator) Send(rid uint64, data []byte) {
	conn, ok := gcoord.GetConnection(rid)
	if !ok {
		gcoord.Connect(rid)
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

// TODO: Implement these.

func (gcoord *GroupCoordinator) SendRead(rid, ts uint64, key string) {
	gcoord.Send(rid, message.EncodeTxnRead(ts, key))
}

func (gcoord *GroupCoordinator) SendFastPrepare(rid, ts uint64, pwrs tulip.KVMap, ptgs []uint64) {
	gcoord.Send(rid, message.EncodeTxnFastPrepare(ts, pwrs, ptgs))
}

func (gcoord *GroupCoordinator) SendValidate(rid, ts, rank uint64, pwrs tulip.KVMap, ptgs []uint64) {
}

func (gcoord *GroupCoordinator) SendPrepare(rid, ts, rank uint64) {
}

func (gcoord *GroupCoordinator) SendUnprepare(rid, ts, rank uint64) {
}

func (gcoord *GroupCoordinator) SendQuery(rid, ts, rank uint64) {
}

func (gcoord *GroupCoordinator) SendRefresh(rid, ts, rank uint64) {
}

func (gcoord *GroupCoordinator) SendCommit(rid, ts uint64, pwrs tulip.KVMap) {
}

func (gcoord *GroupCoordinator) SendAbort(rid, ts uint64) {
}

func (gcoord *GroupCoordinator) GetConnection(rid uint64) (grove_ffi.Connection, bool) {
	gcoord.mu.Lock()
	conn, ok := gcoord.conns[rid]
	gcoord.mu.Unlock()
	return conn, ok
}

func (gcoord *GroupCoordinator) Connect(rid uint64) bool {
	addr := gcoord.rps[rid]
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

func (grd *GroupReader) processReadResult(rid uint64, key string, ver tulip.Version) {
	_, final := grd.valuem[key]
	if final {
		// The final value is already determined.
		return
	}

	if ver.Timestamp == 0 {
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
	fresps map[uint64]bool
	// Slow-path replica responses.
	// @phase = VALIDATING => records whether a certain replica is validated;
	// @phase = PREPARING / UNPREPARING => records prepared/unprepared.
	// NB: The range doesn't need to be bool, unit would suffice.
	sresps map[uint64]bool
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
	if gpp.phase == GPP_VALIDATING {
		// Check if the fast-path response for replica @rid is available.
		_, fresp := gpp.fresps[rid]
		if !fresp {
			// Have not received the fast-path response.
			return GPP_FAST_PREPARE
		}

		// Check if the validation response for replica @rid is available.
		_, validated := gpp.sresps[rid]
		if !validated {
			// Previous attemp of validation fails; retry.
			return GPP_VALIDATE
		}

		// Successfully validated (in either fast-path or slow-path).
		return GPP_QUERY
	}

	// Prepare the transaction through slow-path.
	if gpp.phase == GPP_PREPARING {
		_, prepared := gpp.sresps[rid]
		if !prepared {
			return GPP_PREPARE
		}
		return GPP_QUERY
	}

	// Unprepare the transaction through slow-path.
	if gpp.phase == GPP_UNPREPARING {
		_, unprepared := gpp.sresps[rid]
		if !unprepared {
			return GPP_UNPREPARE
		}
		return GPP_QUERY
	}

	// Backup coordinator exists, just wait for the result.
	if gpp.phase == GPP_WAITING {
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
		gpp.phase = GPP_WAITING
		return true
	}

	return false
}

func countbm(m map[uint64]bool, b bool) uint64 {
	var n uint64 = 0
	for _, v := range(m) {
		if v == b {
			n = n + 1
		}
	}

	return n
}

func (gpp *GroupPreparer) tryBecomeAborted() bool {
	// Count how many replicas have fast unprepared.
	n := countbm(gpp.fresps, false)

	// Move to the ABORTED phase if obtaining a fast quorum of fast unprepares.
	if gpp.fquorum(n) {
		gpp.phase = GPP_ABORTED
		return true
	}
	return false
}

func (gpp *GroupPreparer) tryBecomePrepared() bool {
	// Count how many replicas have fast prepared.
	n := countbm(gpp.fresps, true)

	// Move to the PREPARED phase if obtaining a fast quorum of fast prepares.
	if gpp.fquorum(n) {
		gpp.phase = GPP_PREPARED
		return true
	}
	return false
}

func (gpp *GroupPreparer) tryBecomePreparing() {
	// Count how many replicas have validated.
	n := uint64(len(gpp.sresps))

	// Move to the PREPARING phase if obtaining a classic quorum of positive
	// validation responses.
	if gpp.cquorum(n) {
		gpp.phase = GPP_PREPARING
		// Reset the slow-path responses to record prepare decisions.
		gpp.sresps = make(map[uint64]bool)
	}
}

func (gpp *GroupPreparer) processFastPrepareResult(rid uint64, res uint64) {
	// Result is ready or a backup coordinator has become live.
	if gpp.tryResign(res) {
		return
	}

	// Fast-prepare fails; fast abort if possible.
	if res == tulip.REPLICA_FAILED_VALIDATION {
		gpp.fresps[rid] = false
		gpp.tryBecomeAborted()
		return
	}

	// Fast-prepare succeeds; fast commit if possible.
	gpp.fresps[rid] = true
	if gpp.tryBecomePrepared() {
		return
	}

	// Ignore the result if it's not in the validating phase. At this point, the
	// other possible phases are preparing and unpreparing.
	if gpp.phase != GPP_VALIDATING {
		return
	}

	// Record success of validation and try to move to the preparing phase.
	gpp.sresps[rid] = true
	gpp.tryBecomePreparing()
}

func (gpp *GroupPreparer) processValidateResult(rid uint64, res uint64) {
	// Result is ready or a backup coordinator has become live.
	if gpp.tryResign(res) {
		return
	}

	// Skip if the coordiantor is not in the validating phase. At this point,
	// the other possible phases are preparing and unpreparing.
	if gpp.phase != GPP_VALIDATING {
		return
	}

	// Validation fails; nothing to record.
	if res == tulip.REPLICA_FAILED_VALIDATION {
		return
	}

	// Record success of validation and try to move to the preparing phase.
	gpp.sresps[rid] = true
	gpp.tryBecomePreparing()
}

func (gpp *GroupPreparer) processPrepareResult(rid uint64, res uint64) {
	// Result is ready or a backup coordinator has become live.
	if gpp.tryResign(res) {
		return
	}

	// Prove that at this point the only possible phase is preparing.
	// Resource: Proposal map at rank 1 is true
	// Invariant: UNPREPARING => proposal map at rank 1 is false: contradiction
	// Invariant: Proposal entry present -> not VALIDATING

	// Record success of preparing the replica and try to move to prepared.
	gpp.sresps[rid] = true

	// Count how many replicas have prepared/unprepared.
	n := uint64(len(gpp.sresps))

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

	// Prove that at this point the only possible phase is unpreparing.

	// Record success of unpreparing the replica and try to move to aborted.
	gpp.sresps[rid] = true

	// Count how many replicas have prepared/unprepared.
	n := uint64(len(gpp.sresps))

	// Go to aborted phase if successful unprepares reaches a classic quorum.
	if gpp.cquorum(n) {
		gpp.phase = GPP_ABORTED
	}
}

func (gpp *GroupPreparer) processQueryResult(rid uint64, res uint64) {
	// Result is ready or a backup coordinator has become live.
	gpp.tryResign(res)
}

func (gpp *GroupPreparer) getPhase() uint64 {
	return gpp.phase
}
