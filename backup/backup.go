package backup

import (
	"sync"
	"github.com/goose-lang/primitive"
	"github.com/goose-lang/std"
	"github.com/mit-pdos/gokv/grove_ffi"
	"github.com/mit-pdos/tulip/message"
	"github.com/mit-pdos/tulip/params"
	"github.com/mit-pdos/tulip/quorum"
	"github.com/mit-pdos/tulip/tulip"
	"github.com/mit-pdos/tulip/trusted_proph"
)

// A note on relationship between @phase and @pwrsok/@pwrs: Ideally, we should
// construct an invariant saying that if @phase is VALIDATING, PREPARING, or
// PREPARED, then @pwrsok = true (and @pwrs is available). But before figuring
// out the right invariant, we added some redundant checks (i.e., ones that
// should never fail) to make the proof happy (see calls of @gcoord.GetPwrs).
type BackupGroupPreparer struct {
	// Number of replicas. Read-only.
	nrps   uint64
	// Control phase.
	phase  uint64
	// Buffered writes ready.
	pwrsok bool
	// Buffered writes to this group.
	pwrs   map[string]tulip.Value
	// Latest prepare proposal on each replica.
	pps    map[uint64]tulip.PrepareProposal
	// Replicas that have validated.
	vdm    map[uint64]bool
	// Replicas that prepared/unprepared (depending on @phase).
	srespm map[uint64]bool
	//
	// TODO: Merge @vdm and @srespm.
	// @phase = INQUIRING / VALIDATING => records validated;
	// @phase = PREPARING / UNPREPARING => records prepared / unprepared.
	// NB: The range doesn't need to be bool, unit would suffice.
	//
}

// Control phases of backup group coordinator.
const (
	BGPP_INQUIRING   uint64 = 0
	BGPP_VALIDATING  uint64 = 1
	BGPP_PREPARING   uint64 = 2
	BGPP_UNPREPARING uint64 = 3
	BGPP_PREPARED    uint64 = 4
	BGPP_COMMITTED   uint64 = 5
	BGPP_ABORTED     uint64 = 6
	BGPP_STOPPED     uint64 = 7
)

func mkBackupGroupPreparer(nrps uint64) *BackupGroupPreparer {
	gpp := &BackupGroupPreparer{
		nrps   : nrps,
		phase  : BGPP_INQUIRING,
		pwrsok : false,
		pps    : make(map[uint64]tulip.PrepareProposal),
		vdm    : make(map[uint64]bool),
		srespm : make(map[uint64]bool),
	}

	return gpp
}

// Actions of backup group coordinator.
const (
	BGPP_INQUIRE   uint64 = 0
	BGPP_VALIDATE  uint64 = 1
	BGPP_PREPARE   uint64 = 2
	BGPP_UNPREPARE uint64 = 3
	BGPP_REFRESH   uint64 = 4
)

func (gpp *BackupGroupPreparer) inquired(rid uint64) bool {
	_, inquired := gpp.pps[rid]
	return inquired
}

func (gpp *BackupGroupPreparer) validated(rid uint64) bool {
	_, validated := gpp.vdm[rid]
	return validated
}

func (gpp *BackupGroupPreparer) accepted(rid uint64) bool {
	_, accepted := gpp.srespm[rid]
	return accepted
}

// Argument:
// @rid: ID of the replica to which a new action is performed.
//
// Return value:
// @action: Next action to perform.
func (gpp *BackupGroupPreparer) action(rid uint64) uint64 {
	phase := gpp.getPhase()

	// Inquire the transaction status on replica @rid.
	if phase == BGPP_INQUIRING {
		// Check if the inquire response (i.e., latest proposal + validation
		// status) for replica @rid is available.
		inquired := gpp.inquired(rid)
		if !inquired {
			// Have not received the inquire response.
			return BGPP_INQUIRE
		}

		return BGPP_REFRESH
	}

	// Note that the INQUIRING phase exists merely for performance reason. An
	// alternative design is to remove this phase and simply start with the
	// VALIDATING phase. However, that means the backup coordinator would
	// immediately start validating replicas that are not validated yet, even if
	// it decides to unprepare later, which might create unnecessary lock
	// contention. Thus, we separate the two phases and the coordinator would
	// try to validate the transaction only if when proposing PREPARED becomes
	// an option (for detail, see @ProcessInquireResult).
	//
	// TODO: Is the above statement actually true? It seems like the VALIDATING
	// phase has an additional guarantee that the partial writes are available?

	// Validate the transaction.
	if phase == BGPP_VALIDATING {
		// Check if the inquire response (i.e., latest proposal + validation
		// status) for replica @rid is available.
		inquired := gpp.inquired(rid)
		if !inquired {
			// Have not received inquire response.
			return BGPP_INQUIRE
		}

		// The inquire response is available. Now check if the transaction has
		// been validated on replica @rid.
		validated := gpp.validated(rid)
		if !validated {
			return BGPP_VALIDATE
		}

		return BGPP_REFRESH
	}

	// Prepare the transaction.
	if phase == BGPP_PREPARING {
		prepared := gpp.accepted(rid)
		if !prepared {
			return BGPP_PREPARE
		}
		return BGPP_REFRESH
	}

	// Unprepare the transaction.
	if phase == BGPP_UNPREPARING {
		unprepared := gpp.accepted(rid)
		if !unprepared {
			return BGPP_UNPREPARE
		}
		return BGPP_REFRESH
	}

	return BGPP_REFRESH
}

func (gpp *BackupGroupPreparer) fquorum(n uint64) bool {
	return quorum.FastQuorum(gpp.nrps) <= n
}

func (gpp *BackupGroupPreparer) cquorum(n uint64) bool {
	return quorum.ClassicQuorum(gpp.nrps) <= n
}

func (gpp *BackupGroupPreparer) hcquorum(n uint64) bool {
	return quorum.Half(quorum.ClassicQuorum(gpp.nrps)) <= n
}

func (gpp *BackupGroupPreparer) tryResign(res uint64) bool {
	if gpp.ready() {
		return true
	}

	if res == tulip.REPLICA_COMMITTED_TXN {
		gpp.phase = BGPP_COMMITTED
		return true
	}

	if res == tulip.REPLICA_ABORTED_TXN {
		gpp.phase = BGPP_ABORTED
		return true
	}

	if res == tulip.REPLICA_STALE_COORDINATOR {
		gpp.phase = BGPP_STOPPED
		return true
	}

	return false
}

func (gpp *BackupGroupPreparer) accept(rid uint64) {
	gpp.srespm[rid] = true
}

func (gpp *BackupGroupPreparer) quorumAccepted() bool {
	// Count how many replicas have prepared or unprepared, depending on
	// @gpp.phase.
	n := uint64(len(gpp.srespm))
	return gpp.cquorum(n)
}

func (gpp *BackupGroupPreparer) processPrepareResult(rid uint64, res uint64) {
	// Result is ready or another backup coordinator has become live.
	if gpp.tryResign(res) {
		return
	}

	if !gpp.in(BGPP_PREPARING) {
		return
	}

	// Record success of preparing the replica.
	gpp.accept(rid)

	// A necessary condition to move to the PREPARED phase: validated on some
	// classic quorum. TODO: We should be able to remove this check with the
	// safe-propose invariant.
	if !gpp.quorumValidated() {
		return
	}

	// Move to the PREPARED phase if receiving a classic quorum of positive
	// prepare responses.
	if gpp.quorumAccepted() {
		gpp.phase = BGPP_PREPARED
	}
}

func (gpp *BackupGroupPreparer) processUnprepareResult(rid uint64, res uint64) {
	// Result is ready or another backup coordinator has become live.
	if gpp.tryResign(res) {
		return
	}

	if !gpp.in(BGPP_UNPREPARING) {
		return
	}

	// Record success of unpreparing the replica.
	gpp.accept(rid)

	// Move to the ABORTED phase if obtaining a classic quorum of positive
	// unprepare responses.
	if gpp.quorumAccepted() {
		gpp.phase = BGPP_ABORTED
	}
}

// Return value:
// @latest: The latest non-fast proposal if @latest.rank > 0; @gpp.pps
// contain only fast proposals if @latest.rank == 0.
func (gpp *BackupGroupPreparer) latestProposal() tulip.PrepareProposal {
	var latest tulip.PrepareProposal

	for _, pp := range(gpp.pps) {
		if latest.Rank <= pp.Rank {
			latest = pp
		}
	}

	return latest
}

// Return value:
// @nprep: The number of fast unprepares collected in @gpp.pps.
//
// Note that this function requires all proposals in @gpp.pps to be proposed in
// the fast rank in order to match its semantics.
func (gpp *BackupGroupPreparer) countFastProposals(b bool) uint64 {
	var nprep uint64

	for _, pp := range(gpp.pps) {
		if b == pp.Prepared {
			nprep = std.SumAssumeNoOverflow(nprep, 1)
		}
	}

	return nprep
}

func (gpp *BackupGroupPreparer) collectProposal(rid uint64, pp tulip.PrepareProposal) {
	gpp.pps[rid] = pp
}

func (gpp *BackupGroupPreparer) countProposals() uint64 {
	return uint64(len(gpp.pps))
}

func (gpp *BackupGroupPreparer) setPwrs(pwrs tulip.KVMap) {
	gpp.pwrsok = true
	gpp.pwrs = pwrs
}

func (gpp *BackupGroupPreparer) validate(rid uint64) {
	gpp.vdm[rid] = true
}

func (gpp *BackupGroupPreparer) quorumValidated() bool {
	// Count the number of successful validation.
	n := uint64(len(gpp.vdm))
	// Return if the transaction has been validated on a classic quorum.
	return gpp.cquorum(n)
}

func (gpp *BackupGroupPreparer) in(phase uint64) bool {
	return gpp.phase == phase
}

func (gpp *BackupGroupPreparer) tryValidate(rid uint64, vd bool, pwrs tulip.KVMap) {
	if vd {
		gpp.setPwrs(pwrs)
		gpp.validate(rid)
	}
}

func (gpp *BackupGroupPreparer) becomePreparing() {
	gpp.srespm = make(map[uint64]bool)
	gpp.phase = BGPP_PREPARING
}

func (gpp *BackupGroupPreparer) becomeUnpreparing() {
	gpp.srespm = make(map[uint64]bool)
	gpp.phase = BGPP_UNPREPARING
}

func (gpp *BackupGroupPreparer) processInquireResult(rid uint64, pp tulip.PrepareProposal, vd bool, pwrs tulip.KVMap, res uint64) {
	// Result is ready or another backup coordinator has become live.
	if gpp.tryResign(res) {
		return
	}

	// Skip since the coordinator is already in the second phase.
	if gpp.in(BGPP_PREPARING) || gpp.in(BGPP_UNPREPARING) {
		return
	}

	// Record prepare prososal and validation result.
	gpp.collectProposal(rid, pp)
	gpp.tryValidate(rid, vd, pwrs)

	// No decision should be made without a classic quorum of prepare proposals.
	n := gpp.countProposals()
	if !gpp.cquorum(n) {
		return
	}

	// Compute the latest prepare proposal.
	latest := gpp.latestProposal()
	if latest.Rank != 0 {
		// Unprepare this transaction if its latest slow proposal is @false.
		if !latest.Prepared {
			gpp.becomeUnpreparing()
			return
		}

		// If the latest slow proposal is @true, we further check the
		// availability of partial writes. We might be able to prove it
		// without this check by using the fact that a cquorum must have been
		// validated in order to prepare in the slow rank, and the fact that
		// we've received a cquorum of responses at this point, but the
		// reasoning seems pretty tricky. In any case, the check should be valid
		// and would eventually passes with some alive cquorum.
		_, ok := gpp.getPwrs()
		if !ok {
			return
		}
		gpp.becomePreparing()
		return
	}

	// All the proposals collected so far are fast. Now we need to decide the
	// next step based on how many of them are prepared and unprepared.
	nfu := gpp.countFastProposals(false)

	// Note that using majority (i.e., floor(n / 2) + 1) rather than half (i.e.,
	// ceiling(n / 2)) as the threshold would lead to liveness issues.
	//
	// For instance, in a 3-replica setup, using majority means that the
	// coordinator can propose UNPREPARED only if it knows there are at least
	// two fast unprepares. Consider the following scenario:
	// 1. Replica X fails.
	// 2. Txn A validates on replica Y and fails.
	// 3. Txn B validates on replica Z and fails.
	// 4. Backup group coordinators of A and B will obtain each one fast
	// unprepare (on Z and Y, respetively), so they cannot abort, but also not
	// commit since they will not be able to validate on the other replica.
	if gpp.hcquorum(nfu) {
		// The number of fast unprepares has reached at least half of some
		// classic quorum, which means the number of fast prepares must not
		// reach a majority in this quorum. This further implies the transaction
		// could not have fast prepared, and hence it is safe to unprepare.
		//
		// Logical action: Propose.
		gpp.becomeUnpreparing()
		return
	}

	// At this point, we know there exists a classic quorum in which the number
	// of fast unprepares does not reach half (and hence not a majority),
	// meaning the transaction could not have fast unprepared. However, we still
	// need to ensure validation on a majority to achieve mutual exclusion.

	// The check below is a proof artifact. We should be able to deduce safety
	// of proposing PREPARE from the fact that the number of fast unprepares
	// does not reach half, and the fact that decisions are binary. TODO: remove
	// this once that is proven.
	nfp := gpp.countFastProposals(true)
	if !gpp.hcquorum(nfp) {
		return
	}

	if gpp.quorumValidated() {
		// Logical action: Propose.
		gpp.becomePreparing()
		return
	}

	// Cannot proceed to the second phase (i.e., proposing prepares or
	// unprepares). Try to validate on more replicas.
	gpp.phase = BGPP_VALIDATING
}

func (gpp *BackupGroupPreparer) processValidateResult(rid uint64, res uint64) {
	// Result is ready or another backup coordinator has become live.
	if gpp.tryResign(res) {
		return
	}

	// Skip since the coordinator is already in the second phase.
	if !gpp.in(BGPP_VALIDATING) {
		return
	}

	// Validation fails; nothing to record.
	if res == tulip.REPLICA_FAILED_VALIDATION {
		return
	}

	// Record success of validation.
	gpp.validate(rid)

	// To be in the VALIDATING phase, we know the transaction must not have fast
	// unprepared (need an invariant to remember this fact established when
	// transiting from INQUIRING to VALIDATING in @ProcessInquireResult).

	// Move to PREPARING phase if it reaches a majority.
	if gpp.quorumValidated() {
		gpp.becomePreparing()
		return
	}
}

func (gpp *BackupGroupPreparer) processQueryResult(rid uint64, res uint64) {
	gpp.tryResign(res)
}

func (gpp *BackupGroupPreparer) processFinalizationResult(res uint64) {
	if res == tulip.REPLICA_WRONG_LEADER {
		return
	}
	gpp.stop()
}

func (gpp *BackupGroupPreparer) ready() bool {
	return BGPP_PREPARED <= gpp.phase
}

func (gpp *BackupGroupPreparer) finalized() bool {
	return BGPP_COMMITTED <= gpp.phase
}

func (gpp *BackupGroupPreparer) getPhase() uint64 {
	return gpp.phase
}

func (gpp *BackupGroupPreparer) getPwrs() (tulip.KVMap, bool) {
	return gpp.pwrs, gpp.pwrsok
}

func (gpp *BackupGroupPreparer) stop()  {
	gpp.phase = BGPP_STOPPED
}

type BackupGroupCoordinator struct {
	// Coordinator ID. This seems to be a proof artifact due to either the
	// limitation of our network model, or just missing the right network
	// invariants.
	cid       tulip.CoordID
	// Timestamp of the transaction to be finalized.
	ts        uint64
	// Rank of this backup coordinator.
	rank      uint64
	// Replica IDs in this group.
	rps       []uint64
	// Replica addresses. Read-only.
	addrm     map[uint64]grove_ffi.Address
	// Mutex protecting fields below.
	mu        *sync.Mutex
	// Condition variable used to notify arrival of responses.
	cv        *sync.Cond
	// The replica believed to be the leader of this group.
	idxleader uint64
	// Group preparer.
	gpp       *BackupGroupPreparer
	// Connections to replicas.
	conns     map[uint64]grove_ffi.Connection
}

func mkBackupGroupCoordinator(addrm map[uint64]grove_ffi.Address, cid tulip.CoordID, ts uint64, rank uint64) *BackupGroupCoordinator {
	mu := new(sync.Mutex)
	cv := sync.NewCond(mu)
	nrps := uint64(len(addrm))

	var rps = make([]uint64, 0)
	for rid := range(addrm) {
		rps = append(rps, rid)
	}

	gcoord := &BackupGroupCoordinator{
		cid       : cid,
		ts        : ts,
		rank      : rank,
		rps       : rps,
		addrm     : addrm,
		mu        : mu,
		cv        : cv,
		idxleader : 0,
		gpp       : mkBackupGroupPreparer(nrps),
		conns     : make(map[uint64]grove_ffi.Connection),
	}

	return gcoord
}

// TODO: We probably don't need to remember @ts since it can be passsed directly
// to @gcoord.ResponseSession and @gcoord.Prepare. We just need to maintain
// logically the connection between those parameters and the gcoord
// representation predicate. Remembering @cid and @rank makes sense since they
// belong to the group coordinator, rather than the transaction
// coordinator. This means we can remove @rank from @gcoord.Prepare and @gcoord.PrepareSession.
func startBackupGroupCoordinator(addrm map[uint64]grove_ffi.Address, cid tulip.CoordID, ts, rank uint64) *BackupGroupCoordinator {
	gcoord := mkBackupGroupCoordinator(addrm, cid, ts, rank)

	for ridloop := range(addrm) {
		rid := ridloop

		go func() {
			gcoord.ResponseSession(rid)
		}()
	}

	return gcoord
}

// Arguments:
// @ts: Transaction timestamp.
//
// Return values:
// @status: Transaction status.
// @valid: If true, the prepare process goes through without encountering a more
// recent coordinator. @status is meaningful iff @valid is true.
//
// @Prepare blocks until the prepare decision (one of prepared, committed,
// aborted) is made, or a higher-ranked backup coordinator is up.
func (gcoord *BackupGroupCoordinator) Prepare(ts, rank uint64, ptgs []uint64) (uint64, bool) {
	// Spawn a session with each replica.
	for ridloop := range(gcoord.addrm) {
		rid := ridloop

		go func() {
			gcoord.PrepareSession(rid, ts, rank, ptgs)
		}()
	}

	status, valid := gcoord.WaitUntilPrepareDone()
	return status, valid
}

func (gcoord *BackupGroupCoordinator) PrepareSession(rid, ts, rank uint64, ptgs []uint64) {
	for !gcoord.Finalized() {
		act := gcoord.NextPrepareAction(rid)

		if act == BGPP_INQUIRE {
			gcoord.SendInquire(rid, ts, rank, gcoord.cid)
		} else if act == BGPP_VALIDATE {
			pwrs, ok := gcoord.GetPwrs()
			// The write set should be available in the VALIDATING phase; it
			// should not require the check.
			if ok {
				gcoord.SendValidate(rid, ts, rank, pwrs, ptgs)
			}
		} else if act == BGPP_PREPARE {
			gcoord.SendPrepare(rid, ts, rank)
		} else if act == BGPP_UNPREPARE {
			gcoord.SendUnprepare(rid, ts, rank)
		} else if act == BGPP_REFRESH {
			gcoord.SendRefresh(rid, ts, rank)
		}

		if act == BGPP_REFRESH {
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
	}
}

func (gcoord *BackupGroupCoordinator) WaitUntilPrepareDone() (uint64, bool) {
	gcoord.mu.Lock()

	for !gcoord.gpp.ready() {
		gcoord.cv.Wait()
	}

	phase := gcoord.gpp.getPhase()

	gcoord.mu.Unlock()

	if phase == BGPP_STOPPED {
		// TXN_PREPARED here is just a placeholder.
		return tulip.TXN_PREPARED, false
	}

	if phase == BGPP_COMMITTED {
		return tulip.TXN_COMMITTED, true
	}

	if phase == BGPP_ABORTED {
		return tulip.TXN_ABORTED, true
	}

	return tulip.TXN_PREPARED, true
}

func (gcoord *BackupGroupCoordinator) NextPrepareAction(rid uint64) uint64 {
	gcoord.mu.Lock()
	a := gcoord.gpp.action(rid)
	gcoord.mu.Unlock()
	return a
}

func (gcoord *BackupGroupCoordinator) Finalized() bool {
	gcoord.mu.Lock()
	done := gcoord.gpp.finalized()
	gcoord.mu.Unlock()
	return done
}

func (gcoord *BackupGroupCoordinator) GetPwrs() (tulip.KVMap, bool) {
	gcoord.mu.Lock()
	pwrs, ok := gcoord.gpp.getPwrs()
	gcoord.mu.Unlock()
	return pwrs, ok
}

func (gcoord *BackupGroupCoordinator) Commit(ts uint64) {
	pwrs, ok := gcoord.GetPwrs()
	if !ok {
		// If the partial writes are not available, then there is nothing
		// left to do. The reason is that @tcoord.stabilize completes only
		// after all groups have reported their status, which can only be
		// TXN_COMMITTED or TXN_PREPARED at this point. For the former case,
		// the commit will eventually be applied by each replica; for the
		// latter case, the write set is guaranteed to exist.
		return
	}

	var leader = gcoord.GetLeader()
	gcoord.SendCommit(leader, ts, pwrs)
	primitive.Sleep(params.NS_RESEND_COMMIT)

	for !gcoord.Finalized() {
		// Retry with different leaders until success.
		leader = gcoord.ChangeLeader()
		gcoord.SendCommit(leader, ts, pwrs)
		primitive.Sleep(params.NS_RESEND_COMMIT)
	}
}

func (gcoord *BackupGroupCoordinator) Abort(ts uint64) {
	var leader = gcoord.GetLeader()
	gcoord.SendAbort(leader, ts)
	primitive.Sleep(params.NS_RESEND_ABORT)

	for !gcoord.Finalized() {
		// Retry with different leaders until success.
		leader = gcoord.ChangeLeader()
		gcoord.SendAbort(leader, ts)
		primitive.Sleep(params.NS_RESEND_ABORT)
	}
}

func (gcoord *BackupGroupCoordinator) ChangeLeader() uint64 {
	gcoord.mu.Lock()
	idxleader := (gcoord.idxleader + 1) % uint64(len(gcoord.rps))
	gcoord.idxleader = idxleader
	gcoord.mu.Unlock()
	return gcoord.rps[idxleader]
}

func (gcoord *BackupGroupCoordinator) GetLeader() uint64 {
	gcoord.mu.Lock()
	idxleader := gcoord.idxleader
	gcoord.mu.Unlock()
	return gcoord.rps[idxleader]
}

func (gcoord *BackupGroupCoordinator) ResponseSession(rid uint64) {
	for {
		data, ok := gcoord.Receive(rid)
		if !ok {
			// Try to re-establish a connection on failure.
			primitive.Sleep(params.NS_RECONNECT)
			continue
		}

		msg := message.DecodeTxnResponse(data)
		kind := msg.Kind

		if gcoord.ts != msg.Timestamp {
			continue
		}

		gcoord.mu.Lock()

		gpp := gcoord.gpp

		if kind == message.MSG_TXN_INQUIRE {
			// This is a proof artifact since any message received in this
			// session should be delivered to @gcoord.cid.
			if gcoord.cid.GroupID == msg.CoordID.GroupID &&
				gcoord.cid.ReplicaID == msg.CoordID.ReplicaID &&
				gcoord.rank == msg.Rank {
				pp := tulip.PrepareProposal{
					Rank     : msg.RankLast,
					Prepared : msg.Prepared,
				}
				gpp.processInquireResult(msg.ReplicaID, pp, msg.Validated, msg.PartialWrites, msg.Result)
			}
		} else if kind == message.MSG_TXN_VALIDATE {
			gpp.processValidateResult(msg.ReplicaID, msg.Result)
		} else if kind == message.MSG_TXN_PREPARE {
			if gcoord.rank == msg.Rank {
				gpp.processPrepareResult(msg.ReplicaID, msg.Result)
			}
		} else if kind == message.MSG_TXN_UNPREPARE {
			if gcoord.rank == msg.Rank {
				gpp.processUnprepareResult(msg.ReplicaID, msg.Result)
			}
		} else if kind == message.MSG_TXN_REFRESH {
			// No reponse message for REFRESH.
		} else if kind == message.MSG_TXN_COMMIT || kind == message.MSG_TXN_ABORT {
			// Not using msg.Timestamp might be an issue in the proof without an
			// invariant saying that message sent through this connection can
			// only be of that of the transaction we're finalizing here.
			gpp.processFinalizationResult(msg.Result)
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

func (gcoord *BackupGroupCoordinator) Send(rid uint64, data []byte) {
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

func (gcoord *BackupGroupCoordinator) Receive(rid uint64) ([]byte, bool) {
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

func (gcoord *BackupGroupCoordinator) SendInquire(rid, ts, rank uint64, cid tulip.CoordID) {
	data := message.EncodeTxnInquireRequest(ts, rank, cid)
	gcoord.Send(rid, data)
}

func (gcoord *BackupGroupCoordinator) SendValidate(rid, ts, rank uint64, pwrs tulip.KVMap, ptgs []uint64) {
	data := message.EncodeTxnValidateRequest(ts, rank, pwrs, ptgs)
	gcoord.Send(rid, data)
}

func (gcoord *BackupGroupCoordinator) SendPrepare(rid, ts, rank uint64) {
	data := message.EncodeTxnPrepareRequest(ts, rank)
	gcoord.Send(rid, data)
}

func (gcoord *BackupGroupCoordinator) SendUnprepare(rid, ts, rank uint64) {
	data := message.EncodeTxnUnprepareRequest(ts, rank)
	gcoord.Send(rid, data)
}

func (gcoord *BackupGroupCoordinator) SendRefresh(rid, ts, rank uint64) {
	data := message.EncodeTxnRefreshRequest(ts, rank)
	gcoord.Send(rid, data)
}

func (gcoord *BackupGroupCoordinator) SendCommit(rid, ts uint64, pwrs tulip.KVMap) {
	data := message.EncodeTxnCommitRequest(ts, pwrs)
	gcoord.Send(rid, data)
}

func (gcoord *BackupGroupCoordinator) SendAbort(rid, ts uint64) {
	data := message.EncodeTxnAbortRequest(ts)
	gcoord.Send(rid, data)
}

func (gcoord *BackupGroupCoordinator) GetConnection(rid uint64) (grove_ffi.Connection, bool) {
	gcoord.mu.Lock()
	conn, ok := gcoord.conns[rid]
	gcoord.mu.Unlock()
	return conn, ok
}

func (gcoord *BackupGroupCoordinator) Connect(rid uint64) bool {
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

func (gcoord *BackupGroupCoordinator) ConnectAll() {
	for _, rid := range(gcoord.rps) {
		gcoord.Connect(rid)
	}
}

type BackupTxnCoordinator struct {
	// Timestamp of the transaction this backup coordinator tries to finalize.
	ts      uint64
	// Ranks of this backup coordinator.
	rank    uint64
	// Participant groups.
	ptgs    []uint64
	// Group coordinators, one for each participant group.
	gcoords map[uint64]*BackupGroupCoordinator
	// Global prophecy variable (for verification purpose).
	proph   primitive.ProphId
}

func Start(ts, rank uint64, cid tulip.CoordID, ptgs []uint64, gaddrm tulip.AddressMaps, leader uint64, proph primitive.ProphId) *BackupTxnCoordinator {
	gcoords := make(map[uint64]*BackupGroupCoordinator)

	// Create a backup group coordinator for each participant group.
	for _, gid := range(ptgs) {
		addrm := gaddrm[gid]
		gcoord := startBackupGroupCoordinator(addrm, cid, ts, rank)
		gcoords[gid] = gcoord
	}

	tcoord := &BackupTxnCoordinator{
		ts      : ts,
		rank    : rank,
		ptgs    : ptgs,
		gcoords : gcoords,
		proph   : proph,
	}
	return tcoord
}

// @Connect tries to create connections with all the replicas in each
// participant group.
func (tcoord *BackupTxnCoordinator) ConnectAll() {
	for _, gcoord := range(tcoord.gcoords) {
		gcoord.ConnectAll()
	}
}

func (tcoord *BackupTxnCoordinator) stabilize() (uint64, bool) {
	ts := tcoord.ts
	rank := tcoord.rank
	ptgs := tcoord.ptgs

	mu := new(sync.Mutex)
	cv := sync.NewCond(mu)
	// Number of groups that have responded (i.e., groups whose prepare status
	// is determined).
	var nr uint64 = 0
	// Number of groups that have prepared.
	var np uint64 = 0
	var st uint64 = tulip.TXN_PREPARED
	var vd bool = true

	for _, gid := range(ptgs) {
		gcoord := tcoord.gcoords[gid]

		go func() {
			stg, vdg := gcoord.Prepare(ts, rank, ptgs)

			mu.Lock()
			nr += 1
			if !vdg {
				vd = false
			} else if stg == tulip.TXN_PREPARED {
				np += 1
			} else {
				st = stg
			}
			mu.Unlock()
			cv.Signal()
		}()
	}

	// Wait until either a higher-ranked coordinator is found (i.e., as
	// indicated by @valid = false), or all participant groups have responded.
	//
	// A note on the difference between this method and @txn.preapre. Unlike
	// @txn.prepare() where it's OK (and good for performance) to terminate this
	// phase once the transaction status is determined, the backup txn
	// coordinator should wait until it finds out the status of all participant
	// groups to finalize the transaction outcome for every group.
	mu.Lock()
	for vd && nr != uint64(len(ptgs)) {
		cv.Wait()
	}

	// Use the invariant saying that "if @st = TXN_PREPARED, then @np = @nr" to
	// establish the postcondition.

	status := st
	valid := vd
	mu.Unlock()

	return status, valid
}

func mergeKVMap(mw, mr tulip.KVMap) {
	for k, v := range(mr) {
		mw[k] = v
	}
}

// TODO: This function should go to a trusted package (but not trusted_proph
// since that would create a circular dependency), and be implemented as a
// "ghost function".
func (tcoord *BackupTxnCoordinator) mergeWrites() (tulip.KVMap, bool) {
	var valid bool = true
	wrs := make(map[string]tulip.Value)

	for _, gid := range(tcoord.ptgs) {
		// TODO: To prove availability of the write set, we'll have to associate
		// a coordinator-local one-shot ghost variable to @gcoord.pwrsok. The
		// persistent resource is first given by @gcoord.WaitUntilPrepareDone,
		// and then is relayed to @gcoord.Prepare and finally to
		// @tcoord.stabilize.
		gcoord := tcoord.gcoords[gid]
		pwrs, ok := gcoord.GetPwrs()
		if ok {
			mergeKVMap(wrs, pwrs)
		} else {
			valid = false
		}
	}

	return wrs, valid
}

func (tcoord *BackupTxnCoordinator) resolve(status uint64) bool {
	if status == tulip.TXN_COMMITTED {
		return true
	}

	wrs, ok := tcoord.mergeWrites()
	if !ok {
		return false
	}

	// Logical action: Commit.
	trusted_proph.ResolveCommit(tcoord.proph, tcoord.ts, wrs)
	return true
}

func (tcoord *BackupTxnCoordinator) commit() {
	for _, gcoordloop := range(tcoord.gcoords) {
		gcoord := gcoordloop
		go func() {
			gcoord.Commit(tcoord.ts)
		}()
	}
}

func (tcoord *BackupTxnCoordinator) abort() {
	for _, gcoordloop := range(tcoord.gcoords) {
		gcoord := gcoordloop
		go func() {
			gcoord.Abort(tcoord.ts)
		}()
	}
}

// Top-level method of backup transaction coordinator.
func (tcoord *BackupTxnCoordinator) Finalize() {
	status, valid := tcoord.stabilize()

	if !valid {
		// Skip since a more recent backup txn coordinator is found.
		return
	}

	if status == tulip.TXN_ABORTED {
		tcoord.abort()
		return
	}

	// Possible @status: TXN_PREPARED and TXN_COMMITTED. Resolve the prophecy
	// variable if @status == TXN_PREPARED.

	if !tcoord.resolve(status) {
		return
	}

	tcoord.commit()
}
