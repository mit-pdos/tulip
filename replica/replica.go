package replica

import (
	"fmt"
	"sync"
	"github.com/goose-lang/primitive"
	"github.com/goose-lang/std"
	"github.com/mit-pdos/gokv/grove_ffi"
	"github.com/mit-pdos/tulip/backup"
	"github.com/mit-pdos/tulip/index"
	"github.com/mit-pdos/tulip/message"
	"github.com/mit-pdos/tulip/tulip"
	"github.com/mit-pdos/tulip/txnlog"
	"github.com/mit-pdos/tulip/util"
	"github.com/tchajed/marshal"
)

// Criterion for preparedness, either 1-a or 1-b is true, and 2 is true:
// (1-a) a fast quorum of nodes prepared at some rank 0,
// (1-b) a classic quorum of nodes prepared at some non-zero rank n.
// (2) a classic quorum of nodes validated.
//
// For unpreparedness, either one of the following conditions is true:
// (1-a) a fast quorum of nodes unprepared at some rank 0,
// (1-b) a classic quorum of nodes unprepared at some non-zero rank n.

type Replica struct {
	// Mutex.
	mu     *sync.Mutex
	// Group ID.
	gid    uint64
	// Replica ID.
	rid    uint64
	// Address of this replica.
	addr   grove_ffi.Address
	// Name of the write-ahead log file.
	fname  string
	// Replicated transaction log.
	txnlog *txnlog.TxnLog
	//
	// Fields below are application states.
	//
	// LSN up to which all commands have been applied.
	lsna   uint64
	// Write sets of validated transactions.
	prepm  map[uint64][]tulip.WriteEntry
	// Participant groups of validated transactions.
	ptgsm  map[uint64][]uint64
	// Prepare proposal table.
	pstbl  map[uint64]tulip.PrepareProposal
	// Lowest rank allowed to make a prepare proposal for each transaction.
	rktbl  map[uint64]uint64
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
	gaddrm tulip.AddressMaps
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
func (rp *Replica) Commit(ts uint64, pwrs []tulip.WriteEntry) bool {
	// Query the transaction table. Note that if there's an entry for @ts in
	// @txntbl, then transaction @ts can only be committed. That's why we're not
	// even reading the value of entry.
	committed := rp.Terminated(ts)

	if committed {
		return true
	}
	
	lsn, term := rp.txnlog.SubmitCommit(ts, pwrs)
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

// Arguments:
// @ts: Transaction timestamp.
// @key: Key to be read.
//
// Return values:
// @ver: If @ver.Timestamp = 0, then this is a fast-path read---the value at @ts
// has been determined to be @ver.Value. Otherwise, this is a slow-path read,
// the replica promises not to accept prepare requests from transactions that
// modifies this tuple and whose timestamp lies within @ver.Timestamp and @ts.
//
// @ok: @ver is meaningful iff @ok is true.
//
// Design note:
//
// 1. It might seem redundant and inefficient to call @tpl.ReadVersion twice for
// each @rp.Read, but the point is that the first one is called without holding
// the global replica lock, which improves the latency for a fast-read, and
// throughput for non-conflicting fast-reads. An alternative design is to remove
// the first part at all, which favors slow-reads.
//
// 2. Right now the index is still a global lock; ideally we should also shard
// the index lock as done in vMVCC. However, the index lock should be held
// relatively short compared to the replica lock, so the performance impact
// should be less.
func (rp *Replica) Read(ts uint64, key string) (tulip.Version, bool, bool) {
	tpl := rp.idx.GetTuple(key)

	v1, slow1 := tpl.ReadVersion(ts)

	if !slow1 {
		// Fast-path read.
		return v1, false, true
	}

	rp.mu.Lock()

	ok := rp.readableKey(ts, key)
	if !ok {
		// Trying to read a tuple that is locked by a lower-timestamp
		// transaction. This read has to fail because the value to be read is
		// undetermined---the prepared transaction might or might not commit.
		rp.mu.Unlock()
		return tulip.Version{}, false, false
	}

	v2, slow2 := tpl.ReadVersion(ts)

	if !slow2 {
		// Fast-path read.
		rp.mu.Unlock()
		return v2, false, true
	}

	// Slow-path read.
	rp.bumpKey(ts, key)

	// TODO: An optimization is to create a log entry iff the smallest
	// preparable timestamp is actually bumped, which can be checked with the
	// return value of @rp.bumpKey.

	// Logical actions: Execute() and then LocalRead(@ts, @key)
	logRead(rp.fname, rp.lsna, ts, key)

	rp.mu.Unlock()
	return v2, true, true
}

func (rp *Replica) acquire(ts uint64, pwrs []tulip.WriteEntry) {
	for _, ent := range(pwrs) {
		rp.acquireKey(ts, ent.Key)
	}
}

func (rp *Replica) tryAcquire(ts uint64, pwrs []tulip.WriteEntry) bool {
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
	rp.acquire(ts, pwrs)

	return true
}

func (rp *Replica) memorize(ts uint64, pwrs []tulip.WriteEntry, ptgs []uint64) {
	rp.prepm[ts] = pwrs
	rp.ptgsm[ts] = ptgs
}

func (rp *Replica) erase(ts uint64) {
	delete(rp.prepm, ts)
	delete(rp.ptgsm, ts)
}

// Arguments:
// @ts: Transaction timestamp.
// @pwrs: Write set of transaction @ts.
// @ptgs: Participant groups of transaction @ts.
//
// Return values:
// @error: Error code.
func (rp *Replica) validate(ts uint64, pwrs []tulip.WriteEntry, ptgs []uint64) uint64 {
	// Check if the transaction has aborted or committed. If so, returns the
	// status immediately.
	res, final := rp.finalized(ts)
	if final {
		return res
	}

	// Check if the replica has already validated this transaction.
	_, validated := rp.prepm[ts]
	if validated {
		return tulip.REPLICA_OK
	}

	// Validate timestamps.
	acquired := rp.tryAcquire(ts, pwrs)
	if !acquired {
		return tulip.REPLICA_FAILED_VALIDATION
	}

	// Logical action: Validate(@ts, @pwrs, @ptgs).
	logAcquire(rp.fname, rp.lsna, ts, pwrs, ptgs)

	// Record the write set and the participant groups.
	rp.memorize(ts, pwrs, ptgs)

	return tulip.REPLICA_OK
}

func (rp *Replica) Validate(ts uint64, rank uint64, pwrs []tulip.WriteEntry, ptgs []uint64) uint64 {
	rp.mu.Lock()
	res := rp.validate(ts, pwrs, ptgs)
	rp.refresh(ts, rank)
	rp.mu.Unlock()
	return res
}

// Arguments:
// @ts: Transaction timestamp.
//
// @pwrs: Transaction write set.
//
// Return values:
//
// @error: Error code.
func (rp *Replica) fastPrepare(ts uint64, pwrs []tulip.WriteEntry, ptgs []uint64) uint64 {
	// Check if the transaction has aborted or committed. If so, returns the
	// status immediately.
	res, final := rp.finalized(ts)
	if final {
		return res
	}

	// Check if the coordinator is the most recent one. If not, report the
	// existence of a more recent coordinator.
	rank, dec, ok := rp.lastProposal(ts)
	if ok {
		if 0 < rank {
			// TODO: This would be a performance problem if @pp.rank = 1 (i.e.,
			// txn client's slow-path prepare) since the client would stops its
			// 2PC on receiving such response. For now the ad-hoc fix is to not
			// respond to the client in this case, but should figure out a more
			// efficient design.
			return tulip.REPLICA_STALE_COORDINATOR
		}
		if !dec {
			return tulip.REPLICA_FAILED_VALIDATION
		}
		return tulip.REPLICA_OK
	}

	// If the replica has validated this transaction, but no corresponding
	// prepare proposal entry (as is the case after passing the conditional
	// above), this means the client has already proceeded to the slow path, and
	// hence there's nothing more to be done with this fast-prepare.
	_, validated := rp.prepm[ts]
	if validated {
		return tulip.REPLICA_STALE_COORDINATOR
	}

	// Validate timestamps.
	acquired := rp.tryAcquire(ts, pwrs)

	// Update prepare status table to record that @ts is prepared or unprepared
	// at rank 0.
	rp.accept(ts, 0, acquired)

	if !acquired {
		// Logical actions: Execute() and then Accept(@ts, @0, @false).
		logAccept(rp.fname, rp.lsna, ts, 0, false)

		return tulip.REPLICA_FAILED_VALIDATION
	}

	// Logical actions: Execute() and then Validate(@ts, @pwrs, @ptgs) and
	// Accept(@ts, @0, @true).
	logFastPrepare(rp.fname, rp.lsna, ts, pwrs, ptgs)

	// Record the write set and the participant groups.
	rp.memorize(ts, pwrs, ptgs)

	return tulip.REPLICA_OK
}

func (rp *Replica) FastPrepare(ts uint64, pwrs []tulip.WriteEntry, ptgs []uint64) uint64 {
	// fmt.Printf("[replica] R%d receive fast prepare.\n", rp.rid)
	rp.mu.Lock()
	res := rp.fastPrepare(ts, pwrs, ptgs)
	rp.refresh(ts, 0)
	rp.mu.Unlock()
	return res
}


// Accept the prepare decision for @ts at @rank, if @rank is most recent.
//
// Arguments:
// @ts: Transaction timestamp.
// @rank: Coordinator rank.
// @dec: Prepared or unprepared.
//
// Return values:
// @error: Error code.
func (rp *Replica) tryAccept(ts uint64, rank uint64, dec bool) uint64 {
	// Check if the transaction has aborted or committed. If so, returns the
	// status immediately.
	res, final := rp.finalized(ts)
	if final {
		return res
	}

	// Check if the coordinator is the most recent one. If not, report the
	// existence of a more recent coordinator.
	rankl, ok := rp.lowestRank(ts)
	if ok && rank < rankl {
		return tulip.REPLICA_STALE_COORDINATOR
	}

	// Update prepare status table to record that @ts is prepared at @rank.
	rp.accept(ts, rank, dec)

	// Logical actions: Execute() and then Accept(@ts, @rank, @dec).
	logAccept(rp.fname, rp.lsna, ts, rank, dec)

	return tulip.REPLICA_OK
}

func (rp *Replica) Prepare(ts uint64, rank uint64) uint64 {
	rp.mu.Lock()
	res := rp.tryAccept(ts, rank, true)
	rp.refresh(ts, rank)
	rp.mu.Unlock()
	return res
}

func (rp *Replica) Unprepare(ts uint64, rank uint64) uint64 {
	rp.mu.Lock()
	res := rp.tryAccept(ts, rank, false)
	rp.refresh(ts, rank)
	rp.mu.Unlock()
	return res
}

func (rp *Replica) advance(ts uint64, rank uint64) {
	rp.rktbl[ts] = rank
	_, ok := rp.pstbl[ts]
	if !ok {
		pp := tulip.PrepareProposal{
			Rank     : 0,
			Prepared : false,
		}
		rp.pstbl[ts] = pp
	}
}

func (rp *Replica) inquire(ts uint64, rank uint64) (tulip.PrepareProposal, bool, []tulip.WriteEntry, uint64) {
	// Check if the transaction has aborted or committed. If so, returns the
	// status immediately.
	res, done := rp.finalized(ts)
	if done {
		return tulip.PrepareProposal{}, false, nil, res
	}

	// Check if @rank is still available. Note the difference between this
	// method and @tryAccept: The case where @rank = @ps.rankl indicates another
	// replica's attempt to become the coordinator at @rank, which should be
	// rejected. Note the rank setup: rank 0 and 1 are reserved for the client
	// (similarly to Paxos's ballot assignment), and the others are contended by
	// replicas (similarly to Raft's voting process).
	rankl, ok := rp.lowestRank(ts)
	if ok && rank <= rankl {
		return tulip.PrepareProposal{}, false, nil, tulip.REPLICA_STALE_COORDINATOR
	}

	// Note that in the case where the fast path is not taken (i.e., @ok =
	// false), we want (0, false), which happens to be the zero-value.
	pp := rp.pstbl[ts]

	// Update the lowest acceptable rank.
	rp.advance(ts, rank)

	// Check whether the transaction has validated.
	pwrs, vd := rp.prepm[ts]

	// Return the last accepted prepare decision.
	return pp, vd, pwrs, tulip.REPLICA_OK
}

func (rp *Replica) Inquire(ts uint64, rank uint64) (tulip.PrepareProposal, bool, []tulip.WriteEntry, uint64) {
	rp.mu.Lock()
	pp, vd, pwrs, res := rp.inquire(ts, rank)
	rp.refresh(ts, rank)
	rp.mu.Unlock()
	return pp, vd, pwrs, res
}

func (rp *Replica) query(ts uint64, rank uint64) uint64 {
	res, final := rp.finalized(ts)
	if final {
		return res
	}

	// Check if the coordinator is the most recent one. If not, report the
	// existence of a more recent coordinator.
	rankl, ok := rp.lowestRank(ts)
	if ok && rank < rankl {
		return tulip.REPLICA_STALE_COORDINATOR
	}

	return tulip.REPLICA_OK
}

func (rp *Replica) Query(ts uint64, rank uint64) uint64 {
	rp.mu.Lock()
	res := rp.query(ts, rank)
	rp.refresh(ts, rank)
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

func (rp *Replica) applyCommit(ts uint64, pwrs []tulip.WriteEntry) {
	// Query the transaction table. Note that if there's an entry for @ts in
	// @txntbl, then transaction @ts can only be committed. That's why we're not
	// even reading the value of entry.
	committed := rp.terminated(ts)
	if committed {
		return
	}

	rp.multiwrite(ts, pwrs)

	rp.txntbl[ts] = true

	// With PCR, a replica might receive a commit even if it is not prepared on
	// this replica.
	_, prepared := rp.prepm[ts]
	if prepared {
		rp.release(pwrs)
		rp.erase(ts)
	}
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
		rp.erase(ts)
	}
}

func (rp *Replica) apply(cmd txnlog.Cmd) {	
	if cmd.Kind == txnlog.TXNLOG_COMMIT {
		// fmt.Printf("[replica %d] Apply commit(%d, %v).\n", rp.rid, cmd.Timestamp, cmd.PartialWrites)
		rp.applyCommit(cmd.Timestamp, cmd.PartialWrites)
	} else if cmd.Kind == txnlog.TXNLOG_ABORT {
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
			primitive.Sleep(1 * 1000000)
			rp.mu.Lock()
			continue
		}

		rp.apply(cmd)

		rp.lsna = std.SumAssumeNoOverflow(rp.lsna, 1)
	}
}

// The workflow of a backup transaction coordinator is described below:
// 1. A replica with txn @ts prepared on it has not received a keep-alive
// message for a while.
// 2. The replica calls @mkBackupTxnCoordinator(ts) to create @tcoord.
// 3. Call @tcoord.Finalize().

func (rp *Replica) StartBackupTxnCoordinator(ts uint64) {
	rp.mu.Lock()
	// Start the coordinator at a rank one above the largest seen so far.
	rank := rp.rktbl[ts] + 1
	// Obtain the participant groups of transaction @ts.
	ptgs := rp.ptgsm[ts]
	cid := tulip.CoordID { GroupID: rp.gid, ReplicaID: rp.rid }
	tcoord := backup.MkBackupTxnCoordinator(ts, rank, cid, ptgs, rp.gaddrm, rp.leader)
	tcoord.ConnectAll()
	rp.mu.Unlock()
	tcoord.Finalize()
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

func (rp *Replica) accept(ts uint64, rank uint64, dec bool) {
	pp := tulip.PrepareProposal{
		Rank     : rank,
		Prepared : dec,
	}
	rp.pstbl[ts] = pp
	rp.rktbl[ts] = std.SumAssumeNoOverflow(rank, 1)
}

func (rp *Replica) lowestRank(ts uint64) (uint64, bool) {
	rank, ok := rp.rktbl[ts]
	return rank, ok
}

func (rp *Replica) lastProposal(ts uint64) (uint64, bool, bool) {
	ps, ok := rp.pstbl[ts]
	return ps.Rank, ps.Prepared, ok
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

		req  := message.DecodeTxnRequest(ret.Data)
		kind := req.Kind
		ts   := req.Timestamp

		if kind == message.MSG_TXN_READ {
			key := req.Key
			ver, slow, ok := rp.Read(ts, key)
			if !ok {
				// We can optionally respond with an error message to request
				// clients resending.
				continue
			}
			data := message.EncodeTxnReadResponse(ts, rp.rid, key, ver, slow)
			grove_ffi.Send(conn, data)
		} else if kind == message.MSG_TXN_FAST_PREPARE {
			pwrs := req.PartialWrites
			ptgs := req.ParticipantGroups
			res := rp.FastPrepare(ts, pwrs, ptgs)
			data := message.EncodeTxnFastPrepareResponse(ts, rp.rid, res)
			grove_ffi.Send(conn, data)
		} else if kind == message.MSG_TXN_VALIDATE {
			rank := req.Rank
			pwrs := req.PartialWrites
			ptgs := req.ParticipantGroups
			res := rp.Validate(ts, rank, pwrs, ptgs)
			data := message.EncodeTxnValidateResponse(ts, rp.rid, res)
			grove_ffi.Send(conn, data)
		} else if kind == message.MSG_TXN_PREPARE {
			rank := req.Rank
			res := rp.Prepare(ts, rank)
			data := message.EncodeTxnPrepareResponse(ts, rank, rp.rid, res)
			grove_ffi.Send(conn, data)
		} else if kind == message.MSG_TXN_UNPREPARE {
			rank := req.Rank
			res := rp.Unprepare(ts, rank)
			data := message.EncodeTxnUnprepareResponse(ts, rank, rp.rid, res)
			grove_ffi.Send(conn, data)
		} else if kind == message.MSG_TXN_QUERY {
			rank := req.Rank
			res := rp.Query(ts, rank)
			data := message.EncodeTxnQueryResponse(ts, res)
			grove_ffi.Send(conn, data)
		} else if kind == message.MSG_TXN_COMMIT {
			pwrs := req.PartialWrites
			ok := rp.Commit(ts, pwrs)
			if ok {
				data := message.EncodeTxnCommitResponse(ts, tulip.REPLICA_COMMITTED_TXN)
				grove_ffi.Send(conn, data)
			} else {
				data := message.EncodeTxnCommitResponse(ts, tulip.REPLICA_WRONG_LEADER)
				grove_ffi.Send(conn, data)
			}
		} else if kind == message.MSG_TXN_ABORT {
			ok := rp.Abort(ts)
			if ok {
				data := message.EncodeTxnAbortResponse(ts, tulip.REPLICA_ABORTED_TXN)
				grove_ffi.Send(conn, data)
			} else {
				data := message.EncodeTxnAbortResponse(ts, tulip.REPLICA_WRONG_LEADER)
				grove_ffi.Send(conn, data)
			}
		} else if kind == message.MSG_DUMP_STATE {
			gid := req.Timestamp
			rp.DumpState(gid)
		} else if kind == message.MSG_FORCE_ELECTION {
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
	txnlog := txnlog.Start(rid, addrmpx, fnamepx)

	rp := &Replica{
		mu     : new(sync.Mutex),
		// TODO: also init gid
		rid    : rid,
		addr   : addr,
		fname  : fname,
		txnlog : txnlog,
		lsna   : 0,
		prepm  : make(map[uint64][]tulip.WriteEntry),
		ptgsm  : make(map[uint64][]uint64),
		pstbl  : make(map[uint64]tulip.PrepareProposal),
		rktbl  : make(map[uint64]uint64),
		txntbl : make(map[uint64]bool),
		ptsm   : make(map[string]uint64),
		sptsm  : make(map[string]uint64),
		idx    : index.MkIndex(),
	}

	rp.resume()

	go func() {
		rp.Serve()
	}()

	go func() {
		rp.Applier()
	}()

	return rp
}

// Argument:
// 1. @lsn: LSN of consistent command to replay.
func (rp *Replica) replay(lsn uint64) {
	var cmd txnlog.Cmd
	var ok bool = false
	for !ok {
		cmd, ok = rp.txnlog.Lookup(lsn)
	}

	rp.apply(cmd)
}

// Argument:
// 1. @lsnx and @lsny: LSN range [@lsnx, @lsny) of consistent commands to
// replay.
func (rp *Replica) replayBetween(lsnx, lsny uint64) {
	for lsn := lsnx; lsn < lsny; lsn++ {
		rp.replay(lsn)
	}
}

func (rp *Replica) resume() {
	// Set the starting LSN to 0.
	var lsnx uint64 = 0

	// Read the inconsistent log.
	var data = grove_ffi.FileRead(rp.fname)

	for 0 < uint64(len(data)) {
		lsny, bs1 := marshal.ReadInt(data)

		rp.replayBetween(lsnx, lsny)

		lsnx = lsny

		kind, bs2 := marshal.ReadInt(bs1)
		ts, bs3 := marshal.ReadInt(bs2)

		if kind == CMD_READ {
			key, bs4 := util.DecodeString(bs3)
			data = bs4
			// Apply read.
			rp.bumpKey(ts, key)
		} else if kind == CMD_ACQUIRE {
			pwrs, bs4 := util.DecodeKVMapIntoSlice(bs3)
			ptgs, bs5 := util.DecodeInts(bs4)
			data = bs5
			// Apply validate.
			rp.acquire(ts, pwrs)
			rp.memorize(ts, pwrs, ptgs)
		} else if kind == CMD_ADVANCE {
			rank, bs4 := marshal.ReadInt(bs3)
			data = bs4
			rp.advance(ts, rank)
		} else if kind == CMD_ACCEPT {
			rank, bs4 := marshal.ReadInt(bs3)
			dec, bs5 := marshal.ReadBool(bs4)
			data = bs5
			// Apply accept.
			rp.accept(ts, rank, dec)
		}
	}

	rp.lsna = lsnx
}

const (
	CMD_READ    uint64 = 0
	CMD_ACQUIRE uint64 = 1
	CMD_ADVANCE uint64 = 2
	CMD_ACCEPT  uint64 = 3
)

func logRead(fname string, lsn, ts uint64, key string) {
	// Create an inconsistent log entry for reading @key at @ts.
	bs := make([]byte, 0, 32)

	bs0 := marshal.WriteInt(bs, lsn)
	bs1 := marshal.WriteInt(bs0, CMD_READ)
	bs2 := marshal.WriteInt(bs1, ts)
	bs3 := util.EncodeString(bs2, key)

	grove_ffi.FileAppend(fname, bs3)
}

func logAcquire(fname string, lsn, ts uint64, pwrs []tulip.WriteEntry, ptgs []uint64) {
	// Create an inconsistent log entry for validating @ts with @pwrs and @ptgs.
	bs := make([]byte, 0, 64)

	bs0 := marshal.WriteInt(bs, lsn)
	bs1 := marshal.WriteInt(bs0, CMD_ACQUIRE)
	bs2 := marshal.WriteInt(bs1, ts)
	bs3 := util.EncodeKVMapFromSlice(bs2, pwrs)
	bs4 := util.EncodeInts(bs3, ptgs)

	grove_ffi.FileAppend(fname, bs4)
}

func logFastPrepare(fname string, lsn, ts uint64, pwrs []tulip.WriteEntry, ptgs []uint64) {
	// Create an inconsistent log entry for fast preparing @ts.
	bs := make([]byte, 0, 128)

	bs0 := marshal.WriteInt(bs, lsn)
	bs1 := marshal.WriteInt(bs0, CMD_ACQUIRE)
	bs2 := marshal.WriteInt(bs1, ts)
	bs3 := util.EncodeKVMapFromSlice(bs2, pwrs)
	bs4 := util.EncodeInts(bs3, ptgs)
	bs5 := marshal.WriteInt(bs4, lsn)
	bs6 := marshal.WriteInt(bs5, CMD_ACCEPT)
	bs7 := marshal.WriteInt(bs6, ts)
	bs8 := marshal.WriteInt(bs7, 0)
	bs9 := marshal.WriteBool(bs8, true)

	grove_ffi.FileAppend(fname, bs9)
}

func logAccept(fname string, lsn, ts uint64, rank uint64, dec bool) {
	// Create an inconsistent log entry for accepting prepare decision @dec for
	// @ts in @rank.
	bs := make([]byte, 0, 32)

	bs0 := marshal.WriteInt(bs, lsn)
	bs1 := marshal.WriteInt(bs0, CMD_ACCEPT)
	bs2 := marshal.WriteInt(bs1, ts)
	bs3 := marshal.WriteInt(bs2, rank)
	bs4 := marshal.WriteBool(bs3, dec)

	grove_ffi.FileAppend(fname, bs4)
}
