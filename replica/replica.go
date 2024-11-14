package replica

import (
	"sync"
	"github.com/goose-lang/primitive"
	"github.com/goose-lang/std"
	"github.com/mit-pdos/gokv/grove_ffi"
	"github.com/mit-pdos/tulip/tulip"
	"github.com/mit-pdos/tulip/index"
	"github.com/mit-pdos/tulip/txnlog"
	"github.com/mit-pdos/tulip/backup"
)

// Criterion for preparedness, either 1-a or 1-b is true, and 2 is true:
// (1-a) a fast quorum of nodes prepared at some rank 0,
// (1-b) a classic quorum of nodes prepared at some non-zero rank n.
// (2) a classic quorum of nodes validated.
//
// For unpreparedness, either one of the following conditions is true:
// (1-a) a fast quorum of nodes unprepared at some rank 0,
// (1-b) a classic quorum of nodes unprepared at some non-zero rank n.

type PrepareProposal struct {
	// Rank of the prepare proposal.
	rank uint64
	// Prepared or unprepared.
	dec  bool
}

type PrepareStatusEntry struct {
	// Lowest rank allowed to make a prepare proposal.
	rankl uint64
	// Currently accepted prepare proposal.
	prep  PrepareProposal
}

type Replica struct {
	// Mutex.
	mu *sync.Mutex
	// Replica ID.
	rid uint64
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
	// Prepare status table.
	pstbl  map[uint64]PrepareStatusEntry
	// Transaction status table; mapping from transaction timestamps to their
	// commit/abort status.
	txntbl map[uint64]bool
	// Mapping from keys to their prepare timestamps.
	ptsm  map[string]uint64
	// Mapping from keys to their smallest preparable timestamps.
	sptsm map[string]uint64
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
func (rp *Replica) queryTxnTermination(ts uint64) bool {
	_, terminated := rp.txntbl[ts]
	return terminated
}

func (rp *Replica) QueryTxnTermination(ts uint64) bool {
	rp.mu.Lock()
	terminated := rp.queryTxnTermination(ts)
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
	committed := rp.QueryTxnTermination(ts)

	if committed {
		return true
	}
	
	lsn, term := rp.txnlog.SubmitCommit(ts, pwrs)
	if lsn == 0 {
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
	aborted := rp.QueryTxnTermination(ts)

	if aborted {
		return true
	}

	lsn, term := rp.txnlog.SubmitAbort(ts)
	if lsn == 0 {
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
func (rp *Replica) Read(ts uint64, key string) (tulip.Version, bool) {
	tpl := rp.idx.GetTuple(key)

	verfast := tpl.ReadVersion(ts)

	if verfast.Timestamp == 0 {
		// Fast-path read.
		return verfast, true
	}

	rp.mu.Lock()

	ok := rp.readableKey(ts, key)
	if !ok {
		// Trying to read a tuple that is locked by a lower-timestamp
		// transaction. This read has to fail because the value to be read is
		// undetermined---the prepared transaction might or might not commit.
		rp.mu.Unlock()
		return tulip.Version{}, false
	}

	ver := tpl.ReadVersion(ts)

	if ver.Timestamp == 0 {
		// Fast-path read.
		rp.mu.Unlock()
		return ver, true
	}

	// Slow-path read.
	bumped := rp.bumpKey(ts, key)
	if bumped {
		rp.logRead(ts, key)
	}

	rp.mu.Unlock()
	return ver, true
}

func (rp *Replica) logRead(ts uint64, key string) {
	// TODO: Create an inconsistent log entry for reading @key at @ts.
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
func (rp *Replica) validate(ts uint64, pwrs []tulip.WriteEntry, ptgs []uint64) uint64 {
	// Check if the transaction has aborted or committed. If so, returns the
	// status immediately.
	cmted, done := rp.txntbl[ts]
	if done {
		if cmted {
			return tulip.REPLICA_COMMITTED_TXN
		} else {
			return tulip.REPLICA_ABORTED_TXN
		}
	}

	// Check if the replica has already validated this transaction.
	_, validated := rp.prepm[ts]
	if validated {
		return tulip.REPLICA_OK
	}

	// Validate timestamps.
	acquired := rp.acquire(ts, pwrs)
	if !acquired {
		return tulip.REPLICA_FAILED_VALIDATION
	}

	// Record the write set and the participant groups.
	rp.prepm[ts] = pwrs
	// rp.ptgsm[ts] = ptgs

	// Logical action: Validate(@ts, @pwrs, @ptgs).
	rp.logValidate(ts, pwrs, ptgs)

	return tulip.REPLICA_OK
}

func (rp *Replica) logValidate(ts uint64, pwrs []tulip.WriteEntry, ptgs []uint64) {
	// TODO: Create an inconsistent log entry for validating @ts with @pwrs and @ptgs.
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
	cmted, done := rp.txntbl[ts]
	if done {
		if cmted {
			return tulip.REPLICA_COMMITTED_TXN
		} else {
			return tulip.REPLICA_ABORTED_TXN
		}
	}

	// Check if the coordinator is the most recent one. If not, report the
	// existence of a more recent coordinator.
	_, rank, dec, ok := rp.probe(ts)
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
	acquired := rp.acquire(ts, pwrs)

	// Update prepare status table to record that @ts is prepared or unprepared
	// at rank 0.
	rp.accept(ts, 0, acquired)

	if !acquired {
		return tulip.REPLICA_FAILED_VALIDATION
	}

	// Record the write set and the participant groups.
	rp.prepm[ts] = pwrs
	// rp.ptgsm[ts] = ptgs

	return tulip.REPLICA_OK
}

func (rp *Replica) FastPrepare(ts uint64, pwrs []tulip.WriteEntry, ptgs []uint64) uint64 {
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
	cmted, done := rp.txntbl[ts]
	if done {
		if cmted {
			return tulip.REPLICA_COMMITTED_TXN
		} else {
			return tulip.REPLICA_ABORTED_TXN
		}
	}

	// Check if the coordinator is the most recent one. If not, report the
	// existence of a more recent coordinator.
	rankl, _, _, ok := rp.probe(ts)
	if ok && rank < rankl {
		return tulip.REPLICA_STALE_COORDINATOR
	}

	// Update prepare status table to record that @ts is prepared at @rank.
	rp.accept(ts, rank, dec)

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

func (rp *Replica) inquire(ts uint64, rank uint64) (PrepareProposal, bool, []tulip.WriteEntry, uint64) {
	// Check if the transaction has aborted or committed. If so, returns the
	// status immediately.
	cmted, done := rp.txntbl[ts]
	if done {
		if cmted {
			return PrepareProposal{}, false, nil, tulip.REPLICA_COMMITTED_TXN
		} else {
			return PrepareProposal{}, false, nil, tulip.REPLICA_ABORTED_TXN
		}
	}

	// Check if @rank is still available. Note the difference between this
	// method and @acceptPreparedness: The case where @rank = @ps.rankl
	// indicates another replica's attempt to become the coordinator at @rank,
	// which should be rejected. Note the rank setup: rank 0 and 1 are reserved
	// for the client (similarly to Paxos's ballot assignment), and the others
	// are contended by replicas (similarly to Raft's voting process).
	ps, ok := rp.pstbl[ts]
	if ok && rank <= ps.rankl {
		return PrepareProposal{}, false, nil, tulip.REPLICA_INVALID_RANK
	}

	// Note that in the case where the fast path is not taken (i.e., @ok =
	// false), we want (0, false), which happens to be the zero-value.
	pp := ps.prep

	// Update the lowest acceptable rank.
	psnew := PrepareStatusEntry{
		rankl : rank,
		prep  : pp,
	}
	rp.pstbl[ts] = psnew

	// Check whether the transaction has validated.
	pwrs, vd := rp.prepm[ts]

	// Return the last accepted prepare decision.
	return pp, vd, pwrs, tulip.REPLICA_OK
}

func (rp *Replica) Inquire(ts uint64, rank uint64) (PrepareProposal, bool, []tulip.WriteEntry, uint64) {
	rp.mu.Lock()
	pp, vd, pwrs, res := rp.inquire(ts, rank)
	rp.refresh(ts, rank)
	rp.mu.Unlock()
	return pp, vd, pwrs, res
}

func (rp *Replica) query(ts uint64, rank uint64) uint64 {
	cmted, done := rp.txntbl[ts]
	if done {
		if cmted {
			return tulip.REPLICA_COMMITTED_TXN
		} else {
			return tulip.REPLICA_ABORTED_TXN
		}
	}

	// Check if the coordinator is the most recent one. If not, report the
	// existence of a more recent coordinator.
	rankl, _, _, ok := rp.probe(ts)
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
	committed := rp.queryTxnTermination(ts)
	if committed {
		return
	}

	rp.multiwrite(ts, pwrs)

	rp.txntbl[ts] = true

	// With PCR, a replica might receive a commit even if it is not prepared.
	_, prepared := rp.prepm[ts]
	if prepared {
		rp.release(pwrs)
		delete(rp.prepm, ts)
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
	aborted := rp.queryTxnTermination(ts)
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

func (rp *Replica) apply(cmd txnlog.Cmd) {	
	if cmd.Kind == 0 {
		// no-op
	} else if cmd.Kind == 1 {
		rp.applyCommit(cmd.Timestamp, cmd.PartialWrites)
	} else {
		rp.applyAbort(cmd.Timestamp)
	}
}

func (rp *Replica) Start() {
	rp.mu.Lock()

	for {
		lsn := std.SumAssumeNoOverflow(rp.lsna, 1)
		// TODO: a more efficient interface would return multiple safe commands
		// at once (so as to reduce the frequency of acquiring Paxos mutex).

		// Ghost action: Learn a list of new commands.
		cmd, ok := rp.txnlog.Lookup(lsn)

		if !ok {
			// Sleep for 1 ms.
			rp.mu.Unlock()
			primitive.Sleep(1 * 1000000)
			rp.mu.Lock()
			continue
		}

		rp.apply(cmd)

		rp.lsna = lsn
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
	ps := rp.pstbl[ts]
	rank := ps.rankl + 1
	// Obtain the participant groups of transaction @ts.
	ptgs := rp.ptgsm[ts]
	tcoord := backup.MkBackupTxnCoordinator(ts, rank, ptgs, rp.rps, rp.leader)
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
	pp := PrepareProposal{
		rank : rank,
		dec  : dec,
	}
	psnew := PrepareStatusEntry{
		rankl : rank + 1,
		prep  : pp,
	}
	rp.pstbl[ts] = psnew
}

func (rp *Replica) probe(ts uint64) (uint64, uint64, bool, bool) {
	ps, ok := rp.pstbl[ts]
	pp := ps.prep
	return ps.rankl, pp.rank, pp.dec, ok
}
