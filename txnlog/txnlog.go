package txnlog

import (
	"github.com/mit-pdos/gokv/grove_ffi"
	"github.com/tchajed/marshal"
	"github.com/mit-pdos/tulip/tulip"
	"github.com/mit-pdos/tulip/paxos"
	"github.com/mit-pdos/tulip/util"
)

type Cmd struct {
	// Two kinds of command: commit and abort.
	Kind          uint64
	// Transaction timestamp (used in both).
	Timestamp     uint64
	// Transaction write-set in a certain group (used in commit).
	PartialWrites []tulip.WriteEntry
}

type TxnLog struct {
	px *paxos.Paxos
}

const (
	TXNLOG_ABORT  uint64 = 0
	TXNLOG_COMMIT uint64 = 1
)

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
func (log *TxnLog) SubmitCommit(ts uint64, pwrs []tulip.WriteEntry) (uint64, uint64) {
	bs := make([]byte, 0, 32)
	bs1 := marshal.WriteInt(bs, TXNLOG_COMMIT)
	bs2 := marshal.WriteInt(bs1, ts)
	data := util.EncodeKVMapFromSlice(bs2, pwrs)

	lsn, term := log.px.Submit(string(data))

	return lsn, term
}

// Arguments and return values: see description of @SubmitPrepare.
func (log *TxnLog) SubmitAbort(ts uint64) (uint64, uint64) {
	bs := make([]byte, 0, 8)
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

	if kind == TXNLOG_COMMIT {
		ts, bs2 := marshal.ReadInt(bs1)
		pwrs, _ := util.DecodeKVMapIntoSlice(bs2)
		cmd := Cmd{
			Kind          : TXNLOG_COMMIT,
			Timestamp     : ts,
			PartialWrites : pwrs,
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

func Start(nidme uint64, addrm map[uint64]grove_ffi.Address, fname string) *TxnLog {
	px := paxos.Start(nidme, addrm, fname)
	txnlog := &TxnLog{ px : px }
	return txnlog
}
