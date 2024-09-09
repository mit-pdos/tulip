package txnlog

import (
	// "sync"
	"github.com/mit-pdos/tulip/tulip"
)

type Cmd struct {
	// Four kinds of command: read, prepare, commit, abort.
	Kind          uint64
	// Transaction timestamp (used in all).
	Timestamp     uint64
	// Transaction write-set in a certain group (used in prepare and commit).
	PartialWrites []tulip.WriteEntry
	// Key (used in read).
	Key           string
}

type TxnLog struct {
	// TODO
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
func (log *TxnLog) SubmitCommit(ts uint64, pwrs []tulip.WriteEntry) (uint64, uint64) {
	// TODO: marshalling a commit command
	// TODO: invoke paxos.Submit()
	return 0, 0
}

// Arguments and return values: see description of @SubmitPrepare.
func (log *TxnLog) SubmitAbort(ts uint64) (uint64, uint64) {
	// TODO: marshalling a abort command
	// TODO: invoke paxos.Submit()
	return 0, 0
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
	// TODO: invoke paxos.WaitUntilSafe(lsn, term)
	// TODO: have some timeout here
	return false
}

// Argument:
// @lsn: Logical index of the queried command.
func (log *TxnLog) Lookup(lsn uint64) (Cmd, bool) {
	return Cmd{}, false
}
