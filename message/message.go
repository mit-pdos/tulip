package message

import (
	"github.com/mit-pdos/tulip/tulip"
)

///
/// Transaction messages.
///

type TxnRequest struct {
	Kind              uint64
	Timestamp         uint64
	Rank              uint64
	PartialWrites     tulip.KVMap
	ParticipantGroups []uint64
}

type TxnResponse struct {
	Kind      uint64
	Timestamp uint64
	Result    uint64
	Key       string
	Version   tulip.Version
	Rank      uint64
	Prepared  bool
	Validated bool
	PartialWrites tulip.KVMap
}

const (
	MSG_TXN_READ         uint64 = 100
	MSG_TXN_FAST_PREPARE uint64 = 201
	MSG_TXN_VALIDATE     uint64 = 202
	MSG_TXN_PREPARE      uint64 = 203
	MSG_TXN_UNPREPARE    uint64 = 204
	MSG_TXN_QUERY        uint64 = 205
	MSG_TXN_INQUIRE      uint64 = 206
	MSG_TXN_REFRESH      uint64 = 210
	MSG_TXN_COMMIT       uint64 = 300
	MSG_TXN_ABORT        uint64 = 301
)

// TODO: Implement these.

func EncodeTxnRead(ts uint64, key string) []byte {
	return nil
}

func EncodeTxnFastPrepare(ts uint64, pwrs tulip.KVMap, ptgs []uint64) []byte {
	return nil
}

func DecodeTxnRequest(data []byte) TxnRequest {
	return TxnRequest{}
}

func DecodeTxnResponse(data []byte) TxnResponse {
	return TxnResponse{}
}

///
/// Paxos messages.
///

// [REQUEST-VOTE, Term, CommittedLSN]
// [APPEND-ENTRIES, Term, CommittedLSN, LSNEntries, Entries]
type PaxosRequest struct {
	Kind         uint64
	Term         uint64
	CommittedLSN uint64
	EntriesLSN   uint64
	Entries      []string
}

// [REQUEST-VOTE, NodeID, Term, TermEntries, Entries]
// [APPEND-ENTRIES, NodeID, Term, MatchedLSN]
type PaxosResponse struct {
	Kind        uint64
	NodeID      uint64
	Term        uint64
	TermEntries uint64
	Entries     []string
	MatchedLSN  uint64
}

const (
	MSG_PAXOS_REQUEST_VOTE   uint64 = 0
	MSG_PAXOS_APPEND_ENTRIES uint64 = 1
)

// TODO: implement these.

func DecodePaxosRequest(data []byte) PaxosRequest {
	return PaxosRequest{}
}

func DecodePaxosResponse(data []byte) PaxosResponse {
	return PaxosResponse{}
}

func EncodePaxosRequestVoteRequest(term uint64, lsnc uint64) []byte {
	return nil
}

func EncodePaxosRequestVoteResponse(nid, term, terma uint64, ents []string) []byte {
	return nil
}

func EncodePaxosAppendEntriesRequest(term uint64, lsnc, lsne uint64, ents []string) []byte {
	return nil
}

func EncodePaxosAppendEntriesResponse(nid, term uint64, lsn uint64) []byte {
	return nil
}

