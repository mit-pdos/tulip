package message

import (
	"github.com/tchajed/marshal"
	"github.com/mit-pdos/tulip/tulip"
	"github.com/mit-pdos/tulip/util"
)

///
/// Transaction messages.
///

type TxnRequest struct {
	Kind              uint64
	Timestamp         uint64
	Key               string
	Rank              uint64
	PartialWrites     []tulip.WriteEntry
	ParticipantGroups []uint64
}

type TxnResponse struct {
	Kind      uint64
	Timestamp uint64
	ReplicaID uint64
	Result    uint64
	Key       string
	Version   tulip.Version
	Rank      uint64
	Prepared  bool
	Validated bool
	Slow      bool
	PartialWrites tulip.KVMap
	CooordID  tulip.CoordID
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
	MSG_DUMP_STATE       uint64 = 10000
	MSG_FORCE_ELECTION   uint64 = 10001
)

// Read.

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

func EncodeTxnReadResponse(ts, rid uint64, key string, ver tulip.Version, slow bool) []byte {
	bs := make([]byte, 0, 32)
	bs1 := marshal.WriteInt(bs, MSG_TXN_READ)
	bs2 := marshal.WriteInt(bs1, ts)
	bs3 := marshal.WriteInt(bs2, rid)
	bs4 := util.EncodeString(bs3, key)
	bs5 := util.EncodeVersion(bs4, ver)
	data := marshal.WriteBool(bs5, slow)
	return data
}

func DecodeTxnReadResponse(bs []byte) TxnResponse {
	ts, bs1 := marshal.ReadInt(bs)
	rid, bs2 := marshal.ReadInt(bs1)
	key, bs3 := util.DecodeString(bs2)
	ver, bs4 := util.DecodeVersion(bs3)
	slow, _ := marshal.ReadBool(bs4)
	return TxnResponse{
		Kind      : MSG_TXN_READ,
		Timestamp : ts,
		ReplicaID : rid,
		Key       : key,
		Version   : ver,
		Slow      : slow,
	}
}

// Fast prepare.

func EncodeTxnFastPrepareRequest(ts uint64, pwrs tulip.KVMap, ptgs []uint64) []byte {
	bs := make([]byte, 0, 64)
	bs1 := marshal.WriteInt(bs, MSG_TXN_FAST_PREPARE)
	bs2 := marshal.WriteInt(bs1, ts)
	bs3 := util.EncodeKVMap(bs2, pwrs)
	data := util.EncodeInts(bs3, ptgs)
	return data
}

func DecodeTxnFastPrepareRequest(bs []byte) TxnRequest {
	ts, bs1 := marshal.ReadInt(bs)
	pwrs, bs2 := util.DecodeKVMapIntoSlice(bs1)
	ptgs, _ := util.DecodeInts(bs2)
	return TxnRequest{
		Kind              : MSG_TXN_FAST_PREPARE,
		Timestamp         : ts,
		PartialWrites     : pwrs,
		ParticipantGroups : ptgs,
	}
}

func EncodeTxnFastPrepareResponse(ts, rid, res uint64) []byte {
	bs := make([]byte, 0, 32)
	bs1 := marshal.WriteInt(bs, MSG_TXN_FAST_PREPARE)
	bs2 := marshal.WriteInt(bs1, ts)
	bs3 := marshal.WriteInt(bs2, rid)
	data := marshal.WriteInt(bs3, res)
	return data
}

func DecodeTxnFastPrepareResponse(bs []byte) TxnResponse {
	ts, bs1 := marshal.ReadInt(bs)
	rid, bs2 := marshal.ReadInt(bs1)
	res, _ := marshal.ReadInt(bs2)
	return TxnResponse{
		Kind      : MSG_TXN_FAST_PREPARE,
		Timestamp : ts,
		ReplicaID : rid,
		Result    : res,
	}
}

// Validate.

func EncodeTxnValidateRequest(ts, rank uint64, pwrs tulip.KVMap, ptgs []uint64) []byte {
	bs := make([]byte, 0, 64)
	bs1 := marshal.WriteInt(bs, MSG_TXN_VALIDATE)
	bs2 := marshal.WriteInt(bs1, ts)
	bs3 := marshal.WriteInt(bs2, rank)
	bs4 := util.EncodeKVMap(bs3, pwrs)
	data := util.EncodeInts(bs4, ptgs)
	return data
}

func DecodeTxnValidateRequest(bs []byte) TxnRequest {
	ts, bs1 := marshal.ReadInt(bs)
	rank, bs2 := marshal.ReadInt(bs1)
	pwrs, bs3 := util.DecodeKVMapIntoSlice(bs2)
	ptgs, _ := util.DecodeInts(bs3)
	return TxnRequest{
		Kind              : MSG_TXN_VALIDATE,
		Timestamp         : ts,
		Rank              : rank,
		PartialWrites     : pwrs,
		ParticipantGroups : ptgs,
	}
}

func EncodeTxnValidateResponse(ts, rid, res uint64) []byte {
	bs := make([]byte, 0, 32)
	bs1 := marshal.WriteInt(bs, MSG_TXN_VALIDATE)
	bs2 := marshal.WriteInt(bs1, ts)
	bs3 := marshal.WriteInt(bs2, rid)
	data := marshal.WriteInt(bs3, res)
	return data
}

func DecodeTxnValidateResponse(bs []byte) TxnResponse {
	ts, bs1 := marshal.ReadInt(bs)
	rid, bs2 := marshal.ReadInt(bs1)
	res, _ := marshal.ReadInt(bs2)
	return TxnResponse{
		Kind      : MSG_TXN_VALIDATE,
		Timestamp : ts,
		ReplicaID : rid,
		Result    : res,
	}
}

// Prepare.

func EncodeTxnPrepareRequest(ts, rank uint64) []byte {
	bs := make([]byte, 0, 24)
	bs1 := marshal.WriteInt(bs, MSG_TXN_PREPARE)
	bs2 := marshal.WriteInt(bs1, ts)
	data := marshal.WriteInt(bs2, rank)
	return data
}

func DecodeTxnPrepareRequest(bs []byte) TxnRequest {
	ts, bs1 := marshal.ReadInt(bs)
	rank, _ := marshal.ReadInt(bs1)
	return TxnRequest{
		Kind          : MSG_TXN_PREPARE,
		Timestamp     : ts,
		Rank          : rank,
	}
}

func EncodeTxnPrepareResponse(ts, rank, rid, res uint64) []byte {
	bs := make([]byte, 0, 32)
	bs1 := marshal.WriteInt(bs, MSG_TXN_PREPARE)
	bs2 := marshal.WriteInt(bs1, ts)
	bs3 := marshal.WriteInt(bs2, rank)
	bs4 := marshal.WriteInt(bs3, rid)
	data := marshal.WriteInt(bs4, res)
	return data
}

func DecodeTxnPrepareResponse(bs []byte) TxnResponse {
	ts, bs1 := marshal.ReadInt(bs)
	rank, bs2 := marshal.ReadInt(bs1)
	rid, bs3 := marshal.ReadInt(bs2)
	res, _ := marshal.ReadInt(bs3)
	return TxnResponse{
		Kind      : MSG_TXN_PREPARE,
		Timestamp : ts,
		Rank      : rank,
		ReplicaID : rid,
		Result    : res,
	}
}

// Unprepare.

func EncodeTxnUnprepareRequest(ts, rank uint64) []byte {
	bs := make([]byte, 0, 24)
	bs1 := marshal.WriteInt(bs, MSG_TXN_UNPREPARE)
	bs2 := marshal.WriteInt(bs1, ts)
	data := marshal.WriteInt(bs2, rank)
	return data
}

func DecodeTxnUnprepareRequest(bs []byte) TxnRequest {
	ts, bs1 := marshal.ReadInt(bs)
	rank, _ := marshal.ReadInt(bs1)
	return TxnRequest{
		Kind          : MSG_TXN_UNPREPARE,
		Timestamp     : ts,
		Rank          : rank,
	}
}

func EncodeTxnUnprepareResponse(ts, rank, rid, res uint64) []byte {
	bs := make([]byte, 0, 32)
	bs1 := marshal.WriteInt(bs, MSG_TXN_UNPREPARE)
	bs2 := marshal.WriteInt(bs1, ts)
	bs3 := marshal.WriteInt(bs2, rank)
	bs4 := marshal.WriteInt(bs3, rid)
	data := marshal.WriteInt(bs4, res)
	return data
}

func DecodeTxnUnprepareResponse(bs []byte) TxnResponse {
	ts, bs1 := marshal.ReadInt(bs)
	rank, bs2 := marshal.ReadInt(bs1)
	rid, bs3 := marshal.ReadInt(bs2)
	res, _ := marshal.ReadInt(bs3)
	return TxnResponse{
		Kind      : MSG_TXN_UNPREPARE,
		Timestamp : ts,
		Rank      : rank,
		ReplicaID : rid,
		Result    : res,
	}
}

// Query.

func EncodeTxnQueryRequest(ts, rank uint64) []byte {
	bs := make([]byte, 0, 24)
	bs1 := marshal.WriteInt(bs, MSG_TXN_QUERY)
	bs2 := marshal.WriteInt(bs1, ts)
	data := marshal.WriteInt(bs2, rank)
	return data
}

func DecodeTxnQueryRequest(bs []byte) TxnRequest {
	ts, bs1 := marshal.ReadInt(bs)
	rank, _ := marshal.ReadInt(bs1)
	return TxnRequest{
		Kind          : MSG_TXN_QUERY,
		Timestamp     : ts,
		Rank          : rank,
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

// Inquire.

func EncodeTxnInquireRequest(ts, rank uint64) []byte {
	bs := make([]byte, 0, 24)
	bs1 := marshal.WriteInt(bs, MSG_TXN_QUERY)
	bs2 := marshal.WriteInt(bs1, ts)
	data := marshal.WriteInt(bs2, rank)
	return data
}

func DecodeTxnInquireRequest(bs []byte) TxnRequest {
	ts, bs1 := marshal.ReadInt(bs)
	rank, _ := marshal.ReadInt(bs1)
	return TxnRequest{
		Kind      : MSG_TXN_INQUIRE,
		Timestamp : ts,
		Rank      : rank,
	}
}

func EncodeTxnInquireResponse(pp tulip.PrepareProposal, vd bool, pwrs []tulip.WriteEntry, res uint64) []byte {
	bs   := make([]byte, 0, 64)
	bs1  := util.EncodePrepareProposal(bs, pp)
	bs2  := marshal.WriteBool(bs1, vd)
	bs3  := util.EncodeKVMapFromSlice(bs2, pwrs)
	data := marshal.WriteInt(bs3, res)
	return data
}

func DecodeTxnInquireResponse(bs []byte) TxnResponse {
	pp, bs1 := util.DecodePrepareProposal(bs)
	vd, bs2 := marshal.ReadBool(bs1)
	pwrs, bs3 := util.DecodeKVMap(bs2)
	res, _ := marshal.ReadInt(bs3)
	return TxnResponse{
		Kind          : MSG_TXN_INQUIRE,
		Rank          : pp.Rank,
		Prepared      : pp.Prepared,
		Validated     : vd,
		PartialWrites : pwrs,
		Result        : res,
	}
}

// Refresh.

func EncodeTxnRefreshRequest(ts, rank uint64) []byte {
	bs := make([]byte, 0, 24)
	bs1 := marshal.WriteInt(bs, MSG_TXN_REFRESH)
	bs2 := marshal.WriteInt(bs1, ts)
	data := marshal.WriteInt(bs2, rank)
	return data
}

func DecodeTxnRefreshRequest(bs []byte) TxnRequest {
	ts, bs1 := marshal.ReadInt(bs)
	rank, _ := marshal.ReadInt(bs1)
	return TxnRequest{
		Kind      : MSG_TXN_REFRESH,
		Timestamp : ts,
		Rank      : rank,
	}
}

// Commit.

func EncodeTxnCommitRequest(ts uint64, pwrs tulip.KVMap) []byte {
	bs := make([]byte, 0, 64)
	bs1 := marshal.WriteInt(bs, MSG_TXN_COMMIT)
	bs2 := marshal.WriteInt(bs1, ts)
	data := util.EncodeKVMap(bs2, pwrs)
	return data
}

func DecodeTxnCommitRequest(bs []byte) TxnRequest {
	ts, bs1 := marshal.ReadInt(bs)
	pwrs, _ := util.DecodeKVMapIntoSlice(bs1)
	return TxnRequest{
		Kind          : MSG_TXN_COMMIT,
		Timestamp     : ts,
		PartialWrites : pwrs,
	}
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

// Abort.

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


// Dump state.

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

// Force election.

func EncodeForceElectionRequest() []byte {
	bs := make([]byte, 0, 8)
	data := marshal.WriteInt(bs, MSG_FORCE_ELECTION)
	return data
}

func DecodeForceElectionRequest() TxnRequest {
	return TxnRequest{ Kind : MSG_FORCE_ELECTION }
}

func DecodeTxnRequest(bs []byte) TxnRequest {
	kind, bs1 := marshal.ReadInt(bs)

	if kind == MSG_TXN_READ {
		return DecodeTxnReadRequest(bs1)
	}
	if kind == MSG_TXN_FAST_PREPARE {
		return DecodeTxnFastPrepareRequest(bs1)
	}
	if kind == MSG_TXN_VALIDATE {
		return DecodeTxnValidateRequest(bs1)
	}
	if kind == MSG_TXN_PREPARE {
		return DecodeTxnPrepareRequest(bs1)
	}
	if kind == MSG_TXN_UNPREPARE {
		return DecodeTxnUnprepareRequest(bs1)
	}
	if kind == MSG_TXN_QUERY {
		return DecodeTxnQueryRequest(bs1)
	}
	if kind == MSG_TXN_REFRESH {
		return DecodeTxnRefreshRequest(bs1)
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
	if kind == MSG_TXN_FAST_PREPARE {
		return DecodeTxnFastPrepareResponse(bs1)
	}
	if kind == MSG_TXN_VALIDATE {
		return DecodeTxnValidateResponse(bs1)
	}
	if kind == MSG_TXN_PREPARE {
		return DecodeTxnPrepareResponse(bs1)
	}
	if kind == MSG_TXN_UNPREPARE {
		return DecodeTxnUnprepareResponse(bs1)
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
