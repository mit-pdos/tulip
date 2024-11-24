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

func EncodeTxnReadRequest(ts uint64, key string) []byte {
	return nil
}

func DecodeTxnReadRequest(data []byte) TxnRequest {
	return TxnRequest{}
}

func EncodeTxnReadResponse(ts, rid uint64, key string, lts uint64, value tulip.Value) []byte {
	return nil
}

func DecodeTxnReadResponse(data []byte) TxnResponse {
	return TxnResponse{}
}

func EncodeTxnFastPrepareRequest(ts uint64, pwrs tulip.KVMap, ptgs []uint64) []byte {
	return nil
}

func DecodeTxnFastPrepareRequest(data []byte) TxnRequest {
	return TxnRequest{}
}

func EncodeTxnFastPrepareResponse(ts, rid, res uint64) []byte {
	return nil
}

func DecodeTxnFastPrepareResponse(data []byte) TxnResponse {
	return TxnResponse{}
}

func EncodeTxnValidateRequest(ts, rank uint64, pwrs tulip.KVMap, ptgs []uint64) []byte {
	return nil
}

func DecodeTxnValidateRequest(data []byte) TxnRequest {
	return TxnRequest{}
}

func EncodeTxnValidateResponse(ts, rid, res uint64) []byte {
	return nil
}

func DecodeTxnValidateResponse(data []byte) TxnResponse {
	return TxnResponse{}
}

func EncodeTxnPrepareRequest(ts, rank uint64) []byte {
	return nil
}

func DecodeTxnPrepareRequest(data []byte) TxnRequest {
	return TxnRequest{}
}

func EncodeTxnPrepareResponse(ts, rank, rid, res uint64) []byte {
	return nil
}

func DecodeTxnPrepareResponse(data []byte) TxnResponse {
	return TxnResponse{}
}

func EncodeTxnUnprepareRequest(ts, rank uint64) []byte {
	return nil
}

func DecodeTxnUnprepareRequest(data []byte) TxnRequest {
	return TxnRequest{}
}

func EncodeTxnUnprepareResponse(ts, rank, rid, res uint64) []byte {
	return nil
}

func DecodeTxnUnprepareResponse(data []byte) TxnResponse {
	return TxnResponse{}
}

func EncodeTxnQueryRequest(ts, rank uint64) []byte {
	return nil
}

func DecodeTxnQueryRequest(data []byte) TxnRequest {
	return TxnRequest{}
}

func EncodeTxnQueryResponse(ts, res uint64) []byte {
	return nil
}

func DecodeTxnQueryResponse(data []byte) TxnResponse {
	return TxnResponse{}
}

func EncodeTxnRefreshRequest(ts, rank uint64) []byte {
	return nil
}

func DecodeTxnRefreshRequest(data []byte) TxnRequest {
	return TxnRequest{}
}

func EncodeTxnCommitRequest(ts uint64, pwrs tulip.KVMap) []byte {
	return nil
}

func DecodeTxnCommitRequest(data []byte) TxnRequest {
	return TxnRequest{}
}

func EncodeTxnCommitResponse(ts, res uint64) []byte {
	return nil
}

func DecodeTxnCommitResponse(data []byte) TxnResponse {
	return TxnResponse{}
}

func EncodeTxnAbortRequest(ts uint64) []byte {
	return nil
}

func DecodeTxnAbortRequest(data []byte) TxnRequest {
	return TxnRequest{}
}

func EncodeTxnAbortResponse(ts, res uint64) []byte {
	return nil
}

func DecodeTxnAbortResponse(data []byte) TxnResponse {
	return TxnResponse{}
}

func DecodeTxnRequest(data []byte) TxnRequest {
	return TxnRequest{}
}

func DecodeTxnResponse(data []byte) TxnResponse {
	return TxnResponse{}
}
