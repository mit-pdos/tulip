package message

import (
	"github.com/mit-pdos/tulip/tulip"
)

// TODO: move messages to their respective files and rename.

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
