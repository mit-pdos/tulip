package message

import (
	"github.com/mit-pdos/tulip/tulip"
)

type RequestMessage struct {
	Kind              uint64
	Timestamp         uint64
	Rank              uint64
	PartialWrites     tulip.KVMap
	ParticipantGroups []uint64
}

type ResponseMessage struct {
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
	MSG_READ         uint64 = 100
	MSG_FAST_PREPARE uint64 = 201
	MSG_VALIDATE     uint64 = 202
	MSG_PREPARE      uint64 = 203
	MSG_UNPREPARE    uint64 = 204
	MSG_QUERY        uint64 = 205
	MSG_INQUIRE      uint64 = 206
	MSG_REFRESH      uint64 = 210
	MSG_COMMIT       uint64 = 300
	MSG_ABORT        uint64 = 301
)

// TODO: Implement these.

func EncodeRead(ts uint64, key string) []byte {
	return nil
}

func EncodeFastPrepare(ts uint64, pwrs tulip.KVMap, ptgs []uint64) []byte {
	return nil
}

func DecodeRequest(data []byte) RequestMessage {
	return RequestMessage{}
}

func DecodeResponse(data []byte) ResponseMessage {
	return ResponseMessage{}
}
