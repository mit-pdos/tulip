package tulip

import (
	"github.com/mit-pdos/gokv/grove_ffi"
)

type Value struct {
	Present bool
	Content string
}

type WriteEntry struct {
	Key   string
	Value Value
}

type Version struct {
	Timestamp uint64
	Value     Value
}

type PrepareProposal struct {
	// Rank of the prepare proposal.
	Rank     uint64
	// Prepared or unprepared.
	Prepared bool
}

type CoordID struct {
	GroupID   uint64
	ReplicaID uint64
}

type KVMap map[string]Value

// Mapping from replica IDs to replica addresses.
type AddressMap map[uint64]grove_ffi.Address

// Mapping from group IDs to the address map of a group.
type AddressMaps map[uint64]AddressMap

const (
	TXN_PREPARED  uint64 = 0
	TXN_COMMITTED uint64 = 1
	TXN_ABORTED   uint64 = 2
)

// Replica results.
const (
	REPLICA_OK                uint64 = 0
	REPLICA_COMMITTED_TXN     uint64 = 1
	REPLICA_ABORTED_TXN       uint64 = 2
	REPLICA_STALE_COORDINATOR uint64 = 3
	REPLICA_FAILED_VALIDATION uint64 = 4
	// TODO: remove unused INVALID_RANK
	REPLICA_INVALID_RANK      uint64 = 5
	REPLICA_WRONG_LEADER      uint64 = 6
)
