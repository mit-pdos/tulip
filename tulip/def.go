package tulip

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

type KVMap map[string]Value

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
	REPLICA_INVALID_RANK      uint64 = 5
	REPLICA_WRONG_LEADER      uint64 = 6
)
