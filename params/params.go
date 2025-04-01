package params

const N_SHARDS uint64 = 2

// Timeout for re-establishing connection: 100ms.
const NS_RECONNECT uint64 = 100_000_000

// Timeout for re-resending a PREPARE-family message: 800ms.
const NS_RESEND_PREPARE uint64 = 800_000_000

// Timeout for re-resending a READ message: 400ms.
const NS_RESEND_READ uint64 = 400_000_000

// Timeout for re-resending a COMMIT message: 400ms.
const NS_RESEND_COMMIT uint64 = 400_000_000

// Timeout for re-resending an ABORT message: 400ms.
const NS_RESEND_ABORT uint64 = 400_000_000

// Timeout for spawning off a backup coordinator: 5-6s (randomized).
const NS_SPAWN_BACKUP_BASE  uint64 = 5_000_000_000
const NS_SPAWN_BACKUP_DELTA uint64 = 1_000_000_000

// Time interval for sending REFRESH message: 4s.
const NS_SEND_REFRESH uint64 = 4_000_000_000

// Time interval for sending the batched Paxos commands: 300 ms.
const NS_BATCH_INTERVAL uint64 = 300_000_000

// Time interval for sending the batched Paxos commands: 1 s.
const NS_HEARTBEAT_INTERVAL uint64 = 1_000_000_000

// Paxos election timeout: 5-8s.
const NS_ELECTION_TIMEOUT_BASE  uint64 = 5_000_000_000
const NS_ELECTION_TIMEOUT_DELTA uint64 = 3_000_000_000

// Number of retries for checking Paxos command replication.
const N_RETRY_REPLICATED uint64 = 500

// Time interval for checking Paxos command replication: 10us.
// Best value would be a single round trip to some quorum of nodes.
const NS_REPLICATED_INTERVAL uint64 = 10_000

// Number of transaction sites.
const N_TXN_SITES uint64 = 1024
