package params

const N_SHARDS uint64 = 2

// Timeout for re-establishing connection: 10ms.
const NS_RECONNECT uint64 = 10_000_000

// Timeout for re-resending a PREPARE-family message: 20ms.
const NS_RESEND_PREPARE uint64 = 20_000_000

// Timeout for re-resending a READ message: 20ms.
const NS_RESEND_READ uint64 = 20_000_000

// Timeout for re-resending a COMMIT message: 40ms.
const NS_RESEND_COMMIT uint64 = 40_000_000

// Timeout for re-resending an ABORT message: 40ms.
const NS_RESEND_ABORT uint64 = 40_000_000

// Timeout for spawning off a backup coordinator: 5-6s (randomized).
const NS_SPAWN_BACKUP_BASE  uint64 = 5_000_000_000
const NS_SPAWN_BACKUP_DELTA uint64 = 1_000_000_000

// Time interval for sending REFRESH message: 4s.
const NS_SEND_REFRESH uint64 = 4_000_000_000
