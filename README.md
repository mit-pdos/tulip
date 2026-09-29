# Tulip

Tulip is a distributed transaction system with [mechanized proofs of
correctness](https://github.com/mit-pdos/perennial/tree/master/src/program_proof/tulip).

Tulip exposes a strong (i.e., strict serializability) and simple transactional key-value store
interface to users. It supports the following features:
1. Multi-version concurrency control (MVCC)
2. Cross-partition consistency via two-phase commit (2PC)
3. Fault tolerance with Paxos-based replication
4. Single network-roundtrip 2PC latency with inconsistent replication (IR)
5. Transaction coordinator recovery

Tulip's proofs are formalized with the [Perennial framework](https://github.com/mit-pdos/perennial),
which is built on the [Iris separation logic framework](https://iris-project.org/) and mechanized in
the [Rocq theorem prover](https://rocq-prover.org/).

## Running locally

### Prerequisites

Use Go version >= 1.22. Run the following commands from the repository root.

Create the log directory:

```sh
mkdir -p durable
```

### Tulip

For a local Tulip cluster and interactive client:

```sh
go run ./main/tulip-local
```

Use `write <key> <value>`, `read <key>`, or `delete <key>` at the prompt.

### Multi-Paxos

For a local three-node Multi-Paxos cluster:

```sh
go run ./main/paxos-local
```

Use `submit <value>` and `lookup <idx>` at the prompt. If submission fails while finding the leader,
retry; use the log index printed by a successful submission for lookup.

Both demos store write-ahead logs in `durable/`. Press Ctrl-C to stop.

## File structure

Low-level packages:
- `params`: just constants
- `util`: low-level utilities (especially encoding/decoding)
- `tulip`: basic definitions for interface (TODO: move internal defs out)
- `tuple`: a single tuple of the database, with MVCC history
- `quorum`: pure integer quorum computations
- `message`: structs for txn requests/responses (and serialization)
- `index`: key to tuple indexing data structure (safe for concurrent access)

Intermediate packages:
- `paxos`: struct implementing the MultiPaxos algorithm
- `txnlog`: wraps paxos with encoding/decoding of txn commands
- `backup`: backup transaction and group coordinator
- `gcoord`: group coordinator for replicas involved in a transaction

Top-level packages:
- `replica`: top-level struct for one replica of the database
- `txn`: library for clients to submit transactions
