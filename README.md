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

## Benchmarking

### Generating configuration files

Pass the number of groups followed by one `IP replica-port paxos-port` triple per replica. For one
group with three local replicas:

```sh
go run ./main/gen-conf 1 \
  127.0.0.1 49800 49900 \
  127.0.0.1 49801 49901 \
  127.0.0.1 49802 49902 > main/gen-conf/conf-localhost.json
```

Add or remove triples to change the replica count; replica IDs start at `0` in argument order.
Ports are for group `0`; each additional group adds `10` to both ports. Choose ports so all
endpoints on each host are distinct. The generator writes JSON to stdout.

### Prerequisites

Create the server log directory from the repository root:

```sh
mkdir -p main/tulip-node/durable
```

### Servers

Start the servers in three separate terminals, running the following from the repository root with
replica IDs `0`, `1`, and `2`:

```sh
cd main/tulip-node
bash run.sh 0
```

### Clients

In another terminal, start from the repository root, populate the database, and run a benchmark:

```sh
cd main/tulip-ycsb
bash populate.sh
NTHRDS=4 DURATION=10 bash run.sh
```

These scripts build the binaries and use `main/gen-conf/conf-localhost.json` by default. Override
`CONF` consistently for servers and clients to use another configuration. `NTHRDS` sets the client
thread count and `DURATION` sets the run time in seconds.

To run the benchmark suite, run the following from `main/tulip-ycsb` after population. This requires
`stdbuf`, uses `conf.json`, and writes CSV results under `exp/`; the argument sets the repetition
count:

```sh
cp ../gen-conf/conf-localhost.json conf.json
bash ycsb.sh 1
```

## File structure

Low-level packages:
- `params`: just constants
- `util`: low-level utilities (especially encoding/decoding)
- `tulip`: basic definitions for interface
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
