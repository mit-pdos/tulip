# Tulip

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
