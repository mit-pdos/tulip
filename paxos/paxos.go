package paxos

import (
	"fmt"
	"sync"

	"github.com/goose-lang/primitive"
	"github.com/mit-pdos/gokv/grove_ffi"
	"github.com/mit-pdos/tulip/params"
	"github.com/mit-pdos/tulip/quorum"
	"github.com/mit-pdos/tulip/util"
	"github.com/tchajed/marshal"
)

// TODOs:
// 1. crash recovery

type Paxos struct {
	// ID of this node.
	nidme     uint64
	// Node ID of its peers.
	peers     []uint64
	// Addresses of other Paxos nodes.
	addrm     map[uint64]grove_ffi.Address
	// Size of the cluster. @sc = @len(peers) + 1.
	sc        uint64
	// Name of the write-ahead log file.
	fname     string
	// Mutex protecting fields below.
	mu        *sync.Mutex
	// Condition variable used to triggered sending entries.
	cv        *sync.Cond
	// Heartbeat.
	hb        bool
	// Term in which this Paxos node currently is. Persistent.
	termc     uint64
	// Term to which the log entries @ents belong. Persistent.
	terml     uint64
	// List of log entries. Persistent.
	log       []string
	// LSN before which entries are committed (exclusively). Persistent. Note
	// that persistence of @lsnc is *not* a safety requirement, but a
	// performance improvement (so that the leader's corresponding @lsnpeers
	// entry can be updated more efficiently when this node crashes and
	// recovers, rather than always start from 0).
	lsnc      uint64
	// Whether this node is the candidate in @termc.
	iscand    bool
	// Whether this node is the leader in @termc.
	isleader  bool
	//
	// Candidate state below.
	//
	// Largest term seen in the prepare phase.
	termp     uint64
	// Longest entries after @lsnp in @termc in the prepare phase.
	entsp     []string
	// Termed log entries collected from peers in the prepare phase.
	// NB: Unit range would suffice.
	respp     map[uint64]bool
	//
	// Leader state below.
	//
	// For each follower, LSN up to which all entries match @ents (i.e.,
	// consistent in @terml). @lsnpeers are also used to decide which entries
	// should be sent to the follower. It is initialized to be an empty map when
	// a leader is first elected. Absence of an entry means that the node has
	// not reported what is on its log, in which case the leader could simply
	// send an APPEND-ENTRIES with LSN = @len(px.log). Note that once
	// @lsnpeers[nid] is set, it should only increase monotonically, as
	// followers' log are supposed to only grow within a term. This subsumes the
	// next / match indexes in Raft.
	lsnpeers  map[uint64]uint64
	//
	// Connections to peers. Used only when the node is a leader or a candidate.
	//
	conns     map[uint64]grove_ffi.Connection
}

const MAX_NODES uint64 = 16

// External global logical states:
// 1. Log
// 2. Command pool
//
// Internal global logical states:
// 1. Base proposals
// 2. Growing proposals
//
// Node-local logical states:
// 1. Current term
// 2. Log term
// 3. Log
// 4. Committed LSN
// 5. Accepted proposals
// 6. Past node decisions (for encoding invariability of accepted proposals)

// Logical actions (all parametrized by node ID [nid]):
//
// Accept(term, log)
// For node [nid]:
// 1. Extend the log to [log].
// 2. Extend the accepted proposals at term [term] to [log].
//
// Advance(term, log)
// For node [nid]:
// 1. Bump the log term to [term].
// 2. Update the log to [log].
// 3. Update the accepted proposals at term [term] to [log].
//
// Ascend(term, log)
// Globally:
// 1. Insert proposal [(term, log)] to the base proposals and the growing proposals.
// For node [nid]:
// 1. Bump the log term to [term].
// 2. Update the log to [log].
// 3. Update the accepted proposals at term [term] to [log].
//
// Commit(log)
// Globally:
// 1. Extend the global log to [log] if the latter is longer than the former.
//
// Extend(term, log)
// Globally:
// 1. Extend the growing proposals at [term] to [log].
//
// Prepare(term)
// For node [nid]:
// 1. Bump the current term to [term].
// 2. Let [termc] be the current term. Snoc the accepted proposals at [termc],
// if one exists, to the past node decisions, and extend which with [Reject]
// until [term].
//
// Expand(lsnc):
// For node [nid]:
// 1. Bump the committed LSN to [lsnc]

func (px *Paxos) Submit(v string) (uint64, uint64) {
	px.mu.Lock()

	if !px.leading() {
		px.mu.Unlock()
		return 0, 0
	}

	lsn := uint64(len(px.log))
	px.log = append(px.log, v)
	term := px.termc

	// Logical action: Extend(@px.termc, @px.log).
	logAppend(px.fname, v)

	// Potential batch optimization: Even though we update @px.log here, but it
	// should be OK to not immediately write them to disk, but wait until those
	// entries are sent in @LeaderSession. To prove the optimization, we'll need
	// to decouple the "batched entries" from the actual entries @px.log, and
	// relate only @px.log to the invariant.

	px.mu.Unlock()

	px.cv.Broadcast()

	return lsn, term
}

func (px *Paxos) Lookup(lsn uint64) (string, bool) {
	px.mu.Lock()

	if px.lsnc <= lsn {
		px.mu.Unlock()
		return "", false
	}

	v := px.log[lsn]

	// Logical action: Commit(@px.log) if @px.log is longer than the global log.

	px.mu.Unlock()
	return v, true
}

func (px *Paxos) WaitUntilSafe(lsn uint64, term uint64) bool {
	var safe bool
	var nretry uint64

	for {
		px.mu.Lock()
		termc := px.gettermc()
		lsnc := px.getlsnc()
		px.mu.Unlock()

		if term != termc {
			// Term has changed after submission of the command, so this command
			// is unlikely to be replicated.
			break
		}

		if lsn < lsnc {
			// The term in which the command and the current term matches, and
			// the committed LSN has passed the LSN of the command being waited,
			// hence this command is guaranteed to be safe.
			safe = true
			break
		}

		if nretry == params.N_RETRY_REPLICATED {
			break
		}
		nretry++

		primitive.Sleep(params.NS_REPLICATED_INTERVAL)
	}

	return safe
}

//
// Paxos state-machine actions.
//

// Return values:
// 1. @term: New term in which this node attempts to be the leader.
// 2. @lsn: LSN after which log entries whose committedness is yet known, and
// hence the content need to be resolved through the leader-election phase.
func (px *Paxos) nominate() (uint64, uint64) {
	// Compute the new term and proceed to that term.
	term := util.NextAligned(px.termc, MAX_NODES, px.nidme)
	px.termc = term
	px.isleader = false

	// Obtain entries after @px.lsnc.
	lsn := px.lsnc
	ents := make([]string, uint64(len(px.log)) - lsn)
	copy(ents, px.log[lsn :])

	// Use the candidate's log term (@px.terml) and entries (after the committed
	// LSN, @ents) as the initial preparing term and entries.
	px.iscand = true
	px.termp  = px.terml
	px.entsp  = ents
	px.respp  = make(map[uint64]bool)
	px.respp[px.nidme] = true

	fmt.Printf("[paxos %d] Become a candidate in %d.\n", px.nidme, px.termc)

	// Logical action: Prepare(@term).
	logPrepare(px.fname, term)

	return term, lsn
}

func (px *Paxos) stepdown(term uint64) {
	px.iscand = false
	px.isleader = false

	if px.termc == term {
		return
	}

	// Logical action: Prepare(@term).
	px.termc = term
	logPrepare(px.fname, term)
}

// Argument:
// 1. @lsn: LSN after which log entries whose committedness is yet known, and
// hence the content need to be resolved through the leader-election phase.
//
// Return values:
// 1. @terml: Log term of this node (which is also the largest accepted term
// before @px.termc).
// 2. @ents: All entries after @lsn.
func (px *Paxos) prepare(lsn uint64) (uint64, []string) {
	if uint64(len(px.log)) <= lsn {
		return px.terml, make([]string, 0)
	}

	ents := make([]string, uint64(len(px.log)) - lsn)
	copy(ents, px.log[lsn :])
	return px.terml, ents
}

func (px *Paxos) collect(nid uint64, term uint64, ents []string) {
	_, recved := px.respp[nid]
	if recved {
		// Vote from [nid] has already been received.
		return
	}

	if term < px.termp {
		// Simply record the response if the peer has a smaller term.
		px.respp[nid] = true
		return
	}

	if term == px.termp && uint64(len(ents)) <= uint64(len(px.entsp)) {
		// Simply record the response if the peer has the same term, but not
		// more entries (i.e., longer prefix).
		px.respp[nid] = true
		return
	}

	// Update the largest term and longest log seen so far in this preparing
	// phase, and record the response.
	px.termp = term
	px.entsp = ents
	px.respp[nid] = true
}

func (px *Paxos) ascend() {
	// Nothing should be done before obtaining a classic quorum of responses.
	if !px.cquorum(uint64(len(px.respp))) {
		return
	}

	// Add the longest prefix in the largest term among some quorum (i.e.,
	// @px.entsp) to our log starting from @px.lsnc.
	px.log = append(px.log[: px.lsnc], px.entsp...)

	// Update @px.terml to @px.termc here.
	px.terml = px.termc

	// Transit from the candidate to the leader.
	px.iscand = false
	px.isleader = true
	px.lsnpeers = make(map[uint64]uint64)

	fmt.Printf("[paxos %d] Become a leader in %d.\n", px.nidme, px.termc)

	// Logical action: Ascend(@px.termc, @px.log).
	logAdvance(px.fname, px.termc, px.lsnc, px.entsp)
}

func (px *Paxos) obtain(nid uint64) (uint64, []string) {
	lsne, ok := px.lsnpeers[nid]

	if !ok {
		// The follower has not reported the matched LSN, so send an
		// empty APPEND-ENTRIES request.
		return uint64(len(px.log)), make([]string, 0)
	}

	// The follower has reported up to where the log is matched
	// (i.e., @lsne), so send everything after that.
	ents := make([]string, uint64(len(px.log)) - lsne)
	copy(ents, px.log[lsne :])
	return lsne, ents
}

// Arguments:
// 1. @lsn: LSN at which @ents start.
// 2. @term: Term to which @ents belong.
// 3. @ents: Log entries.
//
// Return values:
// 1. @lsna: LSN up to which log consistency at term @term is established.
func (px *Paxos) accept(lsn uint64, term uint64, ents []string) uint64 {
	if term != px.terml {
		// Our log term does not match the term @term of @ents. Return an error
		// if @px.lsnc < @lsn, as log consistency at @term cannot be guaranteed.
		if px.lsnc != lsn {
			return px.lsnc
		}

		// Append @ents to our own log starting at @lsn.
		px.log = px.log[: lsn]
		px.log = append(px.log, ents...)

		// Update the log term to @term.
		px.terml = term

		// Return LSN at the end of our log after accepting @ents.
		lsna := uint64(len(px.log))

		// fmt.Printf("[paxos %d] Accept entries in %d up to %d: %v\n", px.nidme, px.terml, lsna, px.log)

		// Logical action: Advance(term, log).
		logAdvance(px.fname, term, lsn, ents)

		return lsna
	}

	// We're in the same term. Now we should skip appending @ents iff there's
	// gap between @ents and @px.log OR appending @ents starting at @lsn
	// actually shortens the log.
	nents := uint64(len(px.log))
	if nents < lsn || lsn + uint64(len(ents)) <= nents {
		return nents
	}

	// Append @ents to our own log starting at @lsn.
	px.log = px.log[: lsn]
	px.log = append(px.log, ents...)

	lsna := uint64(len(px.log))

	// fmt.Printf("[paxos %d] Accept entries in %d up to %d: %v\n", px.nidme, px.terml, lsna, px.log)

	// Logical action: Accept(term, log)
	logAccept(px.fname, lsn, ents)

	return lsna
}

// @learn monotonically increase the commit LSN @px.lsnc in term @term to @lsn.
func (px *Paxos) learn(lsn uint64, term uint64) {
	// Skip if the log term @px.terml does not match @lsn.
	if term != px.terml {
		return
	}

	px.commit(lsn)
}

func (px *Paxos) forward(nid uint64, lsn uint64) bool {
	lsnpeer, ok := px.lsnpeers[nid]
	if !ok || lsnpeer < lsn {
		// Advance the peer's matching LSN.
		px.lsnpeers[nid] = lsn

		// fmt.Printf("[paxos %d] Advance peer %d matching LSN to %d\n", px.nidme, nid, lsn)

		return true
	}

	return false
}

func (px *Paxos) push() (uint64, bool) {
	if !px.cquorum(uint64(len(px.lsnpeers)) + 1) {
		// Nothing should be done without responses from some quorum of nodes.
		return 0, false
	}

	var lsns = make([]uint64, 0, px.sc)

	for _, lsn := range(px.lsnpeers) {
		lsns = append(lsns, lsn)
	}

	// Note that without adding @nidme to @lsns poses an additional requirement
	// that the cluster should have more than one node, which is totally
	// reasonable. If we ever want to allow running a "single-node" cluster,
	// adding the line below back.
	//
	// lsns = append(lsns, uint64(len(px.log)))

	util.Sort(lsns)

	lsn := lsns[uint64(len(lsns)) - (px.sc / 2)]

	if lsn < px.lsnc {
		return 0, false
	}

	return lsn, true
}

func (px *Paxos) commit(lsn uint64) {
	if lsn <= px.lsnc {
		return
	}

	if uint64(len(px.log)) < lsn {
		px.lsnc = uint64(len(px.log))

		// fmt.Printf("[paxos %d] Commit entries up to %d\n", px.nidme, px.lsnc)

		// Logical action: Expand(len(px.log))
		logExpand(px.fname, px.lsnc)

		return
	}

	px.lsnc = lsn

	// fmt.Printf("[paxos %d] Commit entries up to %d\n", px.nidme, px.lsnc)

	// Logical action: Expand(lsn)
	logExpand(px.fname, lsn)
}

func (px *Paxos) gttermc(term uint64) bool {
	return px.termc < term
}

func (px *Paxos) lttermc(term uint64) bool {
	return term < px.termc
}

func (px *Paxos) latest() bool {
	return px.termc == px.terml
}

func (px *Paxos) gettermc() uint64 {
	return px.termc
}

func (px *Paxos) getlsnc() uint64 {
	return px.lsnc
}

func (px *Paxos) nominated() bool {
	return px.iscand
}

func (px *Paxos) leading() bool {
	return px.isleader
}

func (px *Paxos) resethb() {
	px.hb = false
}

func (px *Paxos) heartbeat() {
	px.hb = true
}

func (px *Paxos) heartbeated() bool {
	return px.hb
}

func (px *Paxos) LeaderSession() {
	for {
		px.mu.Lock()
		px.cv.Wait()

		if !px.leading() {
			px.mu.Unlock()
			continue
		}

		// TODO: Write @px.log to disk before sending out APPEND-ENTRIES.

		for _, nidloop := range(px.peers) {
			nid := nidloop

			lsne, ents := px.obtain(nid)

			termc := px.gettermc()
			lsnc  := px.getlsnc()

			go func() {
				data := EncodeAcceptRequest(termc, lsnc, lsne, ents)
				px.Send(nid, data)
			}()
		}

		px.mu.Unlock()
	}
}

func (px *Paxos) HeartbeatSession() {
	for {
		primitive.Sleep(params.NS_HEARTBEAT_INTERVAL)
		px.cv.Broadcast()
	}
}

func (px *Paxos) ElectionSession() {
	for {
		delta := primitive.RandomUint64() % params.NS_ELECTION_TIMEOUT_DELTA
		primitive.Sleep(params.NS_ELECTION_TIMEOUT_BASE + delta)

		px.mu.Lock()

		if px.leading() {
			px.mu.Unlock()
			continue
		}

		if px.heartbeated() {
			px.resethb()
			px.mu.Unlock()
			continue
		}

		var termc uint64
		var lsnc uint64
		if px.nominated() {
			termc = px.gettermc()
			lsnc = px.getlsnc()
		} else {
			termc, lsnc = px.nominate()
		}

		px.mu.Unlock()

		for _, nidloop := range(px.peers) {
			nid := nidloop
			go func() {
				data := EncodePrepareRequest(termc, lsnc)
				px.Send(nid, data)
			}()
		}
	}
}

func (px *Paxos) ResponseSession(nid uint64) {
	for {
		data, ok := px.Receive(nid)
		if !ok {
			// Try to re-establish a connection on failure.
			primitive.Sleep(params.NS_RECONNECT)
			continue
		}

		resp := DecodeResponse(data)
		kind := resp.Kind

		px.mu.Lock()

		if px.lttermc(resp.Term) {
			// Skip the outdated message.
			px.mu.Unlock()
			continue
		}

		// In the current design, the response would never contain a term higher
		// than that in a request, and that means this check is actually not
		// used. However, it is kept for two reasons: First, if adding an
		// UPDATE-TERM becomes necessary (for performance or liveness reason),
		// then this check would then be useful. Second, in the proof, with this
		// check and the one above we obtain @px.termc = @resp.Term, which is
		// very useful. If we ever want to eliminate this check in the future,
		// we will have to find a way to encode ``responses terms never go higher
		// than request terms'' in the proof.
		if px.gttermc(resp.Term) {
			// Proceed to a new term on receiving a higher-term message.
			px.stepdown(resp.Term)
			px.mu.Unlock()
			continue
		}

		if kind == MSG_PREPARE {
			if !px.nominated() {
				px.mu.Unlock()
				continue
			}
			// Ideally, we should not need to include the node ID in the
			// response, since the entire session is used exclusively by @nid
			// (i.e., in reality @resp.NodeID should always be the same as
			// @nid). In the proof, we could maintain a persistent mapping from
			// channels to node IDs. However, it seems like the current network
			// semantics does not guarantee *freshness* of creating a channel
			// through @Connect, and hence such invariant cannot be established.
			px.collect(resp.NodeID, resp.EntriesTerm, resp.Entries)
			px.ascend()
			px.mu.Unlock()
		} else if kind == MSG_ACCEPT {
			if !px.leading() {
				px.mu.Unlock()
				continue
			}
			// Same as the reason above, the check below is performed merely for
			// the sake of proof.
			if resp.NodeID == px.nidme {
				px.mu.Unlock()
				continue
			}
			forwarded := px.forward(resp.NodeID, resp.MatchedLSN)
			if !forwarded {
				px.mu.Unlock()
				continue
			}
			lsnc, pushed := px.push()
			if !pushed {
				px.mu.Unlock()
				continue
			}
			px.commit(lsnc)
			px.mu.Unlock()
		}
	}
}

func (px *Paxos) RequestSession(conn grove_ffi.Connection) {
	for {
		ret := grove_ffi.Receive(conn)
		if ret.Err {
			break
		}

		req  := DecodeRequest(ret.Data)
		kind := req.Kind

		px.mu.Lock()

		if px.lttermc(req.Term) {
			// Skip the oudated message.
			px.mu.Unlock()

			// We can additionally send an UPDATE-TERM message, but not sure if
			// that's necessary, since eventually the new leader would reach out
			// to every node.
			// fmt.Printf("[replica %d] Outdated request: (mine, req) = (%d, %d)", px.nidme, px.termc, req.Term)
			continue
		}

		// Potentially proceed to a new term on receiving a higher-term message.
		px.stepdown(req.Term)

		px.heartbeat()

		termc := px.gettermc()

		if kind == MSG_PREPARE {
			if px.latest() {
				// The log has already matched up the current term, meaning the
				// leader has already successfully been elected. Simply ignore
				// this request.
				px.mu.Unlock()
				continue
			}
			terml, ents := px.prepare(req.CommittedLSN)
			px.mu.Unlock()
			data := EncodePrepareResponse(px.nidme, termc, terml, ents)
			// Request [REQUEST-VOTE, @termc, @lsnc] and
			// Response [REQUEST-VOTE, @termc, @terml, @ents] means:
			// (1) This node will not accept any proposal with term below @termc.
			// (2) The largest-term entries after LSN @lsnc this node has
			// accepted before @termc is (@terml, @ents).
			grove_ffi.Send(conn, data)
		} else if kind == MSG_ACCEPT {
			lsn := px.accept(req.EntriesLSN, req.Term, req.Entries)
			px.learn(req.CommittedLSN, req.Term)
			px.mu.Unlock()
			data := EncodeAcceptResponse(px.nidme, termc, lsn)
			grove_ffi.Send(conn, data)
		}
	}
}

func (px *Paxos) Serve() {
	addrme := px.addrm[px.nidme]
	ls := grove_ffi.Listen(addrme)
	for {
		conn := grove_ffi.Accept(ls)
		go func() {
			px.RequestSession(conn)
		}()
	}
}

func (px *Paxos) GetConnection(nid uint64) (grove_ffi.Connection, bool) {
	px.mu.Lock()
	conn, ok := px.conns[nid]
	px.mu.Unlock()
	return conn, ok
}

func (px *Paxos) Connect(nid uint64) bool {
	addr := px.addrm[nid]
	ret := grove_ffi.Connect(addr)
	if !ret.Err {
		// fmt.Printf("[paxos %d] Connect to peer %d.\n", px.nidme, nid)
		px.mu.Lock()
		px.conns[nid] = ret.Connection
		px.mu.Unlock()
		return true
	}
	return false
}

func (px *Paxos) ConnectAll() {
	for _, nid := range(px.peers) {
		px.Connect(nid)
	}
}

func (px *Paxos) Send(nid uint64, data []byte) {
	conn, ok := px.GetConnection(nid)
	if !ok {
		px.Connect(nid)
		return
	}

	err := grove_ffi.Send(conn, data)
	if err {
		px.Connect(nid)
	}
}

func (px *Paxos) Receive(nid uint64) ([]byte, bool) {
	conn, ok := px.GetConnection(nid)
	if !ok {
		px.Connect(nid)
		return nil, false
	}

	ret := grove_ffi.Receive(conn)
	if ret.Err {
		px.Connect(nid)
		return nil, false
	}

	return ret.Data, true
}

func (px *Paxos) cquorum(n uint64) bool {
	return quorum.ClassicQuorum(px.sc) <= n
}

func (px *Paxos) DumpState() {
	px.mu.Lock()
	fmt.Printf("Current term: %d\n", px.termc)
	fmt.Printf("Ledger term: %d\n", px.terml)
	fmt.Printf("Number of log entries: %d\n", uint64(len(px.log)))
	fmt.Printf("Committed LSN: %d\n", px.lsnc)
	fmt.Printf("Is candidate / leader: %t / %t\n", px.iscand, px.isleader)
	if px.iscand {
		fmt.Printf("Candidate state:\n")
		fmt.Printf("Largest term seen in prepare: %d\n", px.termp)
		fmt.Printf("Longest log after committed LSN in prepare: %d\n", px.termp)
		fmt.Printf("Number of votes granted: %d\n", uint64(len(px.respp)))
	}
	if px.isleader {
		fmt.Printf("Leader state:\n")
		fmt.Printf("Match LSN for each peer:\n")
		for nid, lsnpeer := range(px.lsnpeers) {
			fmt.Printf("Peer %d -> %d\n", nid, lsnpeer)
		}
	}
	px.mu.Unlock()
}

func (px *Paxos) ForceElection() {
	px.mu.Lock()
	termc, lsnc := px.nominate()
	px.mu.Unlock()

	for _, nidloop := range(px.peers) {
		nid := nidloop
		go func() {
			data := EncodePrepareRequest(termc, lsnc)
			px.Send(nid, data)
		}()
	}
}

func mkPaxos(nidme, termc, terml, lsnc uint64, log []string, addrm map[uint64]grove_ffi.Address, fname string) *Paxos {
	sc := uint64(len(addrm))

	var peers = make([]uint64, 0, sc - 1)
	for nid := range(addrm) {
		if nid != nidme {
			peers = append(peers, nid)
		}
	}

	mu := new(sync.Mutex)
	cv := sync.NewCond(mu)
	px := &Paxos{
		nidme    : nidme,
		peers    : peers,
		addrm    : addrm,
		sc       : sc,
		fname    : fname,
		mu       : mu,
		cv       : cv,
		hb       : false,
		termc    : termc,
		terml    : terml,
		log      : log,
		lsnc     : lsnc,
		iscand   : false,
		isleader : false,
		conns    : make(map[uint64]grove_ffi.Connection),
	}

	return px
}

func Start(nidme uint64, addrm map[uint64]grove_ffi.Address, fname string) *Paxos {
	// Check that the cluster has more than one node.
	primitive.Assume(1 < uint64(len(addrm)))

	// Check that @nidme is part of the cluster.
	_, ok := addrm[nidme]
	primitive.Assume(ok)

	// Check the @nidme is valid.
	primitive.Assume(nidme < MAX_NODES)

	// Recover durable states from the write-ahead log.
	termc, terml, lsnc, log := resume(fname)

	px := mkPaxos(nidme, termc, terml, lsnc, log, addrm, fname)

	go func() {
		px.Serve()
	}()

	go func() {
		px.LeaderSession()
	}()

	go func() {
		px.HeartbeatSession()
	}()

	go func() {
		px.ElectionSession()
	}()

	for _, nidloop := range(px.peers) {
		nid := nidloop

		go func() {
			px.ResponseSession(nid)
		}()
	}

	return px
}

///
/// Paxos messages.
///

// [REQUEST-VOTE, Term, CommittedLSN]
// [APPEND-ENTRIES, Term, CommittedLSN, LSNEntries, Entries]
type PaxosRequest struct {
	Kind         uint64
	Term         uint64
	CommittedLSN uint64
	EntriesLSN   uint64
	Entries      []string
}

// [REQUEST-VOTE, NodeID, Term, EntriesTerm, Entries]
// [APPEND-ENTRIES, NodeID, Term, MatchedLSN]
type PaxosResponse struct {
	Kind        uint64
	NodeID      uint64
	Term        uint64
	EntriesTerm uint64
	Entries     []string
	MatchedLSN  uint64
}

const (
	MSG_PREPARE uint64 = 0
	MSG_ACCEPT  uint64 = 1
)

func EncodePrepareRequest(term, lsnc uint64) []byte {
	bs := make([]byte, 0, 24)

	bs1  := marshal.WriteInt(bs, MSG_PREPARE)
	bs2  := marshal.WriteInt(bs1, term)
	data := marshal.WriteInt(bs2, lsnc)

	return data
}

func DecodePrepareRequest(bs []byte) PaxosRequest {
	term, bs1 := marshal.ReadInt(bs)
	lsnc, _ := marshal.ReadInt(bs1)

	return PaxosRequest{
		Kind : MSG_PREPARE,
		Term : term,
		CommittedLSN : lsnc,
	}
}

func EncodeAcceptRequest(term, lsnc, lsne uint64, ents []string) []byte {
	bs := make([]byte, 0, 64)

	bs1  := marshal.WriteInt(bs, MSG_ACCEPT)
	bs2  := marshal.WriteInt(bs1, term)
	bs3  := marshal.WriteInt(bs2, lsnc)
	bs4  := marshal.WriteInt(bs3, lsne)
	data := util.EncodeStrings(bs4, ents)

	return data
}

func DecodeAcceptRequest(bs []byte) PaxosRequest {
	term, bs1 := marshal.ReadInt(bs)
	lsnc, bs2 := marshal.ReadInt(bs1)
	lsne, bs3 := marshal.ReadInt(bs2)
	ents, _ := util.DecodeStrings(bs3)

	return PaxosRequest{
		Kind         : MSG_ACCEPT,
		Term         : term,
		CommittedLSN : lsnc,
		EntriesLSN   : lsne,
		Entries      : ents,
	}
}

func EncodePrepareResponse(nid, term, terma uint64, ents []string) []byte {
	bs := make([]byte, 0, 64)

	bs1  := marshal.WriteInt(bs, MSG_PREPARE)
	bs2  := marshal.WriteInt(bs1, nid)
	bs3  := marshal.WriteInt(bs2, term)
	bs4  := marshal.WriteInt(bs3, terma)
	data := util.EncodeStrings(bs4, ents)

	return data
}

func DecodePrepareResponse(bs []byte) PaxosResponse {
	nid, bs1   := marshal.ReadInt(bs)
	term, bs2  := marshal.ReadInt(bs1)
	terma, bs3 := marshal.ReadInt(bs2)
	ents, _    := util.DecodeStrings(bs3)

	return PaxosResponse{
		Kind        : MSG_PREPARE,
		NodeID      : nid,
		Term        : term,
		EntriesTerm : terma,
		Entries     : ents,
	}
}

func EncodeAcceptResponse(nid, term, lsn uint64) []byte {
	bs := make([]byte, 0, 32)

	bs1  := marshal.WriteInt(bs, MSG_ACCEPT)
	bs2  := marshal.WriteInt(bs1, nid)
	bs3  := marshal.WriteInt(bs2, term)
	data := marshal.WriteInt(bs3, lsn)

	return data
}

func DecodeAcceptResponse(bs []byte) PaxosResponse {
	nid, bs1  := marshal.ReadInt(bs)
	term, bs2 := marshal.ReadInt(bs1)
	lsn, _    := marshal.ReadInt(bs2)

	return PaxosResponse{
		Kind       : MSG_ACCEPT,
		NodeID     : nid,
		Term       : term,
		MatchedLSN : lsn,
	}
}

func DecodeRequest(bs []byte) PaxosRequest {
	kind, data := marshal.ReadInt(bs)

	if kind == MSG_PREPARE {
		req := DecodePrepareRequest(data)
		return req
	}
	if kind == MSG_ACCEPT {
		req := DecodeAcceptRequest(data)
		return req
	}

	return PaxosRequest{}
}

func DecodeResponse(bs []byte) PaxosResponse {
	kind, data := marshal.ReadInt(bs)

	if kind == MSG_PREPARE {
		resp := DecodePrepareResponse(data)
		return resp
	}
	if kind == MSG_ACCEPT {
		resp := DecodeAcceptResponse(data)
		return resp
	}

	return PaxosResponse{}
}

///
/// Crash recovery.
///

const (
	CMD_EXTEND  uint64 = 0
	CMD_APPEND  uint64 = 1
	CMD_PREPARE uint64 = 2
	CMD_ADVANCE uint64 = 3
	CMD_ACCEPT  uint64 = 4
	CMD_EXPAND  uint64 = 5
)

func logExtend(fname string, ents []string) {
	// Currently not used. For batch optimization.
	bs := make([]byte, 0, 64)

	bs1 := marshal.WriteInt(bs, CMD_EXTEND)
	bs2 := util.EncodeStrings(bs1, ents)

	grove_ffi.FileAppend(fname, bs2)
}

func logAppend(fname string, ent string) {
	bs := make([]byte, 0, 32)

	bs1 := marshal.WriteInt(bs, CMD_APPEND)
	bs2 := util.EncodeString(bs1, ent)

	grove_ffi.FileAppend(fname, bs2)
}

func logPrepare(fname string, term uint64) {
	bs := make([]byte, 0, 16)

	bs1 := marshal.WriteInt(bs, CMD_PREPARE)
	bs2 := marshal.WriteInt(bs1, term)

	grove_ffi.FileAppend(fname, bs2)
}

// Note that we could have defined @logAdvance to take @ents only, since @term
// and @lsn can be determined directly from the state for the
// candidate. However, a similar transition also happens on followers, but @term
// and @lsn are passed through function arguments rather than from state. Given
// that advance is used only on failure cases, defining a single interface
// simplify things a bit without hurting too much of the performance.
func logAdvance(fname string, term uint64, lsn uint64, ents []string) {
	bs := make([]byte, 0, 64)

	bs1 := marshal.WriteInt(bs, CMD_ADVANCE)
	bs2 := marshal.WriteInt(bs1, term)
	bs3 := marshal.WriteInt(bs2, lsn)
	bs4 := util.EncodeStrings(bs3, ents)

	grove_ffi.FileAppend(fname, bs4)
}

func logAccept(fname string, lsn uint64, ents []string) {
	bs := make([]byte, 0, 64)

	bs1 := marshal.WriteInt(bs, CMD_ACCEPT)
	bs2 := marshal.WriteInt(bs1, lsn)
	bs3 := util.EncodeStrings(bs2, ents)

	grove_ffi.FileAppend(fname, bs3)
}

func logExpand(fname string, lsn uint64) {
	bs := make([]byte, 0, 16)

	bs1 := marshal.WriteInt(bs, CMD_EXPAND)
	bs2 := marshal.WriteInt(bs1, lsn)

	grove_ffi.FileAppend(fname, bs2)
}

// Read the underlying file and perform recovery to re-construct @termc, @terml,
// @lsnc, and @log.
func resume(fname string) (uint64, uint64, uint64, []string) {
	var termc uint64
	var terml uint64
	var lsnc  uint64
	var log = make([]string, 0)

	var data = grove_ffi.FileRead(fname)

	for 0 < uint64(len(data)) {
		kind, bs := marshal.ReadInt(data)

		if kind == CMD_EXTEND {
			ents, bs1 := util.DecodeStrings(bs)
			data = bs1
			// Apply extend.
			log = append(log, ents...)
		} else if kind == CMD_APPEND {
			ent, bs1 := util.DecodeString(bs)
			data = bs1
			// Apply append.
			log = append(log, ent)
		} else if kind == CMD_PREPARE {
			term, bs1 := marshal.ReadInt(bs)
			data = bs1
			// Apply prepare.
			termc = term
		} else if kind == CMD_ADVANCE {
			term, bs1 := marshal.ReadInt(bs)
			lsn, bs2 := marshal.ReadInt(bs1)
			ents, bs3 := util.DecodeStrings(bs2)
			data = bs3
			// Apply advance.
			terml = term
			// Prove safety of this triming operation using well-formedness of
			// the write-ahead log. See [execute_paxos_advance] in recovery.v.
			log = log[: lsn]
			log = append(log, ents...)
		} else if kind == CMD_ACCEPT {
			lsn, bs1 := marshal.ReadInt(bs)
			ents, bs2 := util.DecodeStrings(bs1)
			data = bs2
			// Apply accept.
			// Prove safety of this triming operation using well-formedness of
			// the write-ahead log. See [execute_paxos_accept] in recovery.v.
			log = log[: lsn]
			log = append(log, ents...)
		} else if kind == CMD_EXPAND {
			lsn, bs1 := marshal.ReadInt(bs)
			data = bs1
			// Apply expand.
			lsnc = lsn
		}
	}
	
	return termc, terml, lsnc, log
}
