package paxos

import (
	"sync"
	"github.com/goose-lang/primitive"
	"github.com/mit-pdos/gokv/grove_ffi"
	"github.com/mit-pdos/tulip/params"
	"github.com/mit-pdos/tulip/message"
	"github.com/mit-pdos/tulip/util"
	"github.com/mit-pdos/tulip/quorum"
)

// TODOs:
// 1. initialization
// 2. crash recovery
// 3. marshaling

type Paxos struct {
	// ID of this node.
	nidme     uint64
	// Node ID of its peers.
	peers     []uint64
	// Addresses of other Paxos nodes.
	addrm     map[uint64]grove_ffi.Address
	// Size of the cluster. @sc = @len(peers) + 1.
	sc        uint64
	// Mutex protecting fields below.
	mu        *sync.Mutex
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

	// Potential batch optimization: Even though we update @px.log here, but it
	// should be OK to not immediately write them to disk, but wait until those
	// entries are sent in @LeaderSession. To prove the optimization, we'll need
	// to decouple the "batched entries" from the actual entries @px.log, and
	// relate only @px.log to the invariant.

	px.mu.Unlock()
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

	// Logical action: Prepare(@term).

	return term, lsn
}

func (px *Paxos) stepdown(term uint64) {
	px.termc = term
	px.iscand = false
	px.isleader = false

	// TODO: Write @px.termc to disk.

	// Logical action: Prepare(@term).
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

	// Logical action: Ascend(@px.termc, @px.log).

	// TODO: Write @px.log and @px.terml to disk.
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

		// TODO: Write @px.log and @px.terml to disk.

		// Return LSN at the end of our log after accepting @ents.
		lsna := uint64(len(px.log))

		// Logical action: Advance(term, log).

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

	// TODO: Write @px.log to disk.

	lsna := uint64(len(px.log))

	// Logical action: Accept(term, log)

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

		// Logical action: Expand(len(px.log))

		return
	}

	px.lsnc = lsn

	// Logical action: Expand(lsn)

	// TODO: Write @px.lsnc to disk.
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

func (px *Paxos) heartbeat() {
	px.hb = true
}

func (px *Paxos) heartbeated() bool {
	return px.hb
}

func (px *Paxos) LeaderSession() {
	for {
		primitive.Sleep(params.NS_BATCH_INTERVAL)

		px.mu.Lock()

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
				data := message.EncodePaxosAppendEntriesRequest(termc, lsnc, lsne, ents)
				px.Send(nid, data)
			}()
		}

		px.mu.Unlock()
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
			px.mu.Unlock()
			continue
		}

		px.heartbeat()

		termc, lsnc := px.nominate()

		px.mu.Unlock()

		for _, nidloop := range(px.peers) {
			nid := nidloop
			go func() {
				data := message.EncodePaxosRequestVoteRequest(termc, lsnc)
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

		resp := message.DecodePaxosResponse(data)
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

		if kind == message.MSG_PAXOS_REQUEST_VOTE {
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
			px.collect(resp.NodeID, resp.TermEntries, resp.Entries)
			px.ascend()
			px.mu.Unlock()
		} else if kind == message.MSG_PAXOS_APPEND_ENTRIES {
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

		req  := message.DecodePaxosRequest(ret.Data)
		kind := req.Kind

		px.mu.Lock()

		if px.lttermc(req.Term) {
			// Skip the oudated message.
			px.mu.Unlock()

			// We can additionally send an UPDATE-TERM message, but not sure if
			// that's necessary, since eventually the new leader would reach out
			// to every node.
			continue
		}

		// Potentially proceed to a new term on receiving a higher-term message.
		px.stepdown(req.Term)

		px.heartbeat()

		termc := px.gettermc()

		if kind == message.MSG_PAXOS_REQUEST_VOTE {
			if px.latest() {
				// The log has already matched up the current term, meaning the
				// leader has already successfully been elected. Simply ignore
				// this request.
				px.mu.Unlock()
				continue
			}
			terml, ents := px.prepare(req.CommittedLSN)
			px.mu.Unlock()
			data := message.EncodePaxosRequestVoteResponse(px.nidme, termc, terml, ents)
			// Request [REQUEST-VOTE, @termc, @lsnc] and
			// Response [REQUEST-VOTE, @termc, @terml, @ents] means:
			// (1) This node will not accept any proposal with term below @termc.
			// (2) The largest-term entries after LSN @lsnc this node has
			// accepted before @termc is (@terml, @ents).
			grove_ffi.Send(conn, data)
		} else if kind == message.MSG_PAXOS_APPEND_ENTRIES {
			lsn := px.accept(req.EntriesLSN, req.Term, req.Entries)
			px.learn(req.CommittedLSN, req.Term)
			px.mu.Unlock()
			data := message.EncodePaxosAppendEntriesResponse(px.nidme, termc, lsn)
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

func mkPaxos(nidme, termc, terml, lsnc uint64, log []string, addrm map[uint64]grove_ffi.Address) *Paxos {
	sc := uint64(len(addrm))

	var peers = make([]uint64, 0, sc - 1)
	for nid := range(addrm) {
		if nid != nidme {
			peers = append(peers, nid)
		}
	}

	px := &Paxos{
		nidme    : nidme,
		peers    : peers,
		addrm    : addrm,
		sc       : sc,
		mu       : new(sync.Mutex),
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

func Start(nidme uint64, addrm map[uint64]grove_ffi.Address) *Paxos {
	// Check that the cluster has more than one node.
	primitive.Assert(1 < uint64(len(addrm)))

	// Check that @nidme is part of the cluster.
	_, ok := addrm[nidme]
	primitive.Assert(ok)

	// Check the @nidme is valid.
	primitive.Assert(nidme < MAX_NODES)

	// TODO: Read the underlying file and perform recovery to re-construct
	// @termc, @terml, @lsnc, and @log.
	var termc uint64
	var terml uint64
	var lsnc  uint64
	log := make([]string, 0)

	px := mkPaxos(nidme, termc, terml, lsnc, log, addrm)

	go func() {
		px.Serve()
	}()

	go func() {
		px.LeaderSession()
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
