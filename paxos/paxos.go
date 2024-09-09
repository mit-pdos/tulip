package paxos

type Paxos struct {
}

// TODO: Figure do we need to return the term?
func (px *Paxos) Propose(v string) (uint64, uint64) {
	return 0, 0
}

func (px *Paxos) Lookup(i uint64) (string, bool) {
	return "", false
}

// TODO: This really should be returning a slice of paxos objects, but now for
// simplicity let's just return one of them.
func MkPaxos() *Paxos {
	var px *Paxos

	return px
}
