package quorum

func FastQuorum(n uint64) uint64 {
	// ceiling(3n / 4)
	return (n + 1) * 3 / 4
}

func ClassicQuorum(n uint64) uint64 {
	// floor(n / 2) + 1
	return n / 2 + 1
}

func Half(n uint64) uint64 {
	// ceiling(n / 2)
	return (n + 1) / 2
}
