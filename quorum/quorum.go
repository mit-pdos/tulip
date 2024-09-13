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

func swap(ns []uint64, i, j uint64) {
	tmp := ns[i]
	ns[i] = ns[j]
	ns[j] = tmp
}

func sort(ns []uint64) {
	var i uint64 = 1
	for i < uint64(len(ns)) {
		var j uint64 = i
		for j > 0 {
			if ns[j - 1] <= ns[j] {
				break
			}
			swap(ns, j - 1, j)
			j--
		}
		i++
	}
}

// NB: Follow the proof of wrbuf.sortEntsByKey
func Median(ns []uint64) uint64 {
	sort(ns)
	return ns[uint64(len(ns)) / 2]
}
