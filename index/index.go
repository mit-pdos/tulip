package index

import (
	"sync"
	"github.com/mit-pdos/tulip/tuple"
)

type Index struct {
	mu   *sync.Mutex
	tplm map[string]*tuple.Tuple
}

func (idx *Index) GetTuple(key string) *tuple.Tuple {
	idx.mu.Lock()
	tpl, ok := idx.tplm[key]

	// Return the tuple if there already exists one.
	if ok {
		idx.mu.Unlock()
		return tpl
	}

	tplnew := tuple.MkTuple()
	idx.tplm[key] = tplnew

	idx.mu.Unlock()
	return tplnew
}

func MkIndex() *Index {
	idx := new(Index)
	idx.mu = new(sync.Mutex)
	idx.tplm = make(map[string]*tuple.Tuple)
	return idx
}
