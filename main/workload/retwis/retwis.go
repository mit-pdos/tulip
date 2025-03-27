package retwis

import "github.com/mit-pdos/tulip/main/workload"

const (
	TXN_ADD_USER = iota
	TXN_FOLLOW
	TXN_POST_TWEET
	TXN_GET_TIMELINE
)

type Generator struct {
	// Value generator.
	gen     *workload.Generator
}

func NewGenerator(seed int, krange, szkey, szvalue uint64, theta float64) *Generator {
	gen := &Generator{
		gen: workload.NewGenerator(seed, krange, szkey, szvalue, theta),
	}

	return gen
}

func (g *Generator) PickKey() string {
	return g.gen.PickKey()
}

func (g *Generator) PickValue() string {
	return g.gen.PickValue()
}

func (g *Generator) PickTxn() int {
	x := g.gen.RandomUint64() % 100
	if x < 5 {
		return TXN_ADD_USER
	} else if x < 20 {
		return TXN_FOLLOW
	} else if x < 50 {
		return TXN_POST_TWEET
	} else {
		return TXN_GET_TIMELINE
	}
}

func (g *Generator) RandomInt() int {
	return g.gen.RandomInt()
}

func (g *Generator) NextKey() string {
	return g.gen.NextKey()
}

func (g *Generator) HasNextKey() bool {
	return g.gen.HasNextKey()
}
