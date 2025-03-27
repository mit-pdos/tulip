package ycsb

import "github.com/mit-pdos/tulip/main/workload"

const (
	OP_RD = iota
	OP_WR
	OP_SCAN
)

type Generator struct {
	// Number of keys accessed in an YCSB-transaction.
	sztxn   int
	// Read ratio in percentage.
	rdratio uint64
	// Value generator.
	gen     *workload.Generator
}

func NewGenerator(
	seed int,
	sztxn int, krange, szkey, szvalue, rdratio uint64,
	theta float64,
) *Generator {
	gen := &Generator{
		sztxn:   sztxn,
		rdratio: rdratio,
		gen:     workload.NewGenerator(seed, krange, szkey, szvalue, theta),
	}

	return gen
}

func (g *Generator) PickKey() string {
	return g.gen.PickKey()
}

func (g *Generator) PickValue() string {
	return g.gen.PickValue()
}

func (g *Generator) PickOp() int {
	x := g.gen.RandomUint64() % 100
	if x < g.rdratio {
		return OP_RD
	} else {
		return OP_WR
	}
}

func (g *Generator) SizeTxn() int {
	return g.sztxn
}

func (g *Generator) NextKey() string {
	return g.gen.NextKey()
}

func (g *Generator) HasNextKey() bool {
	return g.gen.HasNextKey()
}
