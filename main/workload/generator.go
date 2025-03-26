package workload

import (
	"fmt"
	"math/rand"
	/* for Zipfian distribution */
	"github.com/pingcap/go-ycsb/pkg/generator"
)

const (
	DIST_UNIFORM = iota
	DIST_ZIPFIAN
)

type Generator struct {
	// Random number generator.
	rd      *rand.Rand
	// Key range.
	krange  uint64
	// Formater to create keys with padding.
	kfmt    string
	// Formater to create values with padding.
	vfmt    string
	// Next key to generate. Used for populating databases.
	next    uint64
	// Generator following the Zipfian distribution.
	zipfian *generator.Zipfian
	// Distribution kind.
	dist    int
}

func NewGenerator(seed int, krange, szkey, szvalue uint64, theta float64) *Generator {
	rd := rand.New(rand.NewSource(int64(seed)))

	var zipfian *generator.Zipfian
	var dist int
	if theta == -1 {
		dist = DIST_UNIFORM
	} else {
		dist = DIST_ZIPFIAN
		zipfian = generator.NewZipfianWithItems(int64(krange), theta)
	}

	kfmt := fmt.Sprintf("%%.%dd", szkey)
	vfmt := fmt.Sprintf("%%.%dd", szvalue)

	gen := &Generator{
		rd:      rd,
		krange:  krange,
		kfmt:    kfmt,
		vfmt:    vfmt,
		next:    0,
		zipfian: zipfian,
		dist:    dist,
	}

	return gen
}

func (g *Generator) PickOp() uint64 {
	return g.rd.Uint64() % 100
}

func (g *Generator) pickKeyUniform() uint64 {
	return g.rd.Uint64() % g.krange
}

func (g *Generator) pickKeyZipfian() uint64 {
	return uint64(g.zipfian.Next(g.rd))
}

func (g *Generator) PickKey() string {
	var n uint64
	if g.dist == DIST_ZIPFIAN {
		n = g.pickKeyZipfian()
	} else if g.dist == DIST_UNIFORM {
		n = g.pickKeyUniform()
	} else {
		panic("incorrect distribution kind")
	}

	return fmt.Sprintf(g.kfmt, n)
}

func (g *Generator) PickValue() string {
	return fmt.Sprintf(g.vfmt, 0)
}

func (g *Generator) NextKey() string {
	n := g.next
	g.next++

	return fmt.Sprintf(g.kfmt, n)
}

func (g *Generator) HasNextKey() bool {
	if g.next >= g.krange {
		return false
	}

	return true
}
