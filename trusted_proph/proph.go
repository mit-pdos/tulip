package trusted_proph

import (
	"github.com/mit-pdos/tulip/tulip"
	"github.com/goose-lang/primitive"
)

type ProphId = primitive.ProphId
func ResolveRead(p ProphId, tid uint64, key string)                              {}
func ResolveAbort(p ProphId, tid uint64)                                         {}
func ResolveCommit(p ProphId, tid uint64, wrs map[uint64]map[string]tulip.Value) {}
