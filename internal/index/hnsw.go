package index

import (
	"math"
	"math/rand"
	"sync"

	"github.com/sandeep89846/nebuladb/pkg/vec"
)

type Config struct {
	M               int     // Max connections per layer
	M0              int     // Max connections at Layer 0 (usually 2*M)
	EfConstruction  int     // Search range during insertion
	EfSearch        int     // Default ef for search (tunable)
	LevelMultiplier float64 // Probabilistic factor
}

func DefaultConfig() Config {
	m := 16
	return Config{
		M:               m,
		M0:              m * 2,
		EfConstruction:  200,
		EfSearch:        50, // default ef used by Search if not overridden
		LevelMultiplier: 1.0 / math.Log(float64(m)),
	}
}

type Node struct {
	id    uint64
	level int
	vec   vec.Vector

	// adj list representation.
	neighbors [][]uint64

	mu sync.RWMutex
}

type HNSW struct {
	config Config

	idToInternal map[string]uint64
	internalToID map[uint64]string
	nextID       uint64 // Atomic counter

	nodes        []*Node
	entryPointID uint64
	maxLevel     int // Current highest layer

	// globalLock protects id maps, nodes slice, entryPoint, maxLevel
	globalLock sync.RWMutex
}

func NewHNSW(cfg Config) *HNSW {
	return &HNSW{
		config:       cfg,
		idToInternal: make(map[string]uint64),
		internalToID: make(map[uint64]string),
		nodes:        make([]*Node, 0),
		maxLevel:     -1,
		nextID:       0,
	}
}

// randomLevel determines the height of a new node using LevelMultiplier.
func (h *HNSW) randomLevel() int {
	mult := h.config.LevelMultiplier
	if mult <= 0 {
		lvl := 0
		for rand.Float64() < 0.5 {
			lvl++
		}
		return lvl
	}

	u := rand.Float64()
	if u <= 0 {
		return 0
	}
	lvl := int(-math.Log(u) * mult)
	if lvl < 0 {
		return 0
	}
	return lvl
}

// fastDot: a simple, fast float32 dot product optimized for typical vector lengths.
// Assumes both vectors have the same length. If lengths mismatch, returns ok=false.
// Unrolls loop by 4 for a small speedup (Replacement to earlier Dot func).
func fastDot(a, b vec.Vector) (float32, bool) {
	na := len(a)
	nb := len(b)
	if na != nb {
		return 0, false
	}
	var sum float32 = 0.0
	i := 0
	for ; i+3 < na; i += 4 {
		sum += a[i]*b[i] + a[i+1]*b[i+1] + a[i+2]*b[i+2] + a[i+3]*b[i+3]
	}
	for ; i < na; i++ {
		sum += a[i] * b[i]
	}
	return sum, true
}

// dist calculates "Cosine Distance" (1 - Similarity).
// Assumes vectors are normalized (magnitude == 1). Returns a large
// distance when dimensions mismatch (defensive).
func (h *HNSW) dist(v1, v2 vec.Vector) float32 {
	if v1 == nil || v2 == nil {
		return float32(math.MaxFloat32)
	}
	if dot, ok := fastDot(v1, v2); ok {
		return 1.0 - dot
	}
	return float32(math.MaxFloat32)
}

// nodeByID returns the *Node for a given internalID, or nil if not present.
// It acquires the read lock briefly.
func (h *HNSW) nodeByID(internalID uint64) *Node {
	h.globalLock.RLock()
	defer h.globalLock.RUnlock()
	if internalID == 0 {
		return nil
	}
	idx := int(internalID - 1)
	if idx < 0 || idx >= len(h.nodes) {
		return nil
	}
	return h.nodes[idx]
}

// snapshotNodes returns a slice of *Node for the provided internalIDs.
// This acquires a single RLock and collects pointers into a new slice.
func (h *HNSW) snapshotNodes(ids []uint64) []*Node {
	h.globalLock.RLock()
	defer h.globalLock.RUnlock()

	out := make([]*Node, 0, len(ids))
	for _, id := range ids {
		if id == 0 {
			continue
		}
		idx := int(id - 1)
		if idx >= 0 && idx < len(h.nodes) {
			n := h.nodes[idx]
			if n != nil {
				out = append(out, n)
			}
		}
	}
	return out
}
