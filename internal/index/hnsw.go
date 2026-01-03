package index

import (
	"math"
	"math/rand/v2"
	"sync"

	"github.com/sandeep89846/nebuladb/pkg/vec"
)

type Config struct {
	M               int     // Max connections per layer
	M0              int     // Max connections at Layer 0 (usually 2*M)
	EfConstruction  int     // Search range during insertion
	LevelMultiplier float64 // Probabilistic factor
}

func DefaultConfig() Config {
	m := 16
	return Config{
		M:               m,
		M0:              m * 2,
		EfConstruction:  200,
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

	nodes        map[uint64]*Node
	entryPointID uint64
	maxLevel     int // Current highest layer

	// globalLock protects the map structure and entryPoint
	globalLock sync.RWMutex
}

func NewHNSW(cfg Config) *HNSW {
	return &HNSW{
		config:       cfg,
		idToInternal: make(map[string]uint64),
		internalToID: make(map[uint64]string),
		nodes:        make(map[uint64]*Node),
		maxLevel:     -1,
		nextID:       0,
	}
}

// randomLevel determines the height of a new node.
func (h *HNSW) randomLevel() int {
	lvl := 0
	for rand.Float64() < 0.5 {
		lvl++
	}
	return lvl
}

// dist calculates "Cosine Distance" (1 - Similarity).
func (h *HNSW) dist(v1, v2 vec.Vector) float32 {
	sim, _ := vec.CosineSimilarity(v1, v2)
	// Convert Similarity (-1 to 1) to Distance (0 to 2)
	// 1.0 sim -> 0.0 dist (Close)
	// -1.0 sim -> 2.0 dist (Far)
	return 1.0 - sim
}
