package index

import (
	"math"
	"math/rand/v2"
	"sync"

	"github.com/sandeep89846/nebuladb/pkg/vec"
)

// Configuration for HNSW hyperparmeters.
type Config struct {
	// M : the maximum number of connections per element per layer.
	M int

	// EfConstrction : size of dynamic candidate list.
	EfConstrction int

	// M0 : max connections in the bottom layer.
	M0 int

	// parameter for random level generation
	LevelMultiplier float64
}

// Default config.
func DefaultConfig() Config {
	m := 16
	return Config{
		M:               m,
		M0:              2 * m,
		EfConstrction:   200,
		LevelMultiplier: 1.0 / math.Log(float64(m)), // 0.25 here.
	}
}

// Node is a point in the HNSW graph.
type Node struct {
	id    uint64
	level int
	vec   vec.Vector

	// adj list of neighbors.
	neighbors [][]uint64

	// lock to protect neighbors list during insertions.
	lock sync.RWMutex
}

// HNSW : the index structure.
type HNSW struct {
	config Config

	//entryPointID : First node through which search or insertions work through.
	entryPointID uint64 // needs atomic access for updation.

	currentMaxLevel int64 // needs atomic access for updation.

	nodes map[uint64]*Node

	nextId uint64

	globalLock sync.RWMutex // Need to change later.
}

// NewHNSW creates a new index
func NewHNSW(cfg Config) *HNSW {
	return &HNSW{
		config:          cfg,
		nodes:           make(map[uint64]*Node),
		currentMaxLevel: -1, // Empty graph has no levels
		entryPointID:    0,
	}
}

func (h *HNSW) randomLevel() int {
	lvl := 0
	for rand.Float64() < 0.5 { // Simplified probability; we'll refine this
		lvl++
	}
	return lvl
}
