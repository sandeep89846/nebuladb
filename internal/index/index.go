package index

import "github.com/sandeep89846/nebuladb/pkg/vec"

type Match struct {
	ID    string
	Score float32
}

// VectorIndex : Defines the contract for any vector indexing algorithm.
// (Brute Force, HNSW, IVF, etc.)
type VectorIndex interface {
	Insert(id string, v vec.Vector) error
	Search(query vec.Vector, k int) ([]Match, error)
}
