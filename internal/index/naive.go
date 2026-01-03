package index

import (
	"container/heap"
	"sync"

	"github.com/sandeep89846/nebuladb/pkg/vec"
)

type NaiveIndex struct {
	store map[string]vec.Vector
	mu    sync.RWMutex
}

func NewNaiveIndex() *NaiveIndex {
	return &NaiveIndex{
		store: make(map[string]vec.Vector),
	}
}

func (n *NaiveIndex) Insert(id string, v vec.Vector) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.store[id] = v
	return nil
}

func (n *NaiveIndex) Search(query vec.Vector, k int) ([]Match, error) {
	n.mu.RLock() // only allow reads during the process.
	defer n.mu.RUnlock()

	pq := &MatchQueue{}
	heap.Init(pq)

	// O(n) scan
	for id, v := range n.store {
		score, err := vec.CosineSimilarity(query, v)
		if err != nil {
			continue
		}
		pq.PushWithLimit(Match{ID: id, Score: score}, k)
	}

	results := make([]Match, pq.Len())
	for i := len(results) - 1; i >= 0; i-- {
		results[i] = heap.Pop(pq).(Match)
	}

	return results, nil
}
