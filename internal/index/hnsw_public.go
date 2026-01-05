package index

import (
	"fmt"

	"github.com/sandeep89846/nebuladb/pkg/vec"
)

// Search implements the VectorIndex interface
func (h *HNSW) Search(query vec.Vector, k int) ([]Match, error) {
	// Validate & normalize query
	if len(query) == 0 {
		return nil, fmt.Errorf("empty query vector")
	}
	mag := vec.Magnitude(query)
	if mag == 0 {
		return nil, fmt.Errorf("zero-magnitude query vector")
	}
	nq := make(vec.Vector, len(query))
	for i := range query {
		nq[i] = query[i] / mag
	}

	h.globalLock.RLock()
	entryPointID := h.entryPointID
	maxLevel := h.maxLevel
	h.globalLock.RUnlock()

	if maxLevel == -1 {
		return []Match{}, nil
	}

	currObjID := entryPointID

	for l := maxLevel; l > 0; l-- {
		res := h.searchLayer(nq, []uint64{currObjID}, 1, l)
		if res.Len() > 0 {

			currObjID = res.Pop().id
		}
		// MEMORY FIX: Return the queue to the pool!
		resultPool.Put(res)
	}

	// Layer 0: The Full Search
	efSearch := h.config.EfSearch
	if efSearch < k {
		efSearch = k
	}

	res := h.searchLayer(nq, []uint64{currObjID}, efSearch, 0)

	// res.PopAll() returns Furthest->Closest
	allCandidates := res.PopAll()

	// MEMORY FIX: Return the queue to the pool!
	resultPool.Put(res)

	finalMatches := make([]Match, 0, k)

	// Iterate backwards to get Closest -> Furthest
	count := 0
	for i := len(allCandidates) - 1; i >= 0; i-- {
		c := allCandidates[i]

		h.globalLock.RLock()
		externalID := h.internalToID[c.id]
		h.globalLock.RUnlock()

		// Dist = 1 - Sim  =>  Sim = 1 - Dist
		score := 1.0 - c.dist

		finalMatches = append(finalMatches, Match{
			ID:    externalID,
			Score: score,
		})

		count++
		if count >= k {
			break
		}
	}

	return finalMatches, nil
}
