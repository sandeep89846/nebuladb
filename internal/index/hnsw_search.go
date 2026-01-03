package index

import (
	"container/heap"

	"github.com/sandeep89846/nebuladb/pkg/vec"
)

type candidate struct {
	id   uint64
	dist float32
}

// Priority Queue for Candidates -- Min Heap over their distances.
type candidateQueue []candidate

func (cq candidateQueue) Len() int           { return len(cq) }
func (cq candidateQueue) Less(i, j int) bool { return cq[i].dist < cq[j].dist } // Min distance first
func (cq candidateQueue) Swap(i, j int)      { cq[i], cq[j] = cq[j], cq[i] }

func (cq *candidateQueue) Push(x any) {
	*cq = append(*cq, x.(candidate))
}

func (cq *candidateQueue) Pop() any {
	old := *cq
	n := len(old)
	item := old[n-1]
	*cq = old[0 : n-1]
	return item
}

// Max-Heap (Reverse of above)
// to track the "Furthest" element in our top-k list (so we can discard it).
type resultQueue []candidate

func (rq resultQueue) Len() int           { return len(rq) }
func (rq resultQueue) Less(i, j int) bool { return rq[i].dist > rq[j].dist } // Max distance first
func (rq resultQueue) Swap(i, j int)      { rq[i], rq[j] = rq[j], rq[i] }
func (rq *resultQueue) Push(x any)        { *rq = append(*rq, x.(candidate)) }
func (rq *resultQueue) Pop() any {
	old := *rq
	n := len(old)
	item := old[n-1]
	*rq = old[0 : n-1]
	return item
}

func (h *HNSW) searchLayer(query vec.Vector, entryPointIDs []uint64, ef int, layer int) *resultQueue {
	// needs Optimization later on.
	visited := make(map[uint64]bool)

	candidates := &candidateQueue{}
	heap.Init(candidates)

	results := &resultQueue{}
	heap.Init(results)

	// Initialize with entry points
	for _, epID := range entryPointIDs {
		h.globalLock.RLock()
		node := h.nodes[epID]
		h.globalLock.RUnlock()

		d := h.dist(query, node.vec)

		visited[epID] = true
		heap.Push(candidates, candidate{id: epID, dist: d})
		heap.Push(results, candidate{id: epID, dist: d})
	}

	for candidates.Len() > 0 {

		curr := heap.Pop(candidates).(candidate)

		furthestResult := (*results)[0]
		if curr.dist > furthestResult.dist && results.Len() >= ef {
			break
		}

		h.globalLock.RLock()
		currNode := h.nodes[curr.id]
		// We lock the node to read its neighbors safely
		currNode.mu.RLock()
		neighbors := currNode.neighbors[layer]
		currNode.mu.RUnlock()
		h.globalLock.RUnlock()

		for _, neighborID := range neighbors {
			if visited[neighborID] {
				continue
			}
			visited[neighborID] = true

			h.globalLock.RLock()
			neighborNode := h.nodes[neighborID]
			h.globalLock.RUnlock()

			d := h.dist(query, neighborNode.vec)

			// If the result list isn't full, just add it
			// OR if this neighbor is closer than the worst result we have
			if results.Len() < ef || d < (*results)[0].dist {
				heap.Push(candidates, candidate{id: neighborID, dist: d})
				heap.Push(results, candidate{id: neighborID, dist: d})

				// If we have too many results, remove the worst one
				if results.Len() > ef {
					heap.Pop(results)
				}
			}
		}

	}
	return results
}
