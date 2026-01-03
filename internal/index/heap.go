package index

import "container/heap"

// MatchQueue is a priorty queue of Matches, ordered by Score.
// we use Min heap to keep track of the Top K largest scores.

type MatchQueue []Match

func (pq MatchQueue) Len() int           { return len(pq) }
func (pq MatchQueue) Less(i, j int) bool { return pq[i].Score < pq[j].Score }
func (pq MatchQueue) Swap(i, j int)      { pq[i], pq[j] = pq[j], pq[i] }

func (pq *MatchQueue) Push(x any) {
	*pq = append(*pq, x.(Match)) // use type-assertion.
}

func (pq *MatchQueue) Pop() any {
	old := *pq
	n := len(*pq)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

// Helper to exercise a size limit of K
func (pq *MatchQueue) PushWithLimit(item Match, k int) {
	heap.Push(pq, item)
	if len(*pq) > k {
		heap.Pop(pq)
	}
}
