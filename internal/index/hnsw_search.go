package index

import (
	"sync"

	"github.com/sandeep89846/nebuladb/pkg/vec"
)

// candidate represents a node traversed during search.
type candidate struct {
	id   uint64
	dist float32
}

// ---------------------------
// minPQ (typed min-heap)
// ---------------------------

type minPQ struct {
	items []candidate
}

func (p *minPQ) Len() int { return len(p.items) }

func (p *minPQ) Push(c candidate) {
	p.items = append(p.items, c)
	p.siftUp(len(p.items) - 1)
}

func (p *minPQ) Pop() candidate {
	n := len(p.items)
	if n == 0 {
		return candidate{}
	}
	if n == 1 {
		x := p.items[0]
		p.items = p.items[:0]
		return x
	}
	// pop root
	root := p.items[0]
	// move last to root and sift down
	last := p.items[n-1]
	p.items[n-1] = candidate{} // help GC
	p.items = p.items[:n-1]
	p.items[0] = last
	p.siftDown(0)
	return root
}

func (p *minPQ) Peek() (candidate, bool) {
	if len(p.items) == 0 {
		return candidate{}, false
	}
	return p.items[0], true
}

func (p *minPQ) Reset() {
	p.items = p.items[:0]
}

func (p *minPQ) siftUp(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if p.items[i].dist < p.items[parent].dist {
			p.items[i], p.items[parent] = p.items[parent], p.items[i]
			i = parent
			continue
		}
		break
	}
}

func (p *minPQ) siftDown(i int) {
	n := len(p.items)
	for {
		l := 2*i + 1
		if l >= n {
			break
		}
		smallest := l
		r := l + 1
		if r < n && p.items[r].dist < p.items[l].dist {
			smallest = r
		}
		if p.items[smallest].dist < p.items[i].dist {
			p.items[i], p.items[smallest] = p.items[smallest], p.items[i]
			i = smallest
			continue
		}
		break
	}
}

// ---------------------------
// maxBoundedPQ (typed bounded max-heap)
// Keeps at most capacity items. Root is the maximum (furthest) distance.
// ---------------------------

type maxBoundedPQ struct {
	items    []candidate
	capacity int
}

func newMaxBoundedPQ(cap int) *maxBoundedPQ {
	if cap <= 0 {
		cap = 1
	}
	return &maxBoundedPQ{
		items:    make([]candidate, 0, cap),
		capacity: cap,
	}
}

func (p *maxBoundedPQ) Len() int { return len(p.items) }

func (p *maxBoundedPQ) capacityReached() bool {
	return len(p.items) >= p.capacity
}

// root is the maximum (furthest) candidate
func (p *maxBoundedPQ) Peek() (candidate, bool) {
	if len(p.items) == 0 {
		return candidate{}, false
	}
	return p.items[0], true
}

func (p *maxBoundedPQ) Push(c candidate) {
	n := len(p.items)
	if n < p.capacity {
		p.items = append(p.items, c)
		p.siftUpMax(n)
		return
	}
	// capacity reached
	if n == 0 {
		return
	}
	// if new is closer than current furthest (root), replace root and sift down
	if c.dist < p.items[0].dist {
		p.items[0] = c
		p.siftDownMax(0)
	}
}

func (p *maxBoundedPQ) Pop() candidate {
	n := len(p.items)
	if n == 0 {
		return candidate{}
	}
	if n == 1 {
		x := p.items[0]
		p.items = p.items[:0]
		return x
	}
	root := p.items[0]
	last := p.items[n-1]
	p.items[n-1] = candidate{} // help GC
	p.items = p.items[:n-1]
	p.items[0] = last
	p.siftDownMax(0)
	return root
}

// PopAll returns items in order Root->... i.e., furthest -> closest
func (p *maxBoundedPQ) PopAll() []candidate {
	out := make([]candidate, 0, len(p.items))
	for p.Len() > 0 {
		out = append(out, p.Pop())
	}
	return out
}

func (p *maxBoundedPQ) Reset(capacity int) {
	p.capacity = capacity
	p.items = p.items[:0]
	// ensure capacity
	if cap(p.items) < capacity {
		p.items = make([]candidate, 0, capacity)
	}
}

func (p *maxBoundedPQ) siftUpMax(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if p.items[i].dist > p.items[parent].dist {
			p.items[i], p.items[parent] = p.items[parent], p.items[i]
			i = parent
			continue
		}
		break
	}
}

func (p *maxBoundedPQ) siftDownMax(i int) {
	n := len(p.items)
	for {
		l := 2*i + 1
		if l >= n {
			break
		}
		largest := l
		r := l + 1
		if r < n && p.items[r].dist > p.items[l].dist {
			largest = r
		}
		if p.items[largest].dist > p.items[i].dist {
			p.items[i], p.items[largest] = p.items[largest], p.items[i]
			i = largest
			continue
		}
		break
	}
}

// ---------------------------
// Pools
// ---------------------------

var candidatePool = sync.Pool{
	New: func() any {
		p := &minPQ{items: make([]candidate, 0, 64)}
		return p
	},
}

var resultPool = sync.Pool{
	New: func() any {
		// default capacity 64; reset with desired ef on use
		return newMaxBoundedPQ(64)
	},
}

var visitedPool = sync.Pool{
	New: func() any { m := make(map[uint64]bool); return &m },
}

// ---------------------------
// searchLayer (rewirte using typed heaps)
// ---------------------------

// searchLayer performs a greedy graph traversal at a specific layer.
// Returns a bounded max-heap of the best 'ef' nodes found.
func (h *HNSW) searchLayer(query vec.Vector, entryPointIDs []uint64, ef int, layer int) *maxBoundedPQ {
	// Acquire candidate queue from pool and reset it.
	cp := candidatePool.Get().(*minPQ)
	cp.Reset()

	// Acquire visited map from pool and reset it.
	vp := visitedPool.Get().(*map[uint64]bool)
	visited := *vp
	for k := range visited {
		delete(visited, k)
	}

	// Acquire result PQ from pool and reset with capacity ef
	rp := resultPool.Get().(*maxBoundedPQ)
	rp.Reset(ef)

	for _, epID := range entryPointIDs {
		node := h.nodeByID(epID)
		if node == nil {
			continue
		}

		dist := h.dist(query, node.vec)
		visited[epID] = true

		c := candidate{id: epID, dist: dist}
		cp.Push(c)
		rp.Push(c)
	}

	for cp.Len() > 0 {
		// Explore the closest candidate first
		curr := cp.Pop()

		if rp.Len() >= ef {
			if root, ok := rp.Peek(); ok {
				if curr.dist > root.dist {
					break
				}
			}
		}

		currNode := h.nodeByID(curr.id)
		if currNode == nil {
			continue
		}

		currNode.mu.RLock()
		if layer >= len(currNode.neighbors) {
			currNode.mu.RUnlock()
			continue
		}
		neighbors := make([]uint64, len(currNode.neighbors[layer]))
		copy(neighbors, currNode.neighbors[layer])
		currNode.mu.RUnlock()

		neighborNodes := h.snapshotNodes(neighbors)

		for _, neighborNode := range neighborNodes {
			neighborID := neighborNode.id
			if visited[neighborID] {
				continue
			}
			visited[neighborID] = true

			d := h.dist(query, neighborNode.vec)

			if rp.Len() < ef {
				cp.Push(candidate{id: neighborID, dist: d})
				rp.Push(candidate{id: neighborID, dist: d})
			} else {
				if root, ok := rp.Peek(); ok && d < root.dist {
					cp.Push(candidate{id: neighborID, dist: d})
					rp.Push(candidate{id: neighborID, dist: d})
				}
			}
		}
	}

	cp.Reset()
	candidatePool.Put(cp)

	for k := range visited {
		delete(visited, k)
	}
	visitedPool.Put(&visited)

	return rp
}
