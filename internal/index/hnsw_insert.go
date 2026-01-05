package index

import (
	"fmt"
	"math"
	"sync/atomic"

	"github.com/sandeep89846/nebuladb/pkg/vec"
)

// Insert adds a vector to the index.
func (h *HNSW) Insert(id string, v vec.Vector) error {

	h.globalLock.RLock()
	_, exists := h.idToInternal[id]
	h.globalLock.RUnlock()
	if exists {
		return fmt.Errorf("vector with ID %s already exists", id)
	}

	// Validate & normalize vector
	if len(v) == 0 {
		return fmt.Errorf("empty vector")
	}
	mag := vec.Magnitude(v)
	if mag == 0 {
		return fmt.Errorf("zero-magnitude vector")
	}
	normalized := make(vec.Vector, len(v))
	for i := range v {
		normalized[i] = v[i] / mag
	}

	internalID := atomic.AddUint64(&h.nextID, 1)
	level := h.randomLevel()

	node := &Node{
		id:        internalID,
		vec:       normalized,
		level:     level,
		neighbors: make([][]uint64, level+1),
	}

	h.globalLock.Lock()
	// Re-check to avoid race where another goroutine inserted same ID
	if _, exists := h.idToInternal[id]; exists {
		h.globalLock.Unlock()
		return fmt.Errorf("vector with ID %s already exists", id)
	}

	h.idToInternal[id] = internalID
	h.internalToID[internalID] = id

	// Place node into slice at index internalID-1 (grow if necessary)
	idx := int(internalID - 1)
	if idx == len(h.nodes) {
		h.nodes = append(h.nodes, node)
	} else if idx < len(h.nodes) {
		h.nodes[idx] = node
	} else {
		// IDs obtained out-of-order (append filler nils)
		newNodes := make([]*Node, idx+1)
		copy(newNodes, h.nodes)
		newNodes[idx] = node
		h.nodes = newNodes
	}

	entryPointID := h.entryPointID
	maxLevel := h.maxLevel

	// First node scenario
	if maxLevel == -1 {
		h.entryPointID = internalID
		h.maxLevel = level
		h.globalLock.Unlock()
		return nil
	}
	h.globalLock.Unlock()

	// Find the closest node at the insertion level
	currObjID := entryPointID

	currNode := h.nodeByID(currObjID)
	if currNode == nil {
		// Fallback: set to our own node
		currObjID = internalID
		currNode = node
	}

	currDist := h.dist(normalized, currNode.vec)

	// Traverse layers down to the node's top level
	for l := maxLevel; l > level; l-- {
		changed := true
		for changed {
			changed = false

			// Safely get neighbors slice
			currNode.mu.RLock()
			if l >= len(currNode.neighbors) {
				currNode.mu.RUnlock()
				break
			}
			neighbors := make([]uint64, len(currNode.neighbors[l]))
			copy(neighbors, currNode.neighbors[l])
			currNode.mu.RUnlock()

			neighborNodes := h.snapshotNodes(neighbors)

			for _, neighborNode := range neighborNodes {
				d := h.dist(normalized, neighborNode.vec)
				if d < currDist {
					currDist = d
					currObjID = neighborNode.id
					currNode = neighborNode
					changed = true
				}
			}
		}
	}

	topLevel := int(math.Min(float64(maxLevel), float64(level)))

	for l := topLevel; l >= 0; l-- {
		// Search for efConstruction neighbors
		searchRes := h.searchLayer(normalized, []uint64{currObjID}, h.config.EfConstruction, l)

		// Select M neighbors to connect to
		neighborsToAdd := h.selectNeighbors(searchRes, h.config.M)

		// Link: NewNode -> Neighbors
		node.mu.Lock()
		node.neighbors[l] = neighborsToAdd
		node.mu.Unlock()

		// Link: Neighbors -> NewNode (Bidirectional)
		for _, neighborID := range neighborsToAdd {
			h.addBidirectionalConnection(neighborID, internalID, l)
		}

		// Update currObjID to the closest node found in this layer
		if searchRes.Len() > 0 {
			// PopAll gives Furthest->Closest
			all := searchRes.PopAll()
			closestDist := float32(math.MaxFloat32)
			for _, cand := range all {
				if cand.dist < closestDist {
					closestDist = cand.dist
					currObjID = cand.id
				}
			}
		}
	}

	h.globalLock.Lock()
	if level > h.maxLevel {
		h.maxLevel = level
		h.entryPointID = internalID
	}
	h.globalLock.Unlock()

	return nil
}

// Helper: Pick M closest from a bounded max-heap results
func (h *HNSW) selectNeighbors(results *maxBoundedPQ, m int) []uint64 {

	temp := results.PopAll()

	count := m
	if len(temp) < m {
		count = len(temp)
	}

	out := make([]uint64, 0, count)
	startIndex := len(temp) - count
	for i := startIndex; i < len(temp); i++ {
		out = append(out, temp[i].id)
	}
	return out
}

// addBidirectionalConnection adds guestID as a neighbor of hostID at given layer.
func (h *HNSW) addBidirectionalConnection(hostID, guestID uint64, layer int) {
	hostNode := h.nodeByID(hostID)
	if hostNode == nil {
		return
	}

	hostNode.mu.Lock()
	defer hostNode.mu.Unlock()

	// Ensure neighbors slice length
	if layer >= len(hostNode.neighbors) {

		newNeighbors := make([][]uint64, layer+1)
		copy(newNeighbors, hostNode.neighbors)
		hostNode.neighbors = newNeighbors
	}

	// Dedup check
	for _, n := range hostNode.neighbors[layer] {
		if n == guestID {
			return
		}
	}

	// Add connection
	hostNode.neighbors[layer] = append(hostNode.neighbors[layer], guestID)

	// Prune if over capacity
	limit := h.config.M
	if layer == 0 {
		limit = h.config.M0
	}

	if len(hostNode.neighbors[layer]) > limit {

		neighbors := make([]uint64, len(hostNode.neighbors[layer]))
		copy(neighbors, hostNode.neighbors[layer])

		neighborNodes := h.snapshotNodes(neighbors)

		worstIdx := -1
		var worstDist float32 = -1.0

		for i, nNode := range neighborNodes {
			d := h.dist(hostNode.vec, nNode.vec)
			if d > worstDist {
				worstDist = d
				worstIdx = i
			}
		}

		if worstIdx != -1 {
			toRemove := neighbors[worstIdx]

			l := len(hostNode.neighbors[layer])
			for i, id := range hostNode.neighbors[layer] {
				if id == toRemove {
					hostNode.neighbors[layer][i] = hostNode.neighbors[layer][l-1]
					hostNode.neighbors[layer] = hostNode.neighbors[layer][:l-1]
					break
				}
			}
		}
	}
}
