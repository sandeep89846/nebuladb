package index

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/sandeep89846/nebuladb/pkg/vec"
)

func init() {
	// deterministic tests / benchmarks
	rand.Seed(42)
}

// --- Helper Functions ---

func randomVec(dim int) vec.Vector {
	v := make(vec.Vector, dim)
	for i := range v {
		v[i] = rand.Float32()
	}
	return v
}

// --- Functional Tests ---

func TestHNSW_Sanity(t *testing.T) {
	cfg := DefaultConfig()
	cfg.M = 10
	cfg.EfConstruction = 50
	idx := NewHNSW(cfg)

	// 1. Insert 100 vectors
	dim := 128
	for i := 0; i < 100; i++ {
		err := idx.Insert(fmt.Sprintf("vec_%d", i), randomVec(dim))
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	targetID := "vec_50"

	idx.globalLock.RLock()
	internalID := idx.idToInternal[targetID]
	idx.globalLock.RUnlock()

	targetVecNode := idx.nodeByID(internalID)
	if targetVecNode == nil {
		t.Fatalf("internal node not found for %s", targetID)
	}
	targetVec := targetVecNode.vec

	results, err := idx.Search(targetVec, 5)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("Got 0 results")
	}

	if results[0].ID != targetID {
		t.Errorf("Top result should be %s, got %s", targetID, results[0].ID)
	}

	if results[0].Score < 0.999 {
		t.Errorf("Top result score should be ~1.0, got %f", results[0].Score)
	}
}

// --- Recall Accuracy Test ---
func TestHNSW_Recall(t *testing.T) {
	// Setup
	count := 1000 // Enough to be interesting, small enough to run fast
	dim := 64
	k := 10

	naive := NewNaiveIndex()
	hnsw := NewHNSW(DefaultConfig())

	fmt.Printf("Generating %d vectors.\n", count)

	// Data generation
	dataset := make([]vec.Vector, count)
	for i := 0; i < count; i++ {
		dataset[i] = randomVec(dim)
		id := fmt.Sprintf("id_%d", i)

		naive.Insert(id, dataset[i])
		hnsw.Insert(id, dataset[i])
	}

	// Test with 50 random queries
	queries := 50
	totalRecall := 0.0

	fmt.Println("Running recall queries.")

	for i := 0; i < queries; i++ {
		query := randomVec(dim)

		truth, _ := naive.Search(query, k)

		prediction, _ := hnsw.Search(query, k)

		matches := 0
		truthMap := make(map[string]bool)
		for _, m := range truth {
			truthMap[m.ID] = true
		}

		for _, m := range prediction {
			if truthMap[m.ID] {
				matches++
			}
		}

		recall := float64(matches) / float64(k)
		totalRecall += recall
	}

	avgRecall := totalRecall / float64(queries)
	fmt.Printf("Average Recall: %.2f%%\n", avgRecall*100)

	// Threshold
	if avgRecall < 0.9 {
		t.Errorf("Recall too low: got %.2f, want > 0.9", avgRecall)
	}
}

func TestHNSW_ConcurrentStress(t *testing.T) {

	cfg := DefaultConfig()
	idx := NewHNSW(cfg)
	dim := 128

	// Parameters
	numInserts := 1000
	numSearches := 1000
	concurrency := 10

	var wg sync.WaitGroup
	start := make(chan struct{})

	// 2. Writer Goroutines
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			<-start

			for j := 0; j < numInserts/concurrency; j++ {
				id := fmt.Sprintf("w_%d_%d", workerID, j)

				_ = idx.Insert(id, randomVec(dim))
			}
		}(i)
	}

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(waitgroup int) {
			defer wg.Done()
			<-start

			query := randomVec(dim)
			for j := 0; j < numSearches/concurrency; j++ {
				matches, err := idx.Search(query, 10)
				if err != nil {
					t.Errorf("Search error: %v", err)
				}

				_ = matches
			}
		}(i)
	}

	close(start)
	wg.Wait()

	if len(idx.nodes) == 0 {
		t.Error("Graph is empty after inserts")
	}
}

// --- Benchmarks ---

func BenchmarkHNSW_Insert(b *testing.B) {
	idx := NewHNSW(DefaultConfig())
	dim := 128
	v := randomVec(dim)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Insert(fmt.Sprintf("%d", i), v)
	}
}

func BenchmarkHNSW_Search(b *testing.B) {

	idx := NewHNSW(DefaultConfig())
	dim := 128
	count := 10000

	for i := 0; i < count; i++ {
		idx.Insert(fmt.Sprintf("%d", i), randomVec(dim))
	}

	query := randomVec(dim)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		idx.Search(query, 10)
	}
}
