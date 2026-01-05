package index

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/sandeep89846/nebuladb/pkg/vec"
)

// Helper (Task :Check for Possible optimizations)
func randomVector(dim int) vec.Vector {
	v := make(vec.Vector, dim)
	for i := range v {
		v[i] = rand.Float32()
	}
	return v
}

func BenchmarkNaiveSearch(b *testing.B) {

	datasetSizes := []int{100, 1000, 10000}

	for _, n := range datasetSizes {
		b.Run(fmt.Sprintf("DatasetSize_%d", n), func(b *testing.B) {
			idx := NewNaiveIndex()
			dim := 128

			for i := 0; i < n; i++ {
				id := fmt.Sprintf("vec_%d", i)
				idx.Insert(id, randomVector(dim))
			}

			query := randomVector(dim)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, _ = idx.Search(query, 5)
			}
		})
	}
}
