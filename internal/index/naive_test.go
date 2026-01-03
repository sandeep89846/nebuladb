package index

import (
	"testing"

	"github.com/sandeep89846/nebuladb/pkg/vec"
)

func TestNaiveIndex(t *testing.T) {
	idx := NewNaiveIndex()

	idx.Insert("A", vec.Vector{1, 0, 1})
	idx.Insert("B", vec.Vector{0, 1, 0})
	idx.Insert("C", vec.Vector{0, 3, 4})
	idx.Insert("D", vec.Vector{0, 1, 1})

	query := vec.Vector{0, 1, 1}
	results, _ := idx.Search(query, 2)

	if len(results) != 2 {
		t.Fatalf("Expected 2 results but got :%d", len(results))
	}
	if results[0].ID != "D" || results[1].ID != "C" {
		t.Fatalf("expected D,C as neighbors but got %s,%s", results[0].ID, results[1].ID)
	}
}
