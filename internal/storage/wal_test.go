package storage

import (
	"os"
	"testing"

	"github.com/sandeep89846/nebuladb/pkg/vec"
)

func TestWAL_WriteAndReplay(t *testing.T) {

	tmpFile := "test_wal.bin"
	defer os.Remove(tmpFile)

	wal, err := OpenWAL(tmpFile)
	if err != nil {
		t.Fatal(err)
	}

	testData := []struct {
		id string
		v  vec.Vector
	}{
		{"vec1", vec.Vector{1.0, 2.0, 3.0}},
		{"vec2", vec.Vector{0.5, 0.5, 0.5}},
	}

	for _, d := range testData {
		if err := wal.WriteInsert(d.id, d.v); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}
	wal.Close()

	wal2, err := OpenWAL(tmpFile)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	replayedCount := 0
	err = wal2.Replay(func(id string, v vec.Vector) {
		// Verify correctness
		expected := testData[replayedCount]
		if id != expected.id {
			t.Errorf("Mismatch ID: got %s, want %s", id, expected.id)
		}
		if len(v) != len(expected.v) {
			t.Errorf("Mismatch Vec Len")
		}
		replayedCount++
	})

	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if replayedCount != 2 {
		t.Errorf("Expected 2 entries, got %d", replayedCount)
	}
}
