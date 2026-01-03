package index

import (
	"testing"
)

func TestHNSWInitialization(t *testing.T) {
	cfg := DefaultConfig()
	idx := NewHNSW(cfg)

	if idx.config.M != 16 {
		t.Errorf("Expected M=16, got %d", idx.config.M)
	}
	if idx.maxLevel != -1 {
		t.Errorf("Expected empty graph level -1, got %d", idx.maxLevel)
	}
}

func TestRandomLevel(t *testing.T) {
	// This is a probabilistic test, so we just check bounds
	cfg := DefaultConfig()
	idx := NewHNSW(cfg)

	for i := 0; i < 1000; i++ {
		lvl := idx.randomLevel()
		if lvl < 0 {
			t.Errorf("Level cannot be negative: %d", lvl)
		}
		// It's statistically very unlikely to get > 20 levels for M=16
		if lvl > 20 {
			t.Logf("Got unusually high level: %d", lvl)
		}
	}
}
