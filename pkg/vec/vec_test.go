package vec

import (
	"testing"
)

const eps float32 = 1e-5

func TestCosineSimilarity(t *testing.T) {
	tests := []struct {
		name    string
		v1      Vector
		v2      Vector
		want    float32
		wantErr bool
	}{
		{
			name: "Identical Vectors",
			v1:   Vector{1, 0, 0},
			v2:   Vector{1, 0, 0},
			want: 1.0,
		},
		{
			name: "Orthogonal Vectors",
			v1:   Vector{1, 0},
			v2:   Vector{0, 1},
			want: 0.0,
		},
		{
			name: "Opposite Vectors",
			v1:   Vector{0, 1, 0},
			v2:   Vector{0, -1, 0},
			want: -1.0,
		},
		{
			name:    "Dimension Mismatch",
			v1:      Vector{1, 0},
			v2:      Vector{0, 1, 0},
			want:    0.0,
			wantErr: true,
		},
		{
			name:    "Flag division by zero",
			v1:      Vector{1, 0},
			v2:      Vector{0, 0},
			want:    0.0,
			wantErr: true,
		},
	}

	cmp := func(f1, f2 float32) bool {
		diff := f1 - f2
		if diff < 0 {
			diff *= -1.0
		}
		return eps >= diff
	}

	for _, tt := range tests {

		got, err := CosineSimilarity(tt.v1, tt.v2)

		if (err != nil) != tt.wantErr {
			t.Errorf("CosineSimilarity() error = %v, wantErr %v", err, tt.wantErr)
			return
		}

		if !tt.wantErr && !cmp(got, tt.want) {
			t.Errorf("CosineSimilarity() = %v, want %v", got, tt.want)
		}
	}
}
