package vec

import (
	"errors"
	"math"
)

type Vector []float32

var ErrDimensionMismatch = errors.New("vector dimensions don't match")
var DivisionByZero = errors.New("attempt to divide by zero")

// DotProduct calculates the dot product of two vectors.
func DotProduct(v1, v2 Vector) (float32, error) {
	if len(v1) != len(v2) {
		return 0, ErrDimensionMismatch
	}

	var result float32

	// scope for Go compiler to produce SIMD instructions.
	for i := range v1 {
		result += v1[i] * v2[i]
	}

	return result, nil
}

// Magnitude calculates the Euclidean Lenght (L2 norm) of the vector.
func Magnitude(v Vector) float32 {
	var sum float32

	for _, val := range v {
		sum += val * val
	}

	return float32(math.Sqrt(float64(sum)))
}

// cosineSimilarity calculates the cosine of angle between two vectors.
// the result ranges from -1  to 1
func CosineSimilarity(v1, v2 Vector) (float32, error) {
	dot, err := DotProduct(v1, v2)

	if err != nil {
		return 0, ErrDimensionMismatch
	}

	mag1 := Magnitude(v1)
	mag2 := Magnitude(v2)

	if mag1 == 0 || mag2 == 0 {
		return 0, DivisionByZero
	}

	return dot / (mag1 * mag2), nil

}
