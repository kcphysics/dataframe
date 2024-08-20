package columns

import (
	"fmt"
	"math"
)

// FloatColumn is a specific implementation of the Column
// interface for Float64 type columns
type FloatColumn struct {
	Column[float64]
}

// Mean calculates the mean for an Float64 column
func (fc FloatColumn) Mean() (float64, error) {
	if fc.Length() <= 0 {
		return 0, fmt.Errorf("no values stored, cannot calculate mean")
	}
	sum := float64(0)
	for _, i := range fc.data {
		sum += i
	}

	return float64(sum) / float64(fc.Length()), nil
}

// StdDev calculates the standard deviation of an Float64 column
func (fc FloatColumn) StdDev() (float64, error) {
	if fc.Length() <= 0 {
		return 0, fmt.Errorf("no values stored, cannot calculate mean")
	}
	mean, err := fc.Mean()
	if err != nil {
		return 0, fmt.Errorf("unable to calculate mean: %w", err)
	}
	sum := float64(0)
	for _, i := range fc.data {
		sum += math.Pow((float64(i) - mean), 2)
	}
	variance := sum / float64(fc.Length())
	return math.Sqrt(variance), nil
}
