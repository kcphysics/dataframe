package columns

import (
	"fmt"
	"math"
)

// IntColumn is a specific implementation of the Column
// interface for Integer type columns
type IntColumn struct {
	Column[int]
}

// Mean calculates the mean for an Integer column
func (i IntColumn) Mean() (float64, error) {
	if i.Length() <= 0 {
		return 0, fmt.Errorf("no values stored, cannot calculate mean")
	}
	sum := 0
	for _, i := range i.data {
		sum += i
	}

	return float64(sum) / float64(i.Length()), nil
}

// StdDev calculates the standard deviation of an integer column
func (i IntColumn) StdDev() (float64, error) {
	if i.Length() <= 0 {
		return 0, fmt.Errorf("no values stored, cannot calculate mean")
	}
	mean, err := i.Mean()
	if err != nil {
		return 0, fmt.Errorf("unable to calculate mean: %w", err)
	}
	sum := float64(0)
	for _, i := range i.data {
		sum += math.Pow((float64(i) - mean), 2)
	}
	variance := sum / float64(i.Length())
	return math.Sqrt(variance), nil
}
