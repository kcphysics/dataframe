package columns

import (
	"fmt"
	"math"
)

// BigIntColumn is a specific implementation of the
// Column interface for Big Ints
type BigIntColumn struct {
	Column[int64]
}

// Mean calculates the mean for an BigInt (Int64) column
func (bi BigIntColumn) Mean() (float64, error) {
	if bi.Length() <= 0 {
		return 0, fmt.Errorf("no values stored, cannot calculate mean")
	}
	sum := int64(0)
	for _, i := range bi.data {
		sum += i
	}

	return float64(sum) / float64(bi.Length()), nil
}

// StdDev calculates the standard deviation of an BigInt (Int64) column
func (bi BigIntColumn) StdDev() (float64, error) {
	if bi.Length() <= 0 {
		return 0, fmt.Errorf("no values stored, cannot calculate mean")
	}
	mean, err := bi.Mean()
	if err != nil {
		return 0, fmt.Errorf("unable to calculate mean: %w", err)
	}
	sum := float64(0)
	for _, i := range bi.data {
		sum += math.Pow((float64(i) - mean), 2)
	}
	variance := sum / float64(bi.Length())
	return math.Sqrt(variance), nil
}
