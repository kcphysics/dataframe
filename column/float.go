package column

import (
	"fmt"
	"math"
	"strconv"
)

func (c Column) meanFloat() (float64, error) {
	sum := float64(0)
	data := c.data.([]float64)
	for _, v := range data {
		sum += v
	}
	return float64(sum) / float64(len(data)), nil
}

func (c Column) stdDevFloat() (float64, error) {
	mean, err := c.meanFloat()
	if err != nil {
		return -1, err
	}
	sum := float64(0)
	data := c.data.([]int)
	for _, v := range data {
		sum += (float64(v) - mean)
	}
	variance := sum / float64(len(data))
	return math.Sqrt(variance), nil
}

// AppendFloatFromString takes a string and appends it to a Float64 type column
func (c *Column) AppendFloatFromString(value string) error {
	val, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fmt.Errorf("error parsing %s to float64: %w", value, err)
	}
	c.Append(val)
	return nil
}
