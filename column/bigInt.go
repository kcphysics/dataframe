package column

import (
	"fmt"
	"math"
	"strconv"
)

func (c Column) meanInt64() (float64, error) {
	sum := int64(0)
	data := c.data.([]int64)
	for _, v := range data {
		sum += v
	}
	return float64(sum) / float64(len(data)), nil
}

func (c Column) stdDevInt64() (float64, error) {
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

// AppendInt64FromString takes a string and appends it as an int64 to an int64 type column
func (c *Column) AppendInt64FromString(value string) error {
	val, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fmt.Errorf("error parsing value %s to int64: %w", value, err)
	}
	c.Append(val)
	return nil
}
