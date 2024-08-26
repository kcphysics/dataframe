package column

import (
	"fmt"
	"math"
	"reflect"
	"strconv"

	"github.com/kcphysics/dataframe/dataframeError"
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

// Float returns the data inside of the column as an []float64, or an error if its not the
// correct column type
func (c Column) Float() ([]float64, error) {
	if c.Type != reflect.Float64 {
		return nil, &dataframeError.WrongColumnTypeError{ColumnName: c.Name, CorrectType: c.Type, CurrentType: reflect.Float64}
	}
	rData, ok := c.data.([]float64)
	if !ok {
		return nil, fmt.Errorf("unknown error, could not convert column %s to []float64", c.Name)
	}
	return rData, nil
}
