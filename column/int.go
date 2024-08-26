package column

import (
	"fmt"
	"math"
	"reflect"
	"strconv"

	"github.com/kcphysics/dataframe/dataframeError"
)

func (c Column) meanInt() (float64, error) {
	sum := 0
	data := c.data.([]int)
	for _, v := range data {
		sum += v
	}
	return float64(sum) / float64(len(data)), nil
}

func (c Column) stdDevInt() (float64, error) {
	mean, err := c.meanInt()
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

// AppendIntFromString takes a string and adds it to the INT type column
func (c *Column) AppendIntFromString(value string) error {
	intVal, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("error parsing %s into an int: %w", value, err)
	}
	c.Append(intVal)
	return nil
}

// Int returns the data inside of the column as an []int, or an error if its not the
// correct column type
func (c Column) Int() ([]int, error) {
	if c.Type != reflect.Int {
		return nil, &dataframeError.WrongColumnTypeError{ColumnName: c.Name, CorrectType: c.Type, CurrentType: reflect.Int}
	}
	rData, ok := c.data.([]int)
	if !ok {
		return nil, fmt.Errorf("unknown error, could not convert column %s to []int", c.Name)
	}
	return rData, nil
}
