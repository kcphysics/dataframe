package column

import (
	"fmt"
	"math"
	"reflect"
	"strconv"

	"github.com/kcphysics/dataframe/dataframeError"
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
	data := c.data.([]int64)
	for _, v := range data {
		sum += math.Pow((float64(v) - mean), 2)
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

// BigInt returns the data inside of the column as an []int64, or an error if its not the
// correct column type
func (c Column) BigInt() ([]int64, error) {
	if c.Type != reflect.Int64 {
		return nil, &dataframeError.WrongColumnTypeError{ColumnName: c.Name, CorrectType: c.Type, CurrentType: reflect.Int64}
	}
	rData, ok := c.data.([]int64)
	if !ok {
		return nil, fmt.Errorf("unknown error, could not convert column %s to []int64", c.Name)
	}
	return rData, nil
}

func (c Column) bigIntIndices(indices []int) (*Column, error) {
	newData := []int64{}
	data := c.data.([]int64)
	for _, ndx := range indices {
		err := c.checkBounds(ndx)
		if err != nil {
			return nil, err
		}
		newData = append(newData, data[ndx])
	}
	return &Column{
		Name: c.Name,
		Type: c.Type,
		data: newData,
	}, nil
}
