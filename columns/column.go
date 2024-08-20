package columns

import (
	"fmt"
	"reflect"

	"github.com/kcphysics/dataframe/errors"
)

// FilterType allows for slicing based off of values
type FilterType string

const (
	Equal     FilterType = "Equal"
	Greater   FilterType = "Greater"
	GreaterEq FilterType = "GreaterEq"
	Lesser    FilterType = "Lesser"
	LesserEq  FilterType = "LesserEq"
)

// Columnable represents the allowed column types
type Columnable interface {
	string | float64 | int | int64
}

// ColumnInterface is an interface that expresses what
// a column can do and can be used for
type ColumnInterface interface {
	ColumnType() reflect.Kind
	ColumnName() string
	Length() int
	Slice(int, int) (*ColumnInterface, error)
	GetValueAtIndex(int) (interface{}, error)
	GetFirstIndexOfValue(interface{}) (int, bool, error)
	Filter(FilterType, interface{}) (*ColumnInterface, error)
	AppendValue(interface{})
}

// Column is a structure that holds data for a dataframe
// the goal is to leave all slicing for each column
// up to the column
type Column[T Columnable] struct {
	ColumnName string
	ColumnType reflect.Kind
	data       []T
}

// GetValueAtIndex will fetch the value for this column
// at the ndx provided.  If the index is out of bounds, it
// will return an IndexOutOfBounds error
func (c Column[T]) GetValueAtIndex(ndx int) (T, error) {
	if ndx > c.Length()-1 || ndx >= c.Length() {
		return *new(T), errors.IndexOutOfBounds{ColumnName: c.ColumnName, BrokenIndex: ndx, MaxIndex: c.Length()}
	}
	return c.data[ndx], nil
}

// GetFirstIndexOfValue will return the first index that has
// the value specified.  It returns the index (an integer) and
// a bool for a comma ok syntax
func (c *Column[T]) GetFirstIndexOfValue(value T) (int, bool) {
	for ndx, v := range c.data {
		if v == value {
			return ndx, true
		}
	}
	return -1, false
}

// AppendValue will append the value provided to the column
func (c *Column[T]) AppendValue(val T) {
	c.data = append(c.data, val)
}

// Length will return an integer representing the number of entries
// in this column
func (c Column[T]) Length() int {
	return len(c.data)
}

// Slice takes a start and stop parameter and returns a new
// column of the same type and name or an error
func (c Column[T]) Slice(start, stop int) (*Column[T], error) {
	newData := c.data[start:stop]
	return NewColumn(c.ColumnName, newData)
}

// Filter will take an operation and a value.  This works by searching
// for the first appearance of the value, and the applying the operation
// and returning a new column with the same name and type.  If it cannot
// find the value, it will return a nil, error
func (c Column[T]) Filter(operation FilterType, value T) (*Column[T], error) {
	ndx, ok := c.GetFirstIndexOfValue(value)
	if !ok {
		return nil, fmt.Errorf("unable to find value %v in column", value)
	}
	start, stop := getBoundsForFilter(operation, ndx, c.Length())
	return c.Slice(start, stop)
}

// NewColumn will create a new column from existing data.  If you don't
// have existing data, just declare the empty slice.  For instance, if
// you wanted to create a string Column, use []string{} as the data
// argument
func NewColumn[T Columnable](colName string, data []T) (*Column[T], error) {
	tType := reflect.TypeOf(data)
	return &Column[T]{
		ColumnName: colName,
		ColumnType: tType.Elem().Kind(),
		data:       data,
	}, nil
}
