package column

import (
	"fmt"
	"reflect"

	"github.com/kcphysics/dataframe/dataframeError"
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

// Column holds the data for a specific series in the dataframe.  This
// is done by storing the data as an interface and then casting when
// it is needed
type Column struct {
	Name string
	Type reflect.Kind
	data interface{}
}

// Length gets the columns length
func (c Column) Length() int {
	dataValue := reflect.ValueOf(c.data)
	return dataValue.Len()
}

// Append takes a value and adds it to the Column
func (c *Column) Append(value interface{}) error {
	err := hasCorrectType(c.Type, value)
	if err != nil {
		return err
	}
	dataValue := reflect.ValueOf(c.data)
	newValue := reflect.Append(dataValue, reflect.ValueOf(value))
	c.data = newValue.Interface()
	return nil
}

// Slice will slice the data and return a new Column with that slice
func (c Column) Slice(start, stop int) *Column {
	dataValue := reflect.ValueOf(c.data)
	newSlice := dataValue.Slice(start, stop)
	return &Column{
		Name: c.Name,
		Type: c.Type,
		data: newSlice.Interface(),
	}
}

// Value takes an index and returns a Value.  To get the actual
// value, you'll need to use Value's type methods
func (c Column) Value(ndx int) (*Value, error) {
	err := c.checkBounds(ndx)
	if err != nil {
		return nil, err
	}
	dataValue := reflect.ValueOf(c.data)
	data := dataValue.Index(ndx)
	return newValue(c.Type, data.Interface()), nil
}

// AppendString takes a string and adds it to the column
func (c *Column) AppendString(value string) error {
	c.Append(value)
	return nil
}

// Mean will return the mean for numeric types or a NotNumericColumn error
// if it is a string type
func (c Column) Mean() (float64, error) {
	if c.Type == reflect.String {
		return -1, &dataframeError.NotNumericColumn{Name: c.Name, Operation: "mean"}
	}
	switch c.Type {
	case reflect.Int:
		return c.meanInt()
	case reflect.Int64:
		return c.meanInt64()
	case reflect.Float64:
		return c.meanFloat()
	default:
		return -1, &dataframeError.UnsupportedType{ColumnType: c.Type}
	}
}

// StdDev will return the standard deviation or a NotNumericColumn error if
// this is a string column or an unsupported type error in case of insanity
func (c Column) StdDev() (float64, error) {
	if c.Type == reflect.String {
		return -1, &dataframeError.NotNumericColumn{Name: c.Name, Operation: "standard deviation"}
	}
	switch c.Type {
	case reflect.Int:
		return c.stdDevInt()
	case reflect.Int64:
		return c.stdDevInt64()
	case reflect.Float64:
		return c.stdDevFloat()
	default:
		return -1, &dataframeError.UnsupportedType{ColumnType: c.Type}
	}
}

func (c Column) checkBounds(ndx int) error {
	if ndx < 0 || ndx > c.Length()-1 {
		return &dataframeError.IndexOutOfBounds{
			ColumnName:  c.Name,
			BrokenIndex: ndx,
			MaxIndex:    c.Length() - 1,
		}
	}
	return nil
}

// New returns a new column based on passed in name, column type, and data
// if you don't have data (the data is nil), an empty array of that type
// will be used
func New(columnType reflect.Kind, columnName string, data interface{}) (*Column, error) {
	switch columnType {
	case reflect.String, reflect.Int, reflect.Int64, reflect.Float64:
	default:
		return nil, &dataframeError.UnsupportedType{ColumnType: columnType}
	}
	var cdata interface{}
	if data == nil {
		switch columnType {
		case reflect.String:
			cdata = []string{}
		case reflect.Int:
			cdata = []int{}
		case reflect.Int64:
			cdata = []int64{}
		case reflect.Float64:
			cdata = []float64{}
		default:
			return nil, &dataframeError.UnsupportedType{ColumnType: columnType}
		}
	} else {
		cdata = data
	}
	err := isSliceWithCorrectType(columnType, cdata)
	if err != nil {
		return nil, err
	}
	return &Column{
		Name: columnName,
		Type: columnType,
		data: cdata,
	}, nil
}

func isSliceWithCorrectType(expectedType reflect.Kind, value interface{}) error {
	dataKind := reflect.TypeOf(value).Kind()
	if dataKind != reflect.Slice {
		return fmt.Errorf("expected slice to be passed in, but found %s", dataKind)
	}
	dataValue := reflect.ValueOf(value)
	if dataValue.Type().Elem().Kind() != expectedType {
		return &dataframeError.WrongType{
			GivenType:    dataValue.Type().Elem().Kind(),
			ExpectedType: expectedType,
		}
	}
	return nil
}

func hasCorrectType(expectedType reflect.Kind, value interface{}) error {
	valueKind := reflect.TypeOf(value).Kind()
	if valueKind != expectedType {
		return &dataframeError.WrongType{
			GivenType:    valueKind,
			ExpectedType: expectedType,
		}
	}
	return nil
}

// AsString returns the data inside of the column as an []string, or an error if its not the
// correct column type
func (c Column) AsString() ([]string, error) {
	if c.Type != reflect.String {
		return nil, &dataframeError.WrongColumnTypeError{ColumnName: c.Name, CorrectType: c.Type, CurrentType: reflect.String}
	}
	rData, ok := c.data.([]string)
	if !ok {
		return nil, fmt.Errorf("unknown error, could not convert column %s to []string", c.Name)
	}
	return rData, nil
}
