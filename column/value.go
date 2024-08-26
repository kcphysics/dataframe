package column

import (
	"reflect"

	"github.com/kcphysics/dataframe/dataframeError"
)

// Value is a holder that contains data that will need to be
// cast via one of the type methods
type Value struct {
	value interface{}
	vType reflect.Kind
}

// AsString will give you the string held in value or an error
// if its not the right type
func (v Value) AsString() (string, error) {
	returnValue, ok := v.value.(string)
	if !ok {
		return "", dataframeError.WrongType{GivenType: reflect.String, ExpectedType: v.vType}
	}
	return returnValue, nil
}

// Float will give you the float64 held in the value or an error
func (v Value) Float() (float64, error) {
	returnValue, ok := v.value.(float64)
	if !ok {
		return -1, dataframeError.WrongType{GivenType: reflect.Float64, ExpectedType: v.vType}
	}
	return returnValue, nil
}

// Int will give you the int held in the value or an error
func (v Value) Int() (int, error) {
	returnValue, ok := v.value.(int)
	if !ok {
		return -1, dataframeError.WrongType{GivenType: reflect.String, ExpectedType: v.vType}
	}
	return returnValue, nil
}

// Int64 will give you the int64 in the value or an error
func (v Value) Int64() (int64, error) {
	returnValue, ok := v.value.(int64)
	if !ok {
		return -1, dataframeError.WrongType{GivenType: reflect.String, ExpectedType: v.vType}
	}
	return returnValue, nil
}

// Interface returns the value as an interface
func (v Value) Interface() interface{} {
	return v.value
}

func newValue(valueType reflect.Kind, value interface{}) *Value {
	return &Value{
		vType: valueType,
		value: value,
	}
}
