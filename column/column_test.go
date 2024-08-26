package column

import (
	"errors"
	"reflect"
	"testing"

	"github.com/kcphysics/dataframe/dataframeError"
)

type ColumnTestCase struct {
	Type           reflect.Kind
	Name           string
	Data           interface{}
	ExpectedLength int
}

func columnTestHelper(testCase ColumnTestCase, t *testing.T) {
	var newColumn *Column
	var err error
	switch testCase.Type {
	case reflect.String:
		data := testCase.Data.([]string)
		newColumn, err = New(testCase.Type, testCase.Name, data)
	case reflect.Float64:
		data := testCase.Data.([]float64)
		newColumn, err = New(testCase.Type, testCase.Name, data)
	case reflect.Int64:
		data := testCase.Data.([]int64)
		newColumn, err = New(testCase.Type, testCase.Name, data)
	case reflect.Int:
		data := testCase.Data.([]int)
		newColumn, err = New(testCase.Type, testCase.Name, data)
	default:
		t.Fatalf("unexpected type %s passed into test helper", testCase.Type)
	}
	if err != nil {
		t.Fatalf("unexpected error during column creation: %s", err)
	}
	if newColumn.Length() != testCase.ExpectedLength {
		t.Fatalf("expected %d entries, but found %d", testCase.ExpectedLength, newColumn.Length())
	}
}

func TestColumnCreation(t *testing.T) {
	testCases := []ColumnTestCase{
		{reflect.String, "StringColumn", []string{"this", "is", "a", "string"}, 4},
		{reflect.Int, "IntColumn", []int{1, 2, 3, 4}, 4},
		{reflect.Int64, "BigIntColumn", []int64{1, 2, 3, 4}, 4},
		{reflect.Float64, "FloatColumn", []float64{1.1, 2.2, 3.3, 4.4}, 4},
	}
	for _, tc := range testCases {
		columnTestHelper(tc, t)
	}
}

func TestUnsupportedColumnType(t *testing.T) {
	// Now we test to make sure we get appropriate errors when we don't create things correctly
	_, err := New(reflect.Float32, "BadColumn", []float32{1, 2, 3, 4})
	if err == nil {
		t.Fatal("expected error while creating float32 column, but found none")
	}
	var expectedErrorType *dataframeError.UnsupportedType
	if !errors.As(err, &expectedErrorType) {
		t.Fatalf("expected unsupported type error, but found: %s", err)
	}
}

func TestColumnMismatchType(t *testing.T) {
	_, err := New(reflect.String, "BadColumn", []float64{1, 2, 3, 4})
	if err == nil {
		t.Fatal("expected error during column creation with type mismatch, but found none")
	}
	var expectedErrorType *dataframeError.WrongType
	if !errors.As(err, &expectedErrorType) {
		t.Fatalf("expected wrong column type, but found: %s", err)
	}
}

func TestColumnMismatchTypeOnAppend(t *testing.T) {
	newColumn, err := New(reflect.String, "String Column", []string{"this", "is", "a", "test"})
	if err != nil {
		t.Fatalf("unexpected error during column creation for string: %s", err)
	}
	err = newColumn.Append(3.14)
	if err == nil {
		t.Fatal("expected error while appending float data to string column, but found none")
	}
	var expectedErrorType *dataframeError.WrongType
	if !errors.As(err, &expectedErrorType) {
		t.Fatalf("expected wrong column type error, but found: %s", err)
	}
}
