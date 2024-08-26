package dataframe

import (
	"errors"
	"reflect"
	"testing"

	"github.com/kcphysics/dataframe/column"
	"github.com/kcphysics/dataframe/dataframeError"
)

type DFTestCase struct {
	Type           reflect.Kind
	Name           string
	Data           interface{}
	ExpectedLength int
}

func DataframeTestHelper(testCase DFTestCase, t *testing.T) {
	var newColumn *column.Column
	var err error
	switch testCase.Type {
	case reflect.String:
		data := testCase.Data.([]string)
		newColumn, err = column.New(testCase.Type, testCase.Name, data)
	case reflect.Float64:
		data := testCase.Data.([]float64)
		newColumn, err = column.New(testCase.Type, testCase.Name, data)
	case reflect.Int64:
		data := testCase.Data.([]int64)
		newColumn, err = column.New(testCase.Type, testCase.Name, data)
	case reflect.Int:
		data := testCase.Data.([]int)
		newColumn, err = column.New(testCase.Type, testCase.Name, data)
	default:
		t.Fatalf("unsupported kind %s passed in", testCase.Type)
	}
	if err != nil {
		t.Fatalf("unexpected error while making column of type %s: %s", testCase.Type, err)
	}
	df := New()
	if df.Length() != 0 {
		t.Fatalf("expected new dataframe to have length 0, but found %d", df.Length())
	}
	err = df.AddColumn(newColumn)
	if err != nil {
		t.Fatalf("unexpected error while adding column of type %s: %s", testCase.Type, err)
	}
	if df.Length() != testCase.ExpectedLength {
		t.Fatalf("expected df to have length %d but found %d", testCase.ExpectedLength, df.Length())
	}
	badColumn, err := column.New(reflect.Int, "Bad Column", []int{1})
	if err != nil {
		t.Fatalf("unexpected error during column creation: %s", err)
	}
	err = df.AddColumn(badColumn)
	if err == nil {
		t.Fatal("expected column mismatch error, but found none")
	}
	var expectedErrorType *dataframeError.RowCountMismatchError
	if !errors.As(err, &expectedErrorType) {
		t.Fatalf("expected row count mismatch error, but found %s", err)
	}
}

func TestNewDataframe(t *testing.T) {
	testCases := []DFTestCase{
		{reflect.String, "StringColumn", []string{"this", "is", "a", "string"}, 4},
		{reflect.Int, "IntColumn", []int{1, 2, 3, 4}, 4},
		{reflect.Int64, "BigIntColumn", []int64{1, 2, 3, 4}, 4},
		{reflect.Float64, "FloatColumn", []float64{1.1, 2.2, 3.3, 4.4}, 4},
	}
	for _, tc := range testCases {
		DataframeTestHelper(tc, t)
	}
}
