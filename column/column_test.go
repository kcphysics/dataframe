package column

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/kcphysics/dataframe/dataframeError"
)

type ColumnTestCase struct {
	Type           reflect.Kind
	Name           string
	Data           interface{}
	ExpectedLength int
}

func columnTestHelper(testCase ColumnTestCase, t *testing.T) *Column {
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
	return newColumn
}

func TestColumnCreation(t *testing.T) {
	testCases := []ColumnTestCase{
		{reflect.String, "StringColumn", []string{"this", "is", "a", "string"}, 4},
		{reflect.Int, "IntColumn", []int{1, 2, 3, 4}, 4},
		{reflect.Int64, "BigIntColumn", []int64{1, 2, 3, 4}, 4},
		{reflect.Float64, "FloatColumn", []float64{1.1, 2.2, 3.3, 4.4}, 4},
	}
	for _, tc := range testCases {
		_ = columnTestHelper(tc, t)
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

func TestColumnIndices(t *testing.T) {
	testCases := []ColumnTestCase{
		{reflect.String, "StringColumn", []string{"this", "is", "a", "string"}, 4},
		{reflect.Int, "IntColumn", []int{1, 2, 3, 4}, 4},
		{reflect.Int64, "BigIntColumn", []int64{1, 2, 3, 4}, 4},
		{reflect.Float64, "FloatColumn", []float64{1.1, 2.2, 3.3, 4.4}, 4},
	}
	for _, tc := range testCases {
		column := columnTestHelper(tc, t)
		newColumn, err := column.Indices([]int{1, 3})
		if err != nil {
			t.Fatalf("unable to get indices 1 and 3 for column %s of type %s", column.Name, column.Type)
		}
		if newColumn.Length() != 2 {
			t.Errorf("expected length of 2 but found %d", newColumn.Length())
		}
	}
}

// TestColumnConcat will test to make sure that columns with the same name and kind
// can be combined
func TestColumnConcat(t *testing.T) {
	testCases1 := []ColumnTestCase{
		{reflect.String, "StringColumn", []string{"this", "is", "a", "string"}, 4},
		{reflect.Int, "IntColumn", []int{1, 2, 3, 4}, 4},
		{reflect.Int64, "BigIntColumn", []int64{1, 2, 3, 4}, 4},
		{reflect.Float64, "FloatColumn", []float64{1.1, 2.2, 3.3, 4.4}, 4},
	}
	testCases2 := []ColumnTestCase{
		{reflect.String, "StringColumn", []string{"one", "is", "super", "lonely"}, 4},
		{reflect.Int, "IntColumn", []int{9, 8, 7, 6}, 4},
		{reflect.Int64, "BigIntColumn", []int64{1, 2, 3, 4}, 4},
		{reflect.Float64, "FloatColumn", []float64{1.1, 2.2, 3.3, 4.4}, 4},
	}
	badCase := ColumnTestCase{Type: reflect.String, Name: "NotAnExistingColumnOhMyWhatShallWeDo", Data: []string{"Oi"}, ExpectedLength: 1}
	for i := 0; i < len(testCases1); i++ {
		tc1 := testCases1[i]
		col1 := columnTestHelper(tc1, t)
		tc2 := testCases2[i]
		col2 := columnTestHelper(tc2, t)
		err := col1.Concatenate(col2)
		if err != nil {
			t.Fatalf("expected no error while concatenating good columns")
		}
		if col1.Length() != 8 {
			t.Errorf("expected 8 rows in first column (changed column), but found %d", col1.Length())
		}
		if col2.Length() != 4 {
			t.Errorf("expected 4 rows in second column (unchanged column), but found %d", col2.Length())
		}
		col3 := columnTestHelper(badCase, t)
		err = col1.Concatenate(col3)
		if err == nil {
			t.Fatalf("expected error when concatenating bad column, but found none")
		}
		if col1.Type == reflect.String {
			if !strings.Contains(err.Error(), "NotAnExistingColumnOhMyWhatShallWeDo") {
				t.Errorf("expected error that contains 'NotAnExistingColumnOhMyWhatShallWeDo' as that is the asymmetric column, but found %s", err)
			}
		} else {
			var expectedErrorType *dataframeError.WrongColumnTypeError
			if !errors.As(err, &expectedErrorType) {
				t.Fatalf("expected wrong column type, but found: %s", err)
			}
		}
	}
}
