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

func prepareTestDF(t *testing.T, testCases []DFTestCase) *Dataframe {
	df := New()
	for _, tc := range testCases {
		column, err := column.New(tc.Type, tc.Name, tc.Data)
		if err != nil {
			t.Fatalf("unable to create column %s of type %s: %s", tc.Name, tc.Type, err)
		}
		err = df.AddColumn(column)
		if err != nil {
			t.Fatalf("unable to add column %s of type %s to dataframe: %s", tc.Name, tc.Type, err)
		}
	}
	type TestStruct struct {
		StringColumn  string
		IntColumn     int
		BigIntColumn  int64
		FloatColumn   float64
		NotARealThing int
	}
	var testStruct TestStruct
	err := df.MapStruct(&testStruct, 1)
	if err != nil {
		t.Fatalf("unexpected error while mapping dataframe record to struct: %s", err)
	}
	if testStruct.StringColumn != "is" {
		t.Errorf("expected 'is' but found %s", testStruct.StringColumn)
	}
	if testStruct.IntColumn != 2 {
		t.Errorf("expected 2 but found %d", testStruct.IntColumn)
	}
	if testStruct.BigIntColumn != int64(2) {
		t.Errorf("expected 2 but found %d", testStruct.BigIntColumn)
	}
	if testStruct.FloatColumn != 2.2 {
		t.Errorf("expected 2.2 but found %f", testStruct.FloatColumn)
	}
	if testStruct.NotARealThing != 0 {
		t.Errorf("expected 'NotARealThing' to be 0, but found %d", testStruct.NotARealThing)
	}
	return df
}

func TestDataframeToStruct(t *testing.T) {
	testCases := []DFTestCase{
		{reflect.String, "StringColumn", []string{"this", "is", "a", "string"}, 4},
		{reflect.Int, "IntColumn", []int{1, 2, 3, 4}, 4},
		{reflect.Int64, "BigIntColumn", []int64{1, 2, 3, 4}, 4},
		{reflect.Float64, "FloatColumn", []float64{1.1, 2.2, 3.3, 4.4}, 4},
	}
	df := prepareTestDF(t, testCases)
	if df.Length() != 4 {
		t.Errorf("expected length of 4 but found %d", df.Length())
	}
}

func TestDataframeSelectIndices(t *testing.T) {
	testCases := []DFTestCase{
		{reflect.String, "StringColumn", []string{"this", "is", "a", "string"}, 4},
		{reflect.Int, "IntColumn", []int{1, 2, 3, 4}, 4},
		{reflect.Int64, "BigIntColumn", []int64{1, 2, 3, 4}, 4},
		{reflect.Float64, "FloatColumn", []float64{1.1, 2.2, 3.3, 4.4}, 4},
	}
	df := prepareTestDF(t, testCases)
	newDf, err := df.SelectForIndices(
		[]string{"StringColumn", "FloatColumn"},
		[]int{1, 3},
	)
	if err != nil {
		t.Fatalf("unexpected error during SelectForIndices operation: %s", err)
	}
	if newDf.Length() != 2 {
		t.Fatalf("expected new dataframe to have length 2, but found %d", newDf.Length())
	}
	stringColumn, err := newDf.Column("StringColumn")
	if err != nil {
		t.Fatalf("unexpected error while fetching StringColumn: %s", err)
	}
	stringData, err := stringColumn.AsString()
	if err != nil {
		t.Fatalf("unexpected error while getting raw string data: %s", err)
	}
	if stringData[1] != "string" {
		t.Fatalf("expected value 'string', but found %s", stringData[1])
	}
}

func TestDataFrameConcatenation(t *testing.T) {
	testCases1 := []DFTestCase{
		{reflect.String, "StringColumn", []string{"this", "is", "a", "string"}, 4},
		{reflect.Int, "IntColumn", []int{1, 2, 3, 4}, 4},
		{reflect.Int64, "BigIntColumn", []int64{1, 2, 3, 4}, 4},
		{reflect.Float64, "FloatColumn", []float64{1.1, 2.2, 3.3, 4.4}, 4},
	}
	df1 := prepareTestDF(t, testCases1)
	testCases2 := []DFTestCase{
		{reflect.String, "StringColumn", []string{"this", "is", "a", "string"}, 4},
		{reflect.Int, "IntColumn", []int{1, 2, 3, 4}, 4},
		{reflect.Int64, "BigIntColumn", []int64{1, 2, 3, 4}, 4},
		{reflect.Float64, "FloatColumn", []float64{1.1, 2.2, 3.3, 4.4}, 4},
	}
	df2 := prepareTestDF(t, testCases2)
	err := df1.Concatenate(df2)
	if err != nil {
		t.Fatalf("unexpected error during dataframe concatenation: %s", err)
	}
	if df1.Length() != 8 {
		t.Errorf("expected length of 8 for df1, but found %d", df1.Length())
	}
	if df2.Length() != 4 {
		t.Errorf("expected length of 4 for df2, but found %d", df2.Length())
	}
	testCases3 := []DFTestCase{
		{reflect.String, "NewColumn", []string{"oh", "no", "poor", "joe", "hasnoarms"}, 5},
		{reflect.Float64, "NotACorrectColumn", []float64{1.1, 2.2, 3.3, 4.4, 5.5}, 5},
	}
	notT := testing.T{}
	df3 := prepareTestDF(&notT, testCases3)
	err = df1.Concatenate(df3)
	if err == nil {
		t.Fatalf("expected error due to mismatch of columns and rows, but found none")
	}
	var expectedErrorType *dataframeError.MissingColumnError
	if !errors.As(err, &expectedErrorType) {
		t.Fatalf("expected row count mismatch error, but found %s", err)
	}
}
