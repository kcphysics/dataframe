package dataframe

import (
	"fmt"
	"reflect"
	"testing"
)

func createColumnTestHelper[T Columnable](t *testing.T, typeString string, data []T) *Column[T] {
	columnName := fmt.Sprintf("Test Column For %s Types", typeString)
	columnType := reflect.TypeOf(data).Elem().Kind()
	expectedLength := len(data)
	col, err := NewColumn(columnName, data)
	if err != nil {
		t.Errorf("unable to create new column for %s type: %s", typeString, err)
		return nil
	}
	if col.ColumnName != columnName {
		t.Errorf("expected column name %s but have %s", columnName, col.ColumnName)
	}
	if col.ColumnType != columnType {
		t.Errorf("expected column type %s but have %s", columnType, col.ColumnType)
	}
	if col.Length() != expectedLength {
		t.Errorf("expected length to be %d, but found %d", expectedLength, col.Length())
	}
	dataType := reflect.TypeOf(col.data)
	k := dataType.Elem().Kind()
	if k != columnType {
		t.Errorf("expected elements of internal data structure to have %s, but found %s", typeString, k)
	}
	return col
}

func TestColumnForStrings(t *testing.T) {
	testData := []string{"this", "is", "a", "test", "string"}
	col := createColumnTestHelper(t, "String", testData)
	currentLength := col.Length()
	if col.Length() != len(col.data) {
		t.Errorf("expected %d data entries, but found %d in call to length", len(col.data), col.Length())
	}
	testString := "Something Cool"
	col.AppendValue(testString)
	newLength := col.Length()
	if newLength != currentLength+1 {
		t.Errorf("expected %d data entries after append, but found %d", currentLength+1, newLength)
	}
	ndx, ok := col.GetFirstIndexOfValue(testString)
	if !ok {
		t.Errorf("expected to find '%s' in the column, but could not", testString)
		return
	}
	if ndx != newLength-1 {
		t.Errorf("expected returned ndx to be %d, but found %d", newLength-1, ndx)
	}
	_, ok = col.GetFirstIndexOfValue("Definitely Not a string that already exists")
	if ok {
		t.Errorf("expected not to find bad string in column, but did")
	}

}

func TestCreateColumnForInt(t *testing.T) {
	testData := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 0}
	col := createColumnTestHelper(t, "Int", testData)
	currentLength := col.Length()
	if col.Length() != len(col.data) {
		t.Errorf("expected %d data entries, but found %d in call to length", len(col.data), col.Length())
	}
	testVariable := 3200000
	col.AppendValue(testVariable)
	newLength := col.Length()
	if newLength != currentLength+1 {
		t.Errorf("expected %d data entries after append, but found %d", currentLength+1, newLength)
	}
	ndx, ok := col.GetFirstIndexOfValue(testVariable)
	if !ok {
		t.Errorf("expected to find '%d' in the column, but could not", testVariable)
		return
	}
	if ndx != newLength-1 {
		t.Errorf("expected returned ndx to be %d, but found %d", newLength-1, ndx)
	}
	_, ok = col.GetFirstIndexOfValue(-10000)
	if ok {
		t.Errorf("expected not to find bad string in column, but did")
	}
}

func TestCreateColumnForInt64(t *testing.T) {
	testData := []int64{int64(0), int64(100), 9223372036854775807}
	col := createColumnTestHelper(t, "Int64", testData)
	currentLength := col.Length()
	if col.Length() != len(col.data) {
		t.Errorf("expected %d data entries, but found %d in call to length", len(col.data), col.Length())
	}
	testVariable := int64(3200000)
	col.AppendValue(testVariable)
	newLength := col.Length()
	if newLength != currentLength+1 {
		t.Errorf("expected %d data entries after append, but found %d", currentLength+1, newLength)
	}
	ndx, ok := col.GetFirstIndexOfValue(testVariable)
	if !ok {
		t.Errorf("expected to find '%d' in the column, but could not", testVariable)
		return
	}
	if ndx != newLength-1 {
		t.Errorf("expected returned ndx to be %d, but found %d", newLength-1, ndx)
	}
	_, ok = col.GetFirstIndexOfValue(922337203685477580)
	if ok {
		t.Errorf("expected not to find bad string in column, but did")
	}
}

func TestCreateColumnForFloat(t *testing.T) {
	testData := []float64{1.2, 2.2, 3.14, 3.3, 123.456, 987.65431}
	col := createColumnTestHelper(t, "Float", testData)
	currentLength := col.Length()
	if col.Length() != len(col.data) {
		t.Errorf("expected %d data entries, but found %d in call to length", len(col.data), col.Length())
	}
	testVariable := 963.852741
	col.AppendValue(testVariable)
	newLength := col.Length()
	if newLength != currentLength+1 {
		t.Errorf("expected %d data entries after append, but found %d", currentLength+1, newLength)
	}
	ndx, ok := col.GetFirstIndexOfValue(testVariable)
	if !ok {
		t.Errorf("expected to find '%f' in the column, but could not", testVariable)
		return
	}
	if ndx != newLength-1 {
		t.Errorf("expected returned ndx to be %d, but found %d", newLength-1, ndx)
	}
	_, ok = col.GetFirstIndexOfValue(922337203685477580.0)
	if ok {
		t.Errorf("expected not to find bad string in column, but did")
	}
}

func testSlice[T Columnable](t *testing.T, columnType reflect.Kind, data []T) {
	col, err := NewColumn("TestColumn", data)
	if err != nil {
		t.Errorf("unable to create test column for type %s: %s", columnType, err)
		return
	}
	sliceVar, err := col.GetValueAtIndex(3)
	if err != nil {
		t.Errorf("unable to retrieve index %d for column type %s", 2, columnType)
		return
	}
	slicedColumn, err := col.Filter(LesserEq, sliceVar)
	if err != nil {
		t.Errorf("unable to slice column of type %s on value %v: %s", columnType, sliceVar, err)
		return
	}
	if slicedColumn.Length() != 3 {
		t.Errorf("expected column length to be 3, but got %d", slicedColumn.Length())
	}
	for i := 0; i < 3; i++ {
		comparisonVar, err := slicedColumn.GetValueAtIndex(i)
		if err != nil {
			t.Errorf("unable to retrieve index %d for sliced Column of type %s", i, columnType)
			continue
		}
		if comparisonVar != data[i] {
			t.Errorf("expected variable %v but found %v", data[i], comparisonVar)
		}
		originalVar, err := col.GetValueAtIndex(i)
		if err != nil {
			t.Errorf("unable to retrieve index %d for original column of type %s", i, columnType)
		}
		if originalVar != comparisonVar {
			t.Errorf("expected variable %v but found %v when comparing pre and post Sliced columns", originalVar, comparisonVar)
		}
	}
}

func TestColumnSlicing(t *testing.T) {
	testData := map[reflect.Kind]interface{}{
		reflect.Float64: []float64{1.2, 2.2, 3.14, 3.3, 123.456, 987.65431},
		reflect.Int64:   []int64{int64(0), int64(100), 9223372036854775807, 1234},
		reflect.Int:     []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 0},
		reflect.String:  []string{"this", "is", "a", "test", "string"},
	}
	for kind, data := range testData {
		switch kind {
		case reflect.Float64:
			testSlice(t, kind, data.([]float64))
		case reflect.Int:
			testSlice(t, kind, data.([]int))
		case reflect.Int64:
			testSlice(t, kind, data.([]int64))
		case reflect.String:
			testSlice(t, kind, data.([]string))
		}
	}
}
