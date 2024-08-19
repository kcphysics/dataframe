package dataframe

import (
	"fmt"
	"os"
	"path"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
)

const (
	testCSV = `ticker,volume,open,close,high,low,window_start,transactions
DFRAME,171463,17.740000,17.675000,17.810000,17.675000,1638334800000000000,452
DFRAME,278397,17.650000,17.580000,17.655000,17.515000,1638421200000000000,914
DFRAME,151971,17.620000,17.720000,17.755000,17.570000,1638507600000000000,535
DFRAME,182507,17.670000,17.680000,17.740000,17.660000,1638766800000000000,571
DFRAME,160450,17.680000,17.740000,17.770000,17.680000,1638853200000000000,563
DFRAME,132764,17.720000,17.740000,17.760000,17.700000,1638939600000000000,431
DFRAME,141119,17.710000,17.650000,17.720000,17.630000,1639026000000000000,337`
)

var (
	testFileSchemaDefs = []SchemaDef{
		{"Symbol", reflect.String},
		{"Volume", reflect.Int},
		{"Open", reflect.Float64},
		{"Close", reflect.Float64},
		{"High", reflect.Float64},
		{"Low", reflect.Float64},
		{"WindowStart", reflect.Int64},
		{"Transactions", reflect.Int64},
	}
)

func createTestCSV(name string) (string, string, error) {
	tempDir, err := os.MkdirTemp("", "fromcsvtest")
	if err != nil {
		return "", "", err
	}
	filename := path.Join(tempDir, name)
	f, err := os.Create(filename)
	if err != nil {
		return "", "", fmt.Errorf("unable to open file %s: %w", filename, err)
	}
	defer f.Close()
	_, err = f.WriteString(testCSV)
	if err != nil {
		return "", "", fmt.Errorf("unable to write test csv: %w", err)
	}
	return tempDir, filename, nil
}

func TestFromCsvWithHeader(t *testing.T) {
	pc, _, _, ok := runtime.Caller(1)
	if !ok {
		t.Fatal("unable to get function name, something is wrong")
		return
	}
	fn := runtime.FuncForPC(pc)
	if fn == nil {
		t.Fatal("unable to get function name from pc, something is wrong")
		return
	}
	baseName := fmt.Sprintf("%s.csv", fn.Name())
	tempDir, testFileName, err := createTestCSV(baseName)
	if err != nil {
		t.Errorf("unable to generate test data: %s", err)
		return
	}
	defer os.RemoveAll(tempDir)
	schema, err := SchemaFromDefs(testFileSchemaDefs)
	if err != nil {
		t.Fatalf("unable to create test schema: %s", err)
		return
	}
	df, err := FromCSV(testFileName, *schema, true)
	if err != nil {
		t.Fatalf("unable to create csv from file: %s", err)
		return
	}
	if df.Length() != 7 {
		t.Errorf("expected 7 entries, but found %d", df.Length())
		return
	}
	for _, def := range testFileSchemaDefs {
		columnName := def.ColumnName
		columnType := def.ColumnType
		if !slices.Contains(df.Names(), columnName) {
			t.Errorf("expected to find column %s but could not in these columns %s", columnName, strings.Join(df.Names(), ", "))
		}
		actualColumnType, err := df.GetColumnType(columnName)
		if err != nil {
			t.Errorf("unable to get column type for column %s: %s", columnName, err)
		}
		if actualColumnType != columnType {
			t.Errorf("expected column type of %s for column %s but found %s", columnType, columnName, actualColumnType)
		}
	}
	// Now we are going to take the 3rd row and test to make sure its correct (index 2)
	testStringHelper(t, "Symbol", 2, "DFRAME", df)
	testIntHelper(t, "Volume", 2, 151971, df)
	testFloatHelper(t, "Open", 2, 17.620000, df)
	testFloatHelper(t, "Close", 2, 17.720000, df)
	testFloatHelper(t, "High", 2, 17.755000, df)
	testFloatHelper(t, "Low", 2, 17.570000, df)
	testBigIntHelper(t, "WindowStart", 2, 1638507600000000000, df)
	testBigIntHelper(t, "Transactions", 2, 535, df)

}

func testStringHelper(t *testing.T, columnName string, ndx int, expectedValue string, df *Dataframe) {
	actualValue, err := df.GetStringValue(columnName, ndx)
	if err != nil {
		t.Fatalf("unable to get index %d from column %s", ndx, columnName)
		return
	}
	if actualValue != expectedValue {
		t.Errorf("expected %s but found %s from index %d for column %s", expectedValue, actualValue, ndx, columnName)
	}
}

func testIntHelper(t *testing.T, columnName string, ndx int, expectedValue int, df *Dataframe) {
	actualValue, err := df.GetIntValue(columnName, ndx)
	if err != nil {
		t.Fatalf("unable to get index %d from column %s", ndx, columnName)
		return
	}
	if actualValue != expectedValue {
		t.Errorf("expected %d but found %d from index %d for column %s", expectedValue, actualValue, ndx, columnName)
	}
}

func testBigIntHelper(t *testing.T, columnName string, ndx int, expectedValue int64, df *Dataframe) {
	actualValue, err := df.GetBigIntValue(columnName, ndx)
	if err != nil {
		t.Fatalf("unable to get index %d from column %s", ndx, columnName)
		return
	}
	if actualValue != expectedValue {
		t.Errorf("expected %d but found %d from index %d for column %s", expectedValue, actualValue, ndx, columnName)
	}
}

func testFloatHelper(t *testing.T, columnName string, ndx int, expectedValue float64, df *Dataframe) {
	actualValue, err := df.GetFloatValue(columnName, ndx)
	if err != nil {
		t.Fatalf("unable to get index %d from column %s", ndx, columnName)
		return
	}
	if actualValue != expectedValue {
		t.Errorf("expected %f but found %f from index %d for column %s", expectedValue, actualValue, ndx, columnName)
	}
}
