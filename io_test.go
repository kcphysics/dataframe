package dataframe

// import (
// 	"encoding/csv"
// 	"fmt"
// 	"os"
// 	"path"
// 	"reflect"
// 	"runtime"
// 	"slices"
// 	"strings"
// 	"testing"
// )

// const (
// 	testCSV = `ticker,volume,open,close,high,low,window_start,transactions
// DFRAME,171463,17.74,17.675,17.81,17.675000,16383348000,452
// DFRAME,278397,17.65,17.58,17.655000,17.515000,16384212000,914
// DFRAME,151971,17.62,17.72,17.755000,17.57,16385076000,535
// DFRAME,182507,17.67,17.68,17.74,17.66,16387668000,571
// DFRAME,160450,17.68,17.74,17.77,17.68,16388532000,563
// DFRAME,132764,17.72,17.74,17.76,17.70,16389396000,431
// DFRAME,141119,17.71,17.65,17.72,17.63,1639026,337`
// )

// var (
// 	testFileSchemaDefs = []SchemaDef{
// 		{"Symbol", reflect.String},
// 		{"Volume", reflect.Int},
// 		{"Open", reflect.Float64},
// 		{"Close", reflect.Float64},
// 		{"High", reflect.Float64},
// 		{"Low", reflect.Float64},
// 		{"WindowStart", reflect.Int64},
// 		{"Transactions", reflect.Int64},
// 	}
// )

// func createTestCSV(name string) (string, string, error) {
// 	tempDir, err := os.MkdirTemp("", "fromcsvtest")
// 	if err != nil {
// 		return "", "", err
// 	}
// 	filename := path.Join(tempDir, name)
// 	f, err := os.Create(filename)
// 	if err != nil {
// 		return "", "", fmt.Errorf("unable to open file %s: %w", filename, err)
// 	}
// 	defer f.Close()
// 	_, err = f.WriteString(testCSV)
// 	if err != nil {
// 		return "", "", fmt.Errorf("unable to write test csv: %w", err)
// 	}
// 	return tempDir, filename, nil
// }

// func testStringHelper(t *testing.T, columnName string, ndx int, expectedValue string, df *Dataframe) {
// 	actualValue, err := df.GetStringValue(columnName, ndx)
// 	if err != nil {
// 		t.Fatalf("unable to get index %d from column %s", ndx, columnName)
// 		return
// 	}
// 	if actualValue != expectedValue {
// 		t.Errorf("expected %s but found %s from index %d for column %s", expectedValue, actualValue, ndx, columnName)
// 	}
// }

// func testIntHelper(t *testing.T, columnName string, ndx int, expectedValue int, df *Dataframe) {
// 	actualValue, err := df.GetIntValue(columnName, ndx)
// 	if err != nil {
// 		t.Fatalf("unable to get index %d from column %s", ndx, columnName)
// 		return
// 	}
// 	if actualValue != expectedValue {
// 		t.Errorf("expected %d but found %d from index %d for column %s", expectedValue, actualValue, ndx, columnName)
// 	}
// }

// func testBigIntHelper(t *testing.T, columnName string, ndx int, expectedValue int64, df *Dataframe) {
// 	actualValue, err := df.GetBigIntValue(columnName, ndx)
// 	if err != nil {
// 		t.Fatalf("unable to get index %d from column %s", ndx, columnName)
// 		return
// 	}
// 	if actualValue != expectedValue {
// 		t.Errorf("expected %d but found %d from index %d for column %s", expectedValue, actualValue, ndx, columnName)
// 	}
// }

// func testFloatHelper(t *testing.T, columnName string, ndx int, expectedValue float64, df *Dataframe) {
// 	actualValue, err := df.GetFloatValue(columnName, ndx)
// 	if err != nil {
// 		t.Fatalf("unable to get index %d from column %s", ndx, columnName)
// 		return
// 	}
// 	if actualValue != expectedValue {
// 		t.Errorf("expected %f but found %f from index %d for column %s", expectedValue, actualValue, ndx, columnName)
// 	}
// }

// func TestFromCsvWithHeader(t *testing.T) {
// 	pc, _, _, ok := runtime.Caller(1)
// 	if !ok {
// 		t.Fatal("unable to get function name, something is wrong")
// 		return
// 	}
// 	fn := runtime.FuncForPC(pc)
// 	if fn == nil {
// 		t.Fatal("unable to get function name from pc, something is wrong")
// 		return
// 	}
// 	baseName := fmt.Sprintf("%s.csv", fn.Name())
// 	tempDir, testFileName, err := createTestCSV(baseName)
// 	if err != nil {
// 		t.Errorf("unable to generate test data: %s", err)
// 		return
// 	}
// 	defer os.RemoveAll(tempDir)
// 	schema, err := SchemaFromDefs(testFileSchemaDefs)
// 	if err != nil {
// 		t.Fatalf("unable to create test schema: %s", err)
// 		return
// 	}
// 	df, err := FromCSV(testFileName, *schema, true)
// 	if err != nil {
// 		t.Fatalf("unable to create csv from file: %s", err)
// 		return
// 	}
// 	if df.Length() != 7 {
// 		t.Errorf("expected 7 entries, but found %d", df.Length())
// 		return
// 	}
// 	for _, def := range testFileSchemaDefs {
// 		columnName := def.ColumnName
// 		columnType := def.ColumnType
// 		if !slices.Contains(df.Names(), columnName) {
// 			t.Errorf("expected to find column %s but could not in these columns %s", columnName, strings.Join(df.Names(), ", "))
// 		}
// 		actualColumnType, err := df.GetColumnType(columnName)
// 		if err != nil {
// 			t.Errorf("unable to get column type for column %s: %s", columnName, err)
// 		}
// 		if actualColumnType != columnType {
// 			t.Errorf("expected column type of %s for column %s but found %s", columnType, columnName, actualColumnType)
// 		}
// 	}
// 	// Now we are going to take the 3rd row and test to make sure its correct (index 2)
// 	testStringHelper(t, "Symbol", 2, "DFRAME", df)
// 	testIntHelper(t, "Volume", 2, 151971, df)
// 	testFloatHelper(t, "Open", 2, 17.62, df)
// 	testFloatHelper(t, "Close", 2, 17.72, df)
// 	testFloatHelper(t, "High", 2, 17.755000, df)
// 	testFloatHelper(t, "Low", 2, 17.57, df)
// 	testBigIntHelper(t, "WindowStart", 2, 16385076000, df)
// 	testBigIntHelper(t, "Transactions", 2, 535, df)

// }

// func TestWriteCSV(t *testing.T) {
// 	pc, _, _, ok := runtime.Caller(1)
// 	if !ok {
// 		t.Fatal("unable to get function name, something is wrong")
// 		return
// 	}
// 	fn := runtime.FuncForPC(pc)
// 	if fn == nil {
// 		t.Fatal("unable to get function name from pc, something is wrong")
// 		return
// 	}
// 	baseName := fmt.Sprintf("%s.csv", fn.Name())
// 	tempDir, testFileName, err := createTestCSV(baseName)
// 	if err != nil {
// 		t.Errorf("unable to generate test data: %s", err)
// 		return
// 	}
// 	defer os.RemoveAll(tempDir)
// 	schema, err := SchemaFromDefs(testFileSchemaDefs)
// 	if err != nil {
// 		t.Fatalf("unable to create test schema: %s", err)
// 		return
// 	}
// 	df, err := FromCSV(testFileName, *schema, true)
// 	if err != nil {
// 		t.Fatalf("unable to create csv from file: %s", err)
// 		return
// 	}
// 	newfilename := path.Join(tempDir, "output_file.csv")
// 	err = df.WriteCSV(newfilename)
// 	if err != nil {
// 		t.Fatalf("unable to write to %s: %s", newfilename, err)
// 		return
// 	}
// 	f, err := os.Open(newfilename)
// 	if err != nil {
// 		t.Fatalf("unable to open test output file %s: %s", newfilename, err)
// 		return
// 	}
// 	defer f.Close()
// 	r := csv.NewReader(f)
// 	records, err := r.ReadAll()
// 	if err != nil {
// 		t.Fatalf("could not read csv data for %s: %s", newfilename, err)
// 		return
// 	}
// 	// We can't test the direct file as the header will change (on purpose)
// 	// from the test data
// 	// Now we test the number of rows (7 + 1 for the header)
// 	if len(records) != df.Length()+1 {
// 		t.Errorf("expected %d rows, but found %d", df.Length()+1, len(records))
// 	}
// 	// Now we test the rows to make sure it has the right number of columns
// 	for _, record := range records {
// 		if len(record) != len(df.Names()) {
// 			t.Errorf("expected %d columns, but found %d", len(df.Names()), len(record))
// 		}
// 	}

// }
