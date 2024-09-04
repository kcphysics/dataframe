package dataframe

import (
	"fmt"
	"log"
	"os"
	"reflect"

	"github.com/jedib0t/go-pretty/table"
	"github.com/kcphysics/dataframe/column"
	"github.com/kcphysics/dataframe/dataframeError"
)

// Dataframe is a structure that stores data in a variety of formats
// the data is primarily stored as Column types, which contain a type
// name, and actual data.  The Dataframe must have all columns of the
// same length
type Dataframe struct {
	columns     map[string]*column.Column
	columnOrder []string
	numberRows  int
}

// Slice will return a pointer to a new dataframe that is sliced from
// the provided start and stop indices using the idiomatic Go slicing
// indices
func (d Dataframe) Slice(start, stop int) (*Dataframe, error) {
	newDF := Dataframe{}
	for colName, col := range d.columns {
		newCol := col.Slice(start, stop)
		err := newDF.AddColumn(newCol)
		if err != nil {
			return nil, fmt.Errorf("error while slicing column %s between %d and %d: %w", colName, start, stop, err)
		}
	}
	return &newDF, nil
}

// AddColumn takes a column and will add it to the dataframe. If the number of rows mismatch
// what is currently in the dataframe, a RowCountMismatch error is returned
func (d *Dataframe) AddColumn(column *column.Column) error {
	if d.numberRows == 0 {
		d.numberRows = column.Length()
	}
	if d.numberRows != column.Length() {
		return &dataframeError.RowCountMismatchError{
			ColumnName: column.Name,
			ShouldHave: d.numberRows,
			DoesHave:   column.Length(),
		}
	}
	d.columnOrder = append(d.columnOrder, column.Name)
	d.columns[column.Name] = column
	return nil
}

// ValueAt takes a Column name and an index in that column and returns a Value
// representing that value
func (d Dataframe) ValueAt(columnName string, ndx int) (*column.Value, error) {
	column, ok := d.columns[columnName]
	if !ok {
		return nil, &dataframeError.MissingColumnError{
			ColumnName: columnName,
		}
	}
	return column.Value(ndx)
}

// IsValid determines if all columns are the same length, returning
// an error if they are not all the same length
func (d *Dataframe) IsValid() error {
	for _, column := range d.columns {
		if d.numberRows == 0 {
			d.numberRows = column.Length()
		}
		if column.Length() != d.numberRows {
			return &dataframeError.RowCountMismatchError{
				ColumnName: column.Name,
				ShouldHave: d.numberRows,
				DoesHave:   column.Length(),
			}
		}
	}
	return nil
}

// AppendTo takes a Column name and a value and appends it.  This will
// return an error if the passed in value is not the same tyoe as the
// column
func (d *Dataframe) AppendTo(columnName string, value interface{}) error {
	column, ok := d.columns[columnName]
	if !ok {
		return &dataframeError.MissingColumnError{ColumnName: columnName}
	}
	return column.Append(value)
}

// AppendFromString takes a columnName and a string value and appends it
// to that column.  This will return an error if the string cannot
// be converted
func (d *Dataframe) AppendFromString(columnName, value string) error {
	column, ok := d.columns[columnName]
	if !ok {
		return &dataframeError.MissingColumnError{ColumnName: columnName}
	}
	switch column.Type {
	case reflect.String:
		return column.AppendString(value)
	case reflect.Int:
		return column.AppendIntFromString(value)
	case reflect.Int64:
		return column.AppendInt64FromString(value)
	case reflect.Float64:
		return column.AppendFloatFromString(value)
	}
	return nil
}

// MapStruct takes a reference to a Struct and an Index and fill the struct with
// the data from columns of the same name.  If columns don't exist, they will be skipped.
// Error is returned in case of problems during mapping
func (d Dataframe) MapStruct(holder interface{}, ndx int) error {
	dataMap, err := d.createMapFromNdx(ndx)
	if err != nil {
		return err
	}
	value := reflect.ValueOf(holder)
	elem := value.Elem()
	for i := 0; i < elem.NumField(); i++ {
		field := elem.Field(i)
		structField := elem.Type().Field(i)
		if !field.CanSet() {
			return fmt.Errorf("unable to set field %s", structField.Name)
		}
		val, ok := dataMap[structField.Name]
		if !ok {
			continue
		}
		switch structField.Type.Kind() {
		case reflect.String:
			v, ok := val.(string)
			if !ok {
				return fmt.Errorf("cannot convert value %v to string for column %s", val, structField.Name)
			}
			field.SetString(v)
		case reflect.Int:
			v, ok := val.(int)
			if !ok {
				return fmt.Errorf("cannot convert value %v to Int for column %s", val, structField.Name)
			}
			field.SetInt(int64(v))
		case reflect.Int64:
			v, ok := val.(int64)
			if !ok {
				return fmt.Errorf("cannot convert value %v to Int64 for column %s", val, structField.Name)
			}
			field.SetInt(v)
		case reflect.Float64:
			v, ok := val.(float64)
			if !ok {
				return fmt.Errorf("cannot convert value %v to Float64 for column %s", val, structField.Name)
			}
			field.SetFloat(v)
		default:
			log.Printf("Can't determine fields set method: %s", structField.Name)
		}
	}
	return nil
}

func (d Dataframe) createHeader(columnCount int) []interface{} {
	var row []interface{}
	for _, columnName := range d.columnOrder[:columnCount] {
		row = append(row, columnName)
	}
	return row
}

func (d Dataframe) createMapFromNdx(ndx int) (map[string]interface{}, error) {
	if ndx < 0 || ndx > d.Length()-1 {
		return nil, dataframeError.IndexOutOfBounds{MaxIndex: d.Length() - 1, BrokenIndex: ndx}
	}
	rowMap := make(map[string]interface{})
	for _, columnName := range d.columnOrder {
		column := d.columns[columnName]
		value, err := column.Value(ndx)
		if err != nil {
			return nil, fmt.Errorf("unable to get value for column %s at ndx %d: %w", columnName, ndx, err)
		}
		rowMap[columnName] = value.Interface()
	}
	return rowMap, nil
}

func (d Dataframe) createRowFromNdx(ndx, columnCount int) ([]interface{}, error) {
	var row []interface{}
	for _, columnName := range d.columnOrder[:columnCount] {
		column := d.columns[columnName]
		val, err := column.Value(ndx)
		if err != nil {
			return nil, err
		}
		row = append(row, val.Interface())
	}
	return row, nil
}

// Table will assemble a go-pretty table.  If there is
// an error during index lookup, it will return an error.  This takes
// the number of columns and rows to show.  If you give 0 or a negative
// number, this will print the entire dataframe
func (d Dataframe) Table(columnCount, rowCount int) (table.Writer, error) {
	columnStop := columnCount
	if columnCount <= 0 {
		columnStop = len(d.columnOrder)
	}
	rowStop := rowCount
	if rowCount <= 0 {
		rowStop = d.Length()
	}
	t := table.NewWriter()
	t.SetStyle(table.StyleBold)
	t.AppendHeader(d.createHeader(columnStop))
	for i := 0; i < rowStop; i++ {
		row, err := d.createRowFromNdx(i, columnStop)
		if err != nil {
			return nil, err
		}
		t.AppendRow(row)
	}
	return t, nil
}

// Length will return the number of rows of the dataframe
func (d Dataframe) Length() int {
	return d.numberRows
}

// String is the stringer interface so it can be printed
func (d Dataframe) String() string {
	returnString := ""
	returnString += fmt.Sprintf("Number of Columns: %d\n", len(d.columnOrder))
	returnString += fmt.Sprintf("Number of Rows:    %d\n", d.numberRows)
	minRows := getMin(10, d.numberRows)
	minCols := getMin(10, len(d.columnOrder))
	table, err := d.Table(minCols, minRows)
	if err != nil {
		returnString += fmt.Sprintf("Cannot Print Table: %s\n", err)
		return returnString
	}
	returnString += table.Render()
	return returnString
}

// Names is a function that will return the names of the dataframe
// in order
func (d Dataframe) Names() []string {
	return d.columnOrder
}

// Column takes a column name and returns a reference to that column or an error
// if it does not exist
func (d Dataframe) Column(name string) (*column.Column, error) {
	column, ok := d.columns[name]
	if !ok {
		return nil, &dataframeError.MissingColumnError{ColumnName: name}
	}
	return column, nil
}

// GetColumnType takes a column name (string) and returns the type of that
// column.  This is useful for determining what function to use to grab a
// Column with.  If the Column doesn't exist, it returns a &dataframeError.MissingColumnError
func (d Dataframe) GetColumnType(columnName string) (reflect.Kind, error) {
	column, ok := d.columns[columnName]
	if !ok {
		return reflect.Int, &dataframeError.MissingColumnError{ColumnName: columnName}
	}
	return column.Type, nil
}

// WriteCSV is a function that takes a filename and returns an error
// if the file cannot be written.
func (d Dataframe) WriteCSV(filename string) error {
	table, err := d.Table(0, 0)
	if err != nil {
		return fmt.Errorf("error during table construction: %w", err)
	}
	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("could not open file %s for writing: %w", filename, err)
	}
	defer f.Close()
	f.WriteString(table.RenderCSV())
	return nil
}

func getMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// SelectForIndices takes a list of column Names and a list of indices
// and creates a new dataframe from those
func (df Dataframe) SelectForIndices(columnNames []string, indices []int) (*Dataframe, error) {
	returnDf := New()
	for _, column := range columnNames {
		col, ok := df.columns[column]
		if !ok {
			return nil, dataframeError.MissingColumnError{ColumnName: column}
		}
		newCol, err := col.Indices(indices)
		if err != nil {
			return nil, fmt.Errorf("cannot get indices for column %s: %w", col.Name, err)
		}
		err = returnDf.AddColumn(newCol)
		if err != nil {
			return nil, fmt.Errorf("cannot add new column %s: %w", col.Name, err)
		}
	}
	return returnDf, nil
}

// Concatenate takes a pointer to a dataframe and then concatenates each column
// in the provided dataframe to this dataframe.  This will return an error if the columns are
// not exactly the same in type or name.
func (df *Dataframe) Concatenate(df2 *Dataframe) error {
	for columnName, column := range df.columns {
		column2, ok := df2.columns[columnName]
		if !ok {
			return &dataframeError.MissingColumnError{ColumnName: columnName, Type: column.Type}
		}
		err := column.Concatenate(column2)
		if err != nil {
			return err
		}
		df.numberRows = column.Length()
	}
	return nil
}

// New is the dataframe constructor, as there are complex data types
// that need to be initialized for use
func New() *Dataframe {
	return &Dataframe{
		numberRows:  0,
		columns:     make(map[string]*column.Column),
		columnOrder: []string{},
	}
}
