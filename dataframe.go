package dataframe

import (
	"fmt"
	"os"
	"reflect"
	"slices"
	"strconv"

	"github.com/jedib0t/go-pretty/table"
)

// Dataframe is a structure that stores data in a variety of formats
// the data is primarily stored as Column types, which contain a type
// name, and actual data.  The Dataframe must have all columns of the
// same length
type Dataframe struct {
	intColumns    map[string]*Column[int]
	floatColumns  map[string]*Column[float64]
	bigIntColumns map[string]*Column[int64]
	stringColumns map[string]*Column[string]
	columnTypes   map[string]reflect.Kind
	columnOrder   []string
	numberRows    int
}

// Slice will return a pointer to a new dataframe that is sliced from
// the provided start and stop indices using the idiomatic Go slicing
// indices
func (d Dataframe) Slice(start, stop int) (*Dataframe, error) {
	df := Dataframe{}
	for _, columnName := range d.columnOrder {
		switch d.columnTypes[columnName] {
		case reflect.String:
			column := d.stringColumns[columnName]
			newColumn, err := column.Slice(start, stop)
			if err != nil {
				return nil, fmt.Errorf("unable to slice column %s: %w", columnName, err)
			}
			d.AddStringColumn(*newColumn)
		case reflect.Int:
			column := d.intColumns[columnName]
			newColumn, err := column.Slice(start, stop)
			if err != nil {
				return nil, fmt.Errorf("unable to slice column %s: %w", columnName, err)
			}
			d.AddIntColumn(*newColumn)
		case reflect.Int64:
			column := d.bigIntColumns[columnName]
			newColumn, err := column.Slice(start, stop)
			if err != nil {
				return nil, fmt.Errorf("unable to slice column %s: %w", columnName, err)
			}
			d.AddBigIntColumn(*newColumn)
		case reflect.Float64:
			column := d.floatColumns[columnName]
			newColumn, err := column.Slice(start, stop)
			if err != nil {
				return nil, fmt.Errorf("unable to slice column %s: %w", columnName, err)
			}
			d.AddFloatColumn(*newColumn)
		default:
			return nil, fmt.Errorf("column %s is an unsupported type", columnName)
		}
	}
	return &df, nil
}

// GetIntValue is a method that will fetch the integer value from
// a specific column and a specific ndx
func (d Dataframe) GetIntValue(columnName string, ndx int) (int, error) {
	if !slices.Contains(d.columnOrder, columnName) {
		return -1, MissingColumnError{columnName, reflect.Int}
	}
	if ndx < 0 || ndx > d.numberRows-1 {
		return -1, IndexOutOfBounds{columnName, ndx, d.numberRows}
	}
	if d.columnTypes[columnName] != reflect.Int {
		return -1, WrongColumnTypeError{columnName, reflect.Int, d.columnTypes[columnName]}
	}
	column := d.intColumns[columnName]
	return column.GetValueAtIndex(ndx)
}

// GetBigIntValue is a method that will fetch the integer value from
// a specific column and a specific index
func (d Dataframe) GetBigIntValue(columnName string, ndx int) (int64, error) {
	if !slices.Contains(d.columnOrder, columnName) {
		return -1, MissingColumnError{columnName, reflect.Int64}
	}
	if ndx < 0 || ndx > d.numberRows-1 {
		return -1, IndexOutOfBounds{columnName, ndx, d.numberRows}
	}
	if d.columnTypes[columnName] != reflect.Int64 {
		return -1, WrongColumnTypeError{columnName, reflect.Int64, d.columnTypes[columnName]}
	}
	column := d.bigIntColumns[columnName]
	return column.GetValueAtIndex(ndx)
}

// GetStringValue is a method that will fetch the integer value from
// a specific column and a specific ndx
func (d Dataframe) GetStringValue(columnName string, ndx int) (string, error) {
	if !slices.Contains(d.columnOrder, columnName) {
		return "", MissingColumnError{columnName, reflect.String}
	}
	if ndx < 0 || ndx > d.numberRows-1 {
		return "", IndexOutOfBounds{columnName, ndx, d.numberRows}
	}
	if d.columnTypes[columnName] != reflect.String {
		return "", WrongColumnTypeError{columnName, reflect.String, d.columnTypes[columnName]}
	}
	column := d.stringColumns[columnName]
	return column.GetValueAtIndex(ndx)
}

// GetFloatValue is a method that will fetch the integer value from
// a specific column and a specific ndx
func (d Dataframe) GetFloatValue(columnName string, ndx int) (float64, error) {
	if !slices.Contains(d.columnOrder, columnName) {
		return -1, MissingColumnError{columnName, reflect.Float64}
	}
	if ndx < 0 || ndx > d.numberRows-1 {
		return -1, IndexOutOfBounds{columnName, ndx, d.numberRows}
	}
	if d.columnTypes[columnName] != reflect.Float64 {
		return -1, WrongColumnTypeError{columnName, reflect.Float64, d.columnTypes[columnName]}
	}
	column := d.floatColumns[columnName]
	return column.GetValueAtIndex(ndx)
}

// AddIntColumn will add a column of type int to the dataframe
// and check validity
func (d *Dataframe) AddIntColumn(col Column[int]) error {
	if d.numberRows == 0 {
		d.numberRows = col.Length()
	}
	if col.Length() != d.numberRows {
		return RowCountMismatchError{col.ColumnName, d.numberRows, col.Length()}
	}
	if slices.Contains(d.columnOrder, col.ColumnName) {
		return ColumnAlreadyExists{col.ColumnName}
	}
	d.columnOrder = append(d.columnOrder, col.ColumnName)
	d.intColumns[col.ColumnName] = &col
	d.columnTypes[col.ColumnName] = col.ColumnType
	return d.IsValid()
}

// AddBigIntColumn will add a column of type int to the dataframe
// and check validity
func (d *Dataframe) AddBigIntColumn(col Column[int64]) error {
	if d.numberRows == 0 {
		d.numberRows = col.Length()
	}
	if col.Length() != d.numberRows {
		return RowCountMismatchError{col.ColumnName, d.numberRows, col.Length()}
	}
	if slices.Contains(d.columnOrder, col.ColumnName) {
		return ColumnAlreadyExists{col.ColumnName}
	}
	d.columnOrder = append(d.columnOrder, col.ColumnName)
	d.bigIntColumns[col.ColumnName] = &col
	d.columnTypes[col.ColumnName] = col.ColumnType
	return d.IsValid()
}

// AddStringColumn will add a column of type int to the dataframe
// and check validity
func (d *Dataframe) AddStringColumn(col Column[string]) error {
	if d.numberRows == 0 {
		d.numberRows = col.Length()
	}
	if col.Length() != d.numberRows {
		return RowCountMismatchError{col.ColumnName, d.numberRows, col.Length()}
	}
	if slices.Contains(d.columnOrder, col.ColumnName) {
		return ColumnAlreadyExists{col.ColumnName}
	}
	d.columnOrder = append(d.columnOrder, col.ColumnName)
	d.stringColumns[col.ColumnName] = &col
	d.columnTypes[col.ColumnName] = col.ColumnType
	return d.IsValid()
}

// AddStringColumn will add a column of type int to the dataframe
// and check validity
func (d *Dataframe) AddFloatColumn(col Column[float64]) error {
	if d.numberRows == 0 {
		d.numberRows = col.Length()
	}
	if col.Length() != d.numberRows {
		return RowCountMismatchError{col.ColumnName, d.numberRows, col.Length()}
	}
	if slices.Contains(d.columnOrder, col.ColumnName) {
		return ColumnAlreadyExists{col.ColumnName}
	}
	d.columnOrder = append(d.columnOrder, col.ColumnName)
	d.floatColumns[col.ColumnName] = &col
	d.columnTypes[col.ColumnName] = col.ColumnType
	return d.IsValid()
}

// IsValid determines if all columns are the same length, returning
// an error if they are not all the same length
func (d *Dataframe) IsValid() error {
	for _, col := range d.intColumns {
		if d.numberRows == 0 {
			d.numberRows = col.Length()
		}
		if col.Length() != d.numberRows {
			return RowCountMismatchError{col.ColumnName, d.numberRows, col.Length()}
		}
	}
	for _, col := range d.floatColumns {
		if d.numberRows == 0 {
			d.numberRows = col.Length()
		}
		if col.Length() != d.numberRows {
			return RowCountMismatchError{col.ColumnName, d.numberRows, col.Length()}
		}
	}
	for _, col := range d.bigIntColumns {
		if d.numberRows == 0 {
			d.numberRows = col.Length()
		}
		if col.Length() != d.numberRows {
			return RowCountMismatchError{col.ColumnName, d.numberRows, col.Length()}
		}
	}
	for _, col := range d.stringColumns {
		if d.numberRows == 0 {
			d.numberRows = col.Length()
		}
		if col.Length() != d.numberRows {
			return RowCountMismatchError{col.ColumnName, d.numberRows, col.Length()}
		}
	}
	return nil
}

// ParseValue takes a columnName and a string value and appends it
// to that column.  This will return an error if the string cannot
// be converted
func (d *Dataframe) ParseValue(columnName, value string) error {
	colType, ok := d.columnTypes[columnName]
	if !ok {
		return fmt.Errorf("column %s does not exist", columnName)
	}
	switch colType {
	case reflect.String:
		col := d.stringColumns[columnName]
		col.AppendValue(value)
	case reflect.Int:
		col := d.intColumns[columnName]
		val, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("unable to parse %s into Int", value)
		}
		col.AppendValue(val)
	case reflect.Int64:
		col := d.bigIntColumns[columnName]
		val, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Errorf("unable to parse %s into Int64", value)
		}
		col.AppendValue(val)
	case reflect.Float64:
		col := d.floatColumns[columnName]
		val, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return fmt.Errorf("unable to parse %s into Float64", value)
		}
		col.AppendValue(val)
	}
	return nil
}

// New is the dataframe constructor, as there are complex data types
// that need to be initialized for use
func New() *Dataframe {
	return &Dataframe{
		numberRows:    0,
		intColumns:    make(map[string]*Column[int]),
		stringColumns: make(map[string]*Column[string]),
		bigIntColumns: make(map[string]*Column[int64]),
		floatColumns:  make(map[string]*Column[float64]),
		columnTypes:   make(map[string]reflect.Kind),
		columnOrder:   []string{},
	}
}

func (d Dataframe) createHeader(columnCount int) []interface{} {
	var row []interface{}
	for _, columnName := range d.columnOrder[:columnCount] {
		row = append(row, columnName)
	}
	return row
}

func (d Dataframe) createRowFromNdx(ndx, columnCount int) ([]interface{}, error) {
	var row []interface{}
	for _, columnName := range d.columnOrder[:columnCount] {
		colType := d.columnTypes[columnName]
		switch colType {
		case reflect.String:
			col := d.stringColumns[columnName]
			val, err := col.GetValueAtIndex(ndx)
			if err != nil {
				return nil, err
			}
			row = append(row, val)
		case reflect.Int:
			col := d.intColumns[columnName]
			val, err := col.GetValueAtIndex(ndx)
			if err != nil {
				return nil, err
			}
			row = append(row, val)
		case reflect.Int64:
			col := d.bigIntColumns[columnName]
			val, err := col.GetValueAtIndex(ndx)
			if err != nil {
				return nil, err
			}
			row = append(row, val)
		case reflect.Float64:
			col := d.floatColumns[columnName]
			val, err := col.GetValueAtIndex(ndx)
			if err != nil {
				return nil, err
			}
			row = append(row, val)
		default:
			return nil, UnsupportedType{colType}
		}
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

// GetColumnType takes a column name (string) and returns the type of that
// column.  This is useful for determining what function to use to grab a
// Column with.  If the Column doesn't exist, it returns a MissingColumnError
func (d Dataframe) GetColumnType(columnName string) (reflect.Kind, error) {
	columnType, ok := d.columnTypes[columnName]
	if !ok {
		return reflect.Int, MissingColumnError{ColumnName: columnName}
	}
	return columnType, nil
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

// NumberRows returns the number of rows that the dataframe currently has
func (d Dataframe) NumberRows() int {
	return d.numberRows
}
