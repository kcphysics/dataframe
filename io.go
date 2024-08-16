package dataframe

import (
	"encoding/csv"
	"fmt"
	"os"
)

// Creates a Dataframe from CSV.  Allows the specification of a header.  If it
// has a header, it will skip the first row.  Schema is required (although this
// will hopefully change in the future)
func FromCSV(filename string, schema Schema, hasHeader bool) (*Dataframe, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("unable to open file %s: %w", filename, err)
	}
	r := csv.NewReader(f)
	if hasHeader {
		r.Read()
	}
	records, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("unable to read records from %s: %w", filename, err)
	}
	df, err := parseCSVRecords(records, schema)
	if err != nil {
		return nil, fmt.Errorf("error during csv record parsing: %w", err)
	}
	return df, nil
}

func parseCSVRecords(records [][]string, schema Schema) (*Dataframe, error) {
	df, err := schema.BuildDF()
	if err != nil {
		return nil, err
	}
	for rowNumber, record := range records {
		for ndx, value := range record {
			columnName, err := schema.ColumnFromIndex(ndx)
			if err != nil {
				return nil, err
			}
			err = df.ParseValue(columnName, value)
			if err != nil {
				return nil, fmt.Errorf("unable to parse column %d on row %d: %w", ndx, rowNumber, err)
			}
		}
	}
	err = df.IsValid()
	if err != nil {
		return nil, fmt.Errorf("unable to create dataframe as its invalid: %w", err)
	}
	return df, nil
}
