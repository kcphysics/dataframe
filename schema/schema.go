package schema

// import (
// 	"fmt"
// 	"reflect"
// 	"slices"

// 	"github.com/kcphysics/dataframe/errors"
// )

// // SchemaDef is a type that contains column name and type
// type SchemaDef struct {
// 	ColumnName string
// 	ColumnType reflect.Kind
// }

// // Schema is used to determine the type of variables
// // being read from a CSV
// type Schema struct {
// 	columnOrder []string
// 	columnType  []reflect.Kind
// }

// func (s Schema) isAllowedType(columnType reflect.Kind) bool {
// 	switch columnType {
// 	case reflect.String, reflect.Int, reflect.Int64, reflect.Float64:
// 		return true
// 	default:
// 		return false
// 	}
// }

// // AddColumn takes a name and Kind and stores it
// // in the schema
// func (s *Schema) AddColumn(columnName string, columnType reflect.Kind) error {
// 	if !s.isAllowedType(columnType) {
// 		return errors.UnsupportedType{ColumnType: columnType}
// 	}
// 	s.columnOrder = append(s.columnOrder, columnName)
// 	s.columnType = append(s.columnType, columnType)
// 	return nil
// }

// // FromMap takes a map[string]reflect.Kind and adds the columns
// // This could have order issues, so be careful.  At the time of
// // writing, order is usually preserved but not guaranteed
// func (s *Schema) FromMap(columns map[string]reflect.Kind) error {
// 	for columnName, columnType := range columns {
// 		err := s.AddColumn(columnName, columnType)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

// // Names returns an ordered list of strings that represents all
// // columns in the schema
// func (s Schema) Names() []string {
// 	return s.columnOrder
// }

// // ReorderColumns takes a list of strings and will change the
// // ordering of the schema.  It requires the correct number
// // of columns and all column names that currently exist in the schema
// // must be represented
// func (s *Schema) ReorderColumns(newOrder []string) error {
// 	if len(newOrder) != len(s.columnOrder) {
// 		return fmt.Errorf("expected new order to have %d entries, but found %d", len(s.columnOrder), len(newOrder))
// 	}
// 	var orderToBeSet []string
// 	for _, columnName := range newOrder {
// 		if !slices.Contains(s.columnOrder, columnName) {
// 			return fmt.Errorf("column %s has been given, but does not currently exist", columnName)
// 		}
// 		orderToBeSet = append(orderToBeSet, columnName)
// 	}
// 	s.columnOrder = orderToBeSet
// 	return nil
// }

// // BuildDF will construct a dataframe from this schema, including
// // initializing all of the internal fields of the DF
// func (s Schema) BuildDF() (*Dataframe, error) {
// 	df := New()
// 	for ndx, columnName := range s.columnOrder {
// 		columnType := s.columnType[ndx]
// 		switch columnType {
// 		case reflect.String:
// 			col, err := NewColumn[string](columnName, []string{})
// 			if err != nil {
// 				return nil, err
// 			}
// 			df.AddStringColumn(*col)
// 		case reflect.Int:
// 			col, err := NewColumn[int](columnName, []int{})
// 			if err != nil {
// 				return nil, err
// 			}
// 			df.AddIntColumn(*col)
// 		case reflect.Int64:
// 			col, err := NewColumn[int64](columnName, []int64{})
// 			if err != nil {
// 				return nil, err
// 			}
// 			df.AddBigIntColumn(*col)
// 		case reflect.Float64:
// 			col, err := NewColumn[float64](columnName, []float64{})
// 			if err != nil {
// 				return nil, err
// 			}
// 			df.AddFloatColumn(*col)
// 		default:
// 			return nil, errors.UnsupportedType{ColumnType: columnType}
// 		}
// 	}
// 	return df, nil
// }

// // ColumnFromIndex retrieves the column name from the index provided
// // and errors otherwise
// func (s Schema) ColumnFromIndex(ndx int) (string, error) {
// 	if ndx < 0 || ndx > len(s.columnOrder)-1 {
// 		return "", errors.IndexOutOfBounds{"NA", ndx, len(s.columnOrder)}
// 	}
// 	return s.columnOrder[ndx], nil
// }

// // SchemaFromDefs is an alternate constructor for Schema and
// // takes a slice of SchemaDef structs and builds a schema.  If
// // an unsupported type is given, an UnsupportedType error will be returned
// func SchemaFromDefs(defs []SchemaDef) (*Schema, error) {
// 	s := Schema{}
// 	for _, def := range defs {
// 		err := s.AddColumn(def.ColumnName, def.ColumnType)
// 		if err != nil {
// 			return nil, err
// 		}
// 	}
// 	return &s, nil
// }
