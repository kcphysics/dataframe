package dataframe

import (
	"fmt"
	"reflect"
)

type MissingColumnError struct {
	ColumnName string
	Type       reflect.Kind
}

func (m MissingColumnError) Error() string {
	return fmt.Sprintf("dataframe has no %s type column called %s", m.Type, m.ColumnName)
}

type WrongColumnTypeError struct {
	ColumnName  string
	CorrectType reflect.Kind
	CurrentType reflect.Kind
}

func (w WrongColumnTypeError) Error() string {
	return fmt.Sprintf("requested column %s of type %s is actually type %s", w.ColumnName, w.CurrentType, w.CorrectType)
}

type RowCountMismatchError struct {
	ColumnName string
	ShouldHave int
	DoesHave   int
}

func (r RowCountMismatchError) Error() string {
	return fmt.Sprintf("column %s has %d rows, but current dataframe requires %d rows", r.ColumnName, r.DoesHave, r.ShouldHave)
}

type UnsupportedType struct {
	ColumnType reflect.Kind
}

func (u UnsupportedType) Error() string {
	return fmt.Sprintf("type %s is unsupported", u.ColumnType)
}

type ColumnAlreadyExists struct {
	ColumnName string
}

func (c ColumnAlreadyExists) Error() string {
	return fmt.Sprintf("Column %s already exists in the dataframe", c.ColumnName)
}

type IndexOutOfBounds struct {
	ColumnName  string
	BrokenIndex int
	MaxIndex    int
}

func (i IndexOutOfBounds) Error() string {
	return fmt.Sprintf("requested index %d is out of bounds for column %s which has max index %d", i.BrokenIndex, i.ColumnName, i.MaxIndex)
}
