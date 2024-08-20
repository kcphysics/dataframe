package columns

import "fmt"

func FilterArray(ndx int, operation FilterType, items []interface{}) ([]interface{}, error) {
	switch operation {
	case Greater:
		return items[ndx+1:], nil
	case GreaterEq:
		return items[ndx:], nil
	case Lesser:
		return items[:ndx-1], nil
	case LesserEq:
		return items[:ndx], nil
	case Equal:
		var returnItems []interface{}
		returnItems = append(returnItems, items[ndx])
		return returnItems, nil
	default:
		return nil, fmt.Errorf("filter of type %s not supported", operation)
	}
}

func getBoundsForFilter(operation FilterType, ndx, length int) (int, int) {
	switch operation {
	case Greater:
		return ndx + 1, length - 1
	case GreaterEq:
		return ndx, length - 1
	case Lesser:
		return 0, ndx - 1
	case LesserEq:
		return 0, ndx
	case Equal:
		return ndx - 1, ndx
	}
	return -1, -1
}

func getMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}
