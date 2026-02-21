package logstorage

import (
	"fmt"
)

// filterValueType filters field entries by value type.
//
// For example, the following filter returns all the logs with uint64 fieldName:
//
//	fieldName:value_type("uint64")
type filterValueType struct {
	valueType string
}

func newFilterValueType(fieldName, valueType string) *filterGeneric {
	fv := &filterValueType{
		valueType: valueType,
	}
	return newFilterGeneric(fieldName, fv)
}

func (fv *filterValueType) String() string {
	return fmt.Sprintf("value_type(%s)", quoteTokenIfNeeded(fv.valueType))
}

func (fv *filterValueType) matchRowByField(fields []Field, fieldName string) bool {
	v := getFieldValueByName(fields, fieldName)
	if v == "" {
		// empty values have no any type
		return false
	}

	// Assume all the fields have string type, since we cannot determine the real type of the value at the given field.
	return fv.valueType == "string"
}

func (fv *filterValueType) applyToBlockResultByField(br *blockResult, bm *bitmap, fieldName string) error {
	var typ string
	c := br.getColumnByName(fieldName)
	if c.isConst {
		typ = "const"
	} else if c.isTime {
		typ = "time"
	} else if br.bs == nil {
		typ = "inmemory"
	} else {
		typ = c.valueType.String()
	}
	if fv.valueType != typ {
		bm.resetBits()
	}
	return nil
}

func (fv *filterValueType) applyToBlockSearchByField(bs *blockSearch, bm *bitmap, fieldName string) error {
	// Verify whether fp matches const column
	v, err := bs.getConstColumnValue(fieldName)
	if err != nil || v != "" {
		if fv.valueType != "const" {
			bm.resetBits()
		}
		return err
	}

	// Verify whether fp matches other columns
	ch, err := bs.getColumnHeader(fieldName)
	if err != nil || ch == nil {
		bm.resetBits()
		return err
	}

	typ := ch.valueType.String()
	if fv.valueType != typ {
		bm.resetBits()
	}
	return nil
}
