package logstorage

import (
	"fmt"
	"math"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/prefixfilter"
)

// filterLeField matches if the fieldName field is smaller or equal to the otherFieldName field
//
// Example LogsQL: `fieldName:le_field(otherField)`
type filterLeField struct {
	fieldName      string
	otherFieldName string

	excludeEqualValues bool

	prefixFilter     prefixfilter.Filter
	prefixFilterOnce sync.Once
}

func newFilterLeField(fieldName, otherFieldName string, excludeEqualValues bool) *filterLeField {
	return &filterLeField{
		fieldName:      getCanonicalColumnName(fieldName),
		otherFieldName: getCanonicalColumnName(otherFieldName),

		excludeEqualValues: excludeEqualValues,
	}
}

func (fe *filterLeField) String() string {
	funcName := "le_field"
	if fe.excludeEqualValues {
		funcName = "lt_field"
	}
	return fmt.Sprintf("%s%s(%s)", quoteFieldNameIfNeeded(fe.fieldName), funcName, quoteTokenIfNeeded(fe.otherFieldName))
}

func (fe *filterLeField) updateNeededFields(pf *prefixfilter.Filter) {
	pf.AddAllowFilter(fe.fieldName)
	pf.AddAllowFilter(fe.otherFieldName)
}

func (fe *filterLeField) getPrefixFilter() *prefixfilter.Filter {
	fe.prefixFilterOnce.Do(fe.initPrefixFilter)
	return &fe.prefixFilter
}

func (fe *filterLeField) initPrefixFilter() {
	fe.prefixFilter.AddAllowFilters([]string{fe.fieldName, fe.otherFieldName})
}

func (fe *filterLeField) matchRow(fields []Field) bool {
	v := getFieldValueByName(fields, fe.fieldName)
	vOther := getFieldValueByName(fields, fe.otherFieldName)
	return leValuesString(v, vOther, fe.excludeEqualValues)
}

func (fe *filterLeField) applyToBlockResult(br *blockResult, bm *bitmap) error {
	if fe.fieldName == fe.otherFieldName {
		if fe.excludeEqualValues {
			bm.resetBits()
		}
		return nil
	}

	c := br.getColumnByName(fe.fieldName)
	cOther := br.getColumnByName(fe.otherFieldName)

	if c.isConst && cOther.isConst {
		v := c.valuesEncoded[0]
		vOther := cOther.valuesEncoded[0]
		if !leValuesString(v, vOther, fe.excludeEqualValues) {
			bm.resetBits()
		}
		return nil
	}
	if c.isTime && cOther.isTime {
		// c and cOther point to the same _time column, since only a single _time column may exist
		if fe.excludeEqualValues {
			bm.resetBits()
		}
		return nil
	}

	if c.valueType != cOther.valueType {
		// Slow path - c and cOther have different valueType, so convert them to string values and compare them
		return applyFilterLeString(br, bm, c, cOther, fe.excludeEqualValues)
	}

	switch c.valueType {
	case valueTypeString:
		return applyFilterLeString(br, bm, c, cOther, fe.excludeEqualValues)
	case valueTypeDict:
		return applyFilterLeDict(br, bm, c, cOther, fe.excludeEqualValues)
	case valueTypeUint8:
		return applyFilterLeUint(br, bm, c, cOther, fe.excludeEqualValues)
	case valueTypeUint16:
		return applyFilterLeUint(br, bm, c, cOther, fe.excludeEqualValues)
	case valueTypeUint32:
		return applyFilterLeUint(br, bm, c, cOther, fe.excludeEqualValues)
	case valueTypeUint64:
		return applyFilterLeUint(br, bm, c, cOther, fe.excludeEqualValues)
	case valueTypeInt64:
		return applyFilterLeInt64(br, bm, c, cOther, fe.excludeEqualValues)
	case valueTypeFloat64:
		return applyFilterLeFloat64(br, bm, c, cOther, fe.excludeEqualValues)
	case valueTypeIPv4:
		return applyFilterLeUint(br, bm, c, cOther, fe.excludeEqualValues)
	case valueTypeTimestampISO8601:
		return applyFilterLeUint(br, bm, c, cOther, fe.excludeEqualValues)
	default:
		logger.Panicf("FATAL: unknown valueType=%d", c.valueType)
	}
	return nil
}

func applyFilterLeString(br *blockResult, bm *bitmap, c, cOther *blockResultColumn, excludeEqualValues bool) error {
	values, err := c.getValues(br)
	if err != nil {
		return err
	}
	valuesOther, err := cOther.getValues(br)
	if err != nil {
		return err
	}
	bm.forEachSetBit(func(idx int) bool {
		return leValuesString(values[idx], valuesOther[idx], excludeEqualValues)
	})
	return nil
}

func applyFilterLeDict(br *blockResult, bm *bitmap, c, cOther *blockResultColumn, excludeEqualValues bool) error {
	valuesEncoded, err := c.getValuesEncoded(br)
	if err != nil {
		return err
	}
	valuesEncodedOther, err := cOther.getValuesEncoded(br)
	if err != nil {
		return err
	}
	bm.forEachSetBit(func(idx int) bool {
		dictIdx := valuesEncoded[idx][0]
		dictIdxOther := valuesEncodedOther[idx][0]
		v := c.dictValues[dictIdx]
		vOther := cOther.dictValues[dictIdxOther]
		return leValuesString(v, vOther, excludeEqualValues)
	})
	return nil
}

func applyFilterLeUint(br *blockResult, bm *bitmap, c, cOther *blockResultColumn, excludeEqualValues bool) error {
	valuesEncoded, err := c.getValuesEncoded(br)
	if err != nil {
		return err
	}
	valuesEncodedOther, err := cOther.getValuesEncoded(br)
	if err != nil {
		return err
	}
	bm.forEachSetBit(func(idx int) bool {
		return leValuesString(valuesEncoded[idx], valuesEncodedOther[idx], excludeEqualValues)
	})
	return nil
}

func applyFilterLeInt64(br *blockResult, bm *bitmap, c, cOther *blockResultColumn, excludeEqualValues bool) error {
	valuesEncoded, err := c.getValuesEncoded(br)
	if err != nil {
		return err
	}
	valuesEncodedOther, err := cOther.getValuesEncoded(br)
	if err != nil {
		return err
	}
	bm.forEachSetBit(func(idx int) bool {
		n := unmarshalInt64(valuesEncoded[idx])
		nOther := unmarshalInt64(valuesEncodedOther[idx])
		return leValuesInt64(n, nOther, excludeEqualValues)
	})
	return nil
}

func applyFilterLeFloat64(br *blockResult, bm *bitmap, c, cOther *blockResultColumn, excludeEqualValues bool) error {
	valuesEncoded, err := c.getValuesEncoded(br)
	if err != nil {
		return err
	}
	valuesEncodedOther, err := cOther.getValuesEncoded(br)
	if err != nil {
		return err
	}
	bm.forEachSetBit(func(idx int) bool {
		f := unmarshalFloat64(valuesEncoded[idx])
		fOther := unmarshalFloat64(valuesEncodedOther[idx])
		return leValuesFloat64(f, fOther, excludeEqualValues)
	})
	return nil
}

func (fe *filterLeField) applyToBlockSearch(bs *blockSearch, bm *bitmap) error {
	if fe.fieldName == fe.otherFieldName {
		if fe.excludeEqualValues {
			bm.resetBits()
		}
		return nil
	}

	v, err := bs.getConstColumnValue(fe.fieldName)
	if err != nil {
		return err
	}
	vOther, err := bs.getConstColumnValue(fe.otherFieldName)
	if err != nil {
		return err
	}
	if v != "" || vOther != "" {
		if v != "" && vOther != "" {
			if !leValuesString(v, vOther, fe.excludeEqualValues) {
				bm.resetBits()
			}
			return nil
		}
		return fe.applyFilterString(bs, bm)
	}

	ch, err := bs.getColumnHeader(fe.fieldName)
	if err != nil {
		return err
	}
	chOther, err := bs.getColumnHeader(fe.otherFieldName)
	if err != nil {
		return err
	}
	if ch == nil || chOther == nil {
		if ch == nil && chOther == nil {
			if fe.excludeEqualValues {
				bm.resetBits()
			}
			return nil
		}
		return fe.applyFilterString(bs, bm)
	}

	if ch.valueType != chOther.valueType {
		// Slow path - c and cOther have different valueType, so convert them to string values and compare them
		return fe.applyFilterString(bs, bm)
	}

	switch ch.valueType {
	case valueTypeString:
		return fe.applyFilterString(bs, bm)
	case valueTypeDict:
		return fe.applyFilterDict(bs, bm, ch, chOther)
	case valueTypeUint8:
		return fe.applyFilterUint(bs, bm, ch, chOther)
	case valueTypeUint16:
		return fe.applyFilterUint(bs, bm, ch, chOther)
	case valueTypeUint32:
		return fe.applyFilterUint(bs, bm, ch, chOther)
	case valueTypeUint64:
		return fe.applyFilterUint(bs, bm, ch, chOther)
	case valueTypeInt64:
		return fe.applyFilterInt64(bs, bm, ch, chOther)
	case valueTypeFloat64:
		return fe.applyFilterFloat64(bs, bm, ch, chOther)
	case valueTypeIPv4:
		return fe.applyFilterUint(bs, bm, ch, chOther)
	case valueTypeTimestampISO8601:
		return fe.applyFilterUint(bs, bm, ch, chOther)
	default:
		logger.Panicf("FATAL: %s: unknown valueType=%d", bs.partPath(), ch.valueType)
	}
	return nil
}

func (fe *filterLeField) applyFilterString(bs *blockSearch, bm *bitmap) error {
	br := getBlockResult()
	defer putBlockResult(br)
	br.mustInit(bs, bm)

	pf := fe.getPrefixFilter()
	if err := br.initColumns(pf); err != nil {
		return err
	}

	c := br.getColumnByName(fe.fieldName)
	cOther := br.getColumnByName(fe.otherFieldName)

	values, err := c.getValues(br)
	if err != nil {
		return err
	}
	valuesOther, err := cOther.getValues(br)
	if err != nil {
		return err
	}

	srcIdx := 0
	bm.forEachSetBit(func(_ int) bool {
		ok := leValuesString(values[srcIdx], valuesOther[srcIdx], fe.excludeEqualValues)
		srcIdx++
		return ok
	})

	return nil
}

func (fe *filterLeField) applyFilterDict(bs *blockSearch, bm *bitmap, ch, chOther *columnHeader) error {
	valuesEncoded, err := bs.getValuesForColumn(ch)
	if err != nil {
		return err
	}
	valuesEncodedOther, err := bs.getValuesForColumn(chOther)
	if err != nil {
		return err
	}
	bm.forEachSetBit(func(idx int) bool {
		dictIdx := valuesEncoded[idx][0]
		dictIdxOther := valuesEncodedOther[idx][0]
		v := ch.valuesDict.values[dictIdx]
		vOther := chOther.valuesDict.values[dictIdxOther]
		return leValuesString(v, vOther, fe.excludeEqualValues)
	})
	return nil
}

func (fe *filterLeField) applyFilterUint(bs *blockSearch, bm *bitmap, ch, chOther *columnHeader) error {
	valuesEncoded, err := bs.getValuesForColumn(ch)
	if err != nil {
		return err
	}
	valuesEncodedOther, err := bs.getValuesForColumn(chOther)
	if err != nil {
		return err
	}
	bm.forEachSetBit(func(idx int) bool {
		return leValuesString(valuesEncoded[idx], valuesEncodedOther[idx], fe.excludeEqualValues)
	})
	return nil
}

func (fe *filterLeField) applyFilterInt64(bs *blockSearch, bm *bitmap, ch, chOther *columnHeader) error {
	valuesEncoded, err := bs.getValuesForColumn(ch)
	if err != nil {
		return err
	}
	valuesEncodedOther, err := bs.getValuesForColumn(chOther)
	if err != nil {
		return err
	}
	bm.forEachSetBit(func(idx int) bool {
		n := unmarshalInt64(valuesEncoded[idx])
		nOther := unmarshalInt64(valuesEncodedOther[idx])
		return leValuesInt64(n, nOther, fe.excludeEqualValues)
	})
	return nil
}

func (fe *filterLeField) applyFilterFloat64(bs *blockSearch, bm *bitmap, ch, chOther *columnHeader) error {
	valuesEncoded, err := bs.getValuesForColumn(ch)
	if err != nil {
		return err
	}
	valuesEncodedOther, err := bs.getValuesForColumn(chOther)
	if err != nil {
		return err
	}
	bm.forEachSetBit(func(idx int) bool {
		f := unmarshalFloat64(valuesEncoded[idx])
		fOther := unmarshalFloat64(valuesEncodedOther[idx])
		return leValuesFloat64(f, fOther, fe.excludeEqualValues)
	})
	return err
}

func leValuesString(a, b string, excludeEqualValues bool) bool {
	fA := parseMathNumber(a)
	if !math.IsNaN(fA) {
		fB := parseMathNumber(b)
		if !math.IsNaN(fB) {
			if excludeEqualValues {
				return fA < fB
			}
			return fA <= fB
		}
	}
	if excludeEqualValues {
		return a < b
	}
	return a <= b
}

func leValuesInt64(a, b int64, excludeEqualValues bool) bool {
	if excludeEqualValues {
		return a < b
	}
	return a <= b
}

func leValuesFloat64(a, b float64, excludeEqualValues bool) bool {
	if excludeEqualValues {
		return a < b
	}
	return a <= b
}
