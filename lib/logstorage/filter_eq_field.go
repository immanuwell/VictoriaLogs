package logstorage

import (
	"fmt"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/prefixfilter"
)

// filterEqField matches if the given fields have equivalent values.
//
// Example LogsQL: `fieldName:eq_field(otherField)`
type filterEqField struct {
	fieldName      string
	otherFieldName string

	prefixFilter     prefixfilter.Filter
	prefixFilterOnce sync.Once
}

func newFilterEqField(fieldName, otherFieldName string) *filterEqField {
	return &filterEqField{
		fieldName:      getCanonicalColumnName(fieldName),
		otherFieldName: getCanonicalColumnName(otherFieldName),
	}
}

func (fe *filterEqField) String() string {
	return fmt.Sprintf("%seq_field(%s)", quoteFieldNameIfNeeded(fe.fieldName), quoteTokenIfNeeded(fe.otherFieldName))
}

func (fe *filterEqField) updateNeededFields(pf *prefixfilter.Filter) {
	pf.AddAllowFilter(fe.fieldName)
	pf.AddAllowFilter(fe.otherFieldName)
}

func (fe *filterEqField) getPrefixFilter() *prefixfilter.Filter {
	fe.prefixFilterOnce.Do(fe.initPrefixFilter)
	return &fe.prefixFilter
}

func (fe *filterEqField) initPrefixFilter() {
	fe.prefixFilter.AddAllowFilters([]string{fe.fieldName, fe.otherFieldName})
}

func (fe *filterEqField) matchRow(fields []Field) bool {
	v := getFieldValueByName(fields, fe.fieldName)
	vOther := getFieldValueByName(fields, fe.otherFieldName)
	return v == vOther
}

func (fe *filterEqField) applyToBlockResult(br *blockResult, bm *bitmap) error {
	if fe.fieldName == fe.otherFieldName {
		return nil
	}

	c := br.getColumnByName(fe.fieldName)
	cOther := br.getColumnByName(fe.otherFieldName)

	if c.isConst && cOther.isConst {
		v := c.valuesEncoded[0]
		vOther := cOther.valuesEncoded[0]
		if v != vOther {
			bm.resetBits()
		}
		return nil
	}
	if c.isTime && cOther.isTime {
		// c and cOther point to the same _time column, since only a single _time column may exist
		return nil
	}

	if c.valueType != cOther.valueType {
		// Slow path - c and cOther have different valueType, so convert them to string values and compare them
		return applyFilterEqString(br, bm, c, cOther)
	}

	switch c.valueType {
	case valueTypeString:
		return applyFilterEqString(br, bm, c, cOther)
	case valueTypeDict:
		return applyFilterEqDict(br, bm, c, cOther)
	case valueTypeUint8:
		return applyFilterEqBinValues(br, bm, c, cOther)
	case valueTypeUint16:
		return applyFilterEqBinValues(br, bm, c, cOther)
	case valueTypeUint32:
		return applyFilterEqBinValues(br, bm, c, cOther)
	case valueTypeUint64:
		return applyFilterEqBinValues(br, bm, c, cOther)
	case valueTypeInt64:
		return applyFilterEqBinValues(br, bm, c, cOther)
	case valueTypeFloat64:
		return applyFilterEqBinValues(br, bm, c, cOther)
	case valueTypeIPv4:
		return applyFilterEqBinValues(br, bm, c, cOther)
	case valueTypeTimestampISO8601:
		return applyFilterEqBinValues(br, bm, c, cOther)
	default:
		logger.Panicf("FATAL: unknown valueType=%d", c.valueType)
	}
	return nil
}

func applyFilterEqString(br *blockResult, bm *bitmap, c, cOther *blockResultColumn) error {
	values, err := c.getValues(br)
	if err != nil {
		return err
	}
	valuesOther, err := cOther.getValues(br)
	if err != nil {
		return err
	}
	bm.forEachSetBit(func(idx int) bool {
		return values[idx] == valuesOther[idx]
	})
	return nil
}

func applyFilterEqDict(br *blockResult, bm *bitmap, c, cOther *blockResultColumn) error {
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
		return v == vOther
	})
	return nil
}

func applyFilterEqBinValues(br *blockResult, bm *bitmap, c, cOther *blockResultColumn) error {
	valuesEncoded, err := c.getValuesEncoded(br)
	if err != nil {
		return err
	}
	valuesEncodedOther, err := cOther.getValuesEncoded(br)
	if err != nil {
		return err
	}
	bm.forEachSetBit(func(idx int) bool {
		return valuesEncoded[idx] == valuesEncodedOther[idx]
	})
	return nil
}

func (fe *filterEqField) applyToBlockSearch(bs *blockSearch, bm *bitmap) error {
	if fe.fieldName == fe.otherFieldName {
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
			if v != vOther {
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
		return fe.applyFilterBinValue(bs, bm, ch, chOther)
	case valueTypeUint16:
		return fe.applyFilterBinValue(bs, bm, ch, chOther)
	case valueTypeUint32:
		return fe.applyFilterBinValue(bs, bm, ch, chOther)
	case valueTypeUint64:
		return fe.applyFilterBinValue(bs, bm, ch, chOther)
	case valueTypeInt64:
		return fe.applyFilterBinValue(bs, bm, ch, chOther)
	case valueTypeFloat64:
		return fe.applyFilterBinValue(bs, bm, ch, chOther)
	case valueTypeIPv4:
		return fe.applyFilterBinValue(bs, bm, ch, chOther)
	case valueTypeTimestampISO8601:
		return fe.applyFilterBinValue(bs, bm, ch, chOther)
	default:
		logger.Panicf("FATAL: %s: unknown valueType=%d", bs.partPath(), ch.valueType)
	}
	return nil
}

func (fe *filterEqField) applyFilterString(bs *blockSearch, bm *bitmap) error {
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
		ok := values[srcIdx] == valuesOther[srcIdx]
		srcIdx++
		return ok
	})

	return nil
}

func (fe *filterEqField) applyFilterDict(bs *blockSearch, bm *bitmap, ch, chOther *columnHeader) error {
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
		return v == vOther
	})
	return nil
}

func (fe *filterEqField) applyFilterBinValue(bs *blockSearch, bm *bitmap, ch, chOther *columnHeader) error {
	valuesEncoded, err := bs.getValuesForColumn(ch)
	if err != nil {
		return err
	}
	valuesEncodedOther, err := bs.getValuesForColumn(chOther)
	if err != nil {
		return err
	}
	bm.forEachSetBit(func(idx int) bool {
		return valuesEncoded[idx] == valuesEncodedOther[idx]
	})
	return nil
}

func getBlockResult() *blockResult {
	v := brPool.Get()
	if v == nil {
		return &blockResult{}
	}
	return v.(*blockResult)
}

func putBlockResult(br *blockResult) {
	br.reset()
	brPool.Put(br)
}

var brPool sync.Pool
