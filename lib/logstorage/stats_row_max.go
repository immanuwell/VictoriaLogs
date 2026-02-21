package logstorage

import (
	"fmt"
	"math"
	"strings"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/prefixfilter"
)

type statsRowMax struct {
	srcField string

	fieldFilters []string
}

func (sm *statsRowMax) String() string {
	s := "row_max(" + quoteTokenIfNeeded(sm.srcField)
	if !prefixfilter.MatchAll(sm.fieldFilters) {
		s += ", " + fieldNamesString(sm.fieldFilters)
	}
	s += ")"
	return s
}

func (sm *statsRowMax) updateNeededFields(pf *prefixfilter.Filter) {
	pf.AddAllowFilters(sm.fieldFilters)
	pf.AddAllowFilter(sm.srcField)
}

func (sm *statsRowMax) newStatsProcessor(a *chunkedAllocator) statsProcessor {
	return a.newStatsRowMaxProcessor()
}

type statsRowMaxProcessor struct {
	max string

	fields []Field
}

func (smp *statsRowMaxProcessor) updateStatsForAllRows(sf statsFunc, br *blockResult) (int, error) {
	sm := sf.(*statsRowMax)
	stateSizeIncrease := 0

	c := br.getColumnByName(sm.srcField)
	if c.isConst {
		v := c.valuesEncoded[0]
		state, err := smp.updateState(sm, v, br, 0)
		if err != nil {
			return 0, err
		}
		stateSizeIncrease += state
		return stateSizeIncrease, nil
	}
	if c.isTime {
		timestamp, ok := TryParseTimestampRFC3339Nano(smp.max)
		if !ok {
			timestamp = -1 << 63
		}
		maxTimestamp, err := br.getMaxTimestamp(timestamp)
		if err != nil {
			return 0, err
		}
		if maxTimestamp <= timestamp {
			return stateSizeIncrease, nil
		}

		bb := bbPool.Get()
		defer bbPool.Put(bb)
		bb.B = marshalTimestampRFC3339NanoString(bb.B[:0], maxTimestamp)
		v := bytesutil.ToUnsafeString(bb.B)
		state, err := smp.updateState(sm, v, br, br.rowsLen-1)
		if err != nil {
			return 0, err
		}
		stateSizeIncrease += state
		return stateSizeIncrease, nil
	}

	needUpdateState := false
	switch c.valueType {
	case valueTypeString:
		needUpdateState = true
	case valueTypeDict:
		err := c.forEachDictValue(br, func(v string) {
			if !needUpdateState && smp.needUpdateStateString(v) {
				needUpdateState = true
			}
		})
		if err != nil {
			return 0, err
		}
	case valueTypeUint8, valueTypeUint16, valueTypeUint32, valueTypeUint64:
		bb := bbPool.Get()
		bb.B = marshalUint64String(bb.B[:0], c.maxValue)
		needUpdateState = smp.needUpdateStateBytes(bb.B)
		bbPool.Put(bb)
	case valueTypeInt64:
		bb := bbPool.Get()
		bb.B = marshalInt64String(bb.B[:0], int64(c.maxValue))
		needUpdateState = smp.needUpdateStateBytes(bb.B)
		bbPool.Put(bb)
	case valueTypeFloat64:
		f := math.Float64frombits(c.maxValue)
		bb := bbPool.Get()
		bb.B = marshalFloat64String(bb.B[:0], f)
		needUpdateState = smp.needUpdateStateBytes(bb.B)
		bbPool.Put(bb)
	case valueTypeIPv4:
		bb := bbPool.Get()
		bb.B = marshalIPv4String(bb.B[:0], uint32(c.maxValue))
		needUpdateState = smp.needUpdateStateBytes(bb.B)
		bbPool.Put(bb)
	case valueTypeTimestampISO8601:
		bb := bbPool.Get()
		bb.B = marshalTimestampISO8601String(bb.B[:0], int64(c.maxValue))
		needUpdateState = smp.needUpdateStateBytes(bb.B)
		bbPool.Put(bb)
	default:
		logger.Panicf("BUG: unknown valueType=%d", c.valueType)
	}

	if needUpdateState {
		values, err := c.getValues(br)
		if err != nil {
			return 0, err
		}
		for i, v := range values {
			state, err := smp.updateState(sm, v, br, i)
			if err != nil {
				return 0, err
			}
			stateSizeIncrease += state
		}
	}

	return stateSizeIncrease, nil
}

func (smp *statsRowMaxProcessor) updateStatsForRow(sf statsFunc, br *blockResult, rowIdx int) (int, error) {
	sm := sf.(*statsRowMax)
	stateSizeIncrease := 0

	c := br.getColumnByName(sm.srcField)
	if c.isConst {
		v := c.valuesEncoded[0]
		state, err := smp.updateState(sm, v, br, rowIdx)
		if err != nil {
			return 0, err
		}
		stateSizeIncrease += state
		return stateSizeIncrease, nil
	}
	if c.isTime {
		timestamps, err := br.getTimestamps()
		if err != nil {
			return 0, err
		}
		bb := bbPool.Get()
		defer bbPool.Put(bb)
		bb.B = marshalTimestampRFC3339NanoString(bb.B[:0], timestamps[rowIdx])
		v := bytesutil.ToUnsafeString(bb.B)
		state, err := smp.updateState(sm, v, br, rowIdx)
		if err != nil {
			return 0, err
		}
		stateSizeIncrease += state
		return stateSizeIncrease, nil
	}

	v, err := c.getValueAtRow(br, rowIdx)
	if err != nil {
		return 0, err
	}
	state, err := smp.updateState(sm, v, br, rowIdx)
	if err != nil {
		return 0, err
	}
	stateSizeIncrease += state

	return stateSizeIncrease, nil
}

func (smp *statsRowMaxProcessor) mergeState(_ *chunkedAllocator, _ statsFunc, sfp statsProcessor) {
	src := sfp.(*statsRowMaxProcessor)
	if smp.needUpdateStateString(src.max) {
		smp.max = src.max
		smp.fields = src.fields
	}
}

func (smp *statsRowMaxProcessor) exportState(dst []byte, _ <-chan struct{}) []byte {
	dst = encoding.MarshalBytes(dst, bytesutil.ToUnsafeBytes(smp.max))
	dst = marshalFields(dst, smp.fields)
	return dst
}

func (smp *statsRowMaxProcessor) importState(src []byte, _ <-chan struct{}) (int, error) {
	maxValue, n := encoding.UnmarshalBytes(src)
	if n <= 0 {
		return 0, fmt.Errorf("cannot read maxValue")
	}
	src = src[n:]
	smp.max = string(maxValue)

	fields, tail, err := unmarshalFields(nil, src)
	if err != nil {
		return 0, fmt.Errorf("cannot unmarshal fields: %w", err)
	}
	if len(tail) > 0 {
		return 0, fmt.Errorf("unexpected non-empty tail left; len(tail)=%d", len(tail))
	}
	smp.fields = fields

	stateSize := len(smp.max) + fieldsStateSize(smp.fields)

	return stateSize, nil
}

func (smp *statsRowMaxProcessor) needUpdateStateBytes(b []byte) bool {
	v := bytesutil.ToUnsafeString(b)
	return smp.needUpdateStateString(v)
}

func (smp *statsRowMaxProcessor) needUpdateStateString(v string) bool {
	if v == "" {
		return false
	}
	return smp.max == "" || lessString(smp.max, v)
}

func (smp *statsRowMaxProcessor) updateState(sm *statsRowMax, v string, br *blockResult, rowIdx int) (int, error) {
	stateSizeIncrease := 0

	if !smp.needUpdateStateString(v) {
		// There is no need in updating state
		return stateSizeIncrease, nil
	}

	stateSizeIncrease -= len(smp.max)
	stateSizeIncrease += len(v)
	smp.max = strings.Clone(v)

	fields := smp.fields
	for _, f := range fields {
		stateSizeIncrease -= len(f.Name) + len(f.Value)
	}

	clear(fields)
	fields = fields[:0]

	mc := getMatchingColumns(br, sm.fieldFilters)
	defer putMatchingColumns(mc)
	for _, c := range mc.cs {
		v, err := c.getValueAtRow(br, rowIdx)
		if err != nil {
			return 0, err
		}
		fields = append(fields, Field{
			Name:  strings.Clone(c.name),
			Value: strings.Clone(v),
		})
		stateSizeIncrease += len(c.name) + len(v)
	}

	smp.fields = fields

	return stateSizeIncrease, nil
}

func (smp *statsRowMaxProcessor) finalizeStats(_ statsFunc, dst []byte, _ <-chan struct{}) []byte {
	return MarshalFieldsToJSON(dst, smp.fields)
}

func parseStatsRowMax(lex *lexer) (statsFunc, error) {
	fieldFilters, err := parseStatsFuncFieldFilters(lex, "row_max")
	if err != nil {
		return nil, err
	}

	if len(fieldFilters) == 0 {
		return nil, fmt.Errorf("missing source field for 'row_max' func")
	}

	srcField := fieldFilters[0]
	if prefixfilter.IsWildcardFilter(srcField) {
		return nil, fmt.Errorf("the source field %q cannot be wildcard", srcField)
	}

	fieldFilters = fieldFilters[1:]
	if len(fieldFilters) == 0 {
		fieldFilters = []string{"*"}
	}

	sm := &statsRowMax{
		srcField:     srcField,
		fieldFilters: fieldFilters,
	}
	return sm, nil
}
