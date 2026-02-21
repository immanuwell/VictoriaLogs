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

type statsFieldMax struct {
	srcField string

	fieldName string
}

func (sm *statsFieldMax) String() string {
	s := "field_max(" + quoteTokenIfNeeded(sm.srcField) + ", " + quoteTokenIfNeeded(sm.fieldName) + ")"
	return s
}

func (sm *statsFieldMax) updateNeededFields(pf *prefixfilter.Filter) {
	pf.AddAllowFilter(sm.fieldName)
	pf.AddAllowFilter(sm.srcField)
}

func (sm *statsFieldMax) newStatsProcessor(a *chunkedAllocator) statsProcessor {
	return a.newStatsFieldMaxProcessor()
}

type statsFieldMaxProcessor struct {
	max   string
	value string
}

func (smp *statsFieldMaxProcessor) updateStatsForAllRows(sf statsFunc, br *blockResult) (int, error) {
	sm := sf.(*statsFieldMax)
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

func (smp *statsFieldMaxProcessor) updateStatsForRow(sf statsFunc, br *blockResult, rowIdx int) (int, error) {
	sm := sf.(*statsFieldMax)
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

func (smp *statsFieldMaxProcessor) mergeState(_ *chunkedAllocator, _ statsFunc, sfp statsProcessor) {
	src := sfp.(*statsFieldMaxProcessor)
	if smp.needUpdateStateString(src.max) {
		smp.max = src.max
		smp.value = src.value
	}
}

func (smp *statsFieldMaxProcessor) exportState(dst []byte, _ <-chan struct{}) []byte {
	dst = encoding.MarshalBytes(dst, bytesutil.ToUnsafeBytes(smp.max))
	dst = encoding.MarshalBytes(dst, bytesutil.ToUnsafeBytes(smp.value))
	return dst
}

func (smp *statsFieldMaxProcessor) importState(src []byte, _ <-chan struct{}) (int, error) {
	maxValue, n := encoding.UnmarshalBytes(src)
	if n <= 0 {
		return 0, fmt.Errorf("cannot unmarshal maxValue")
	}
	src = src[n:]
	smp.max = string(maxValue)

	value, n := encoding.UnmarshalBytes(src)
	if n <= 0 {
		return 0, fmt.Errorf("cannot unmarshal value")
	}
	src = src[n:]
	smp.value = string(value)

	if len(src) > 0 {
		return 0, fmt.Errorf("unexpected non-empty tail; len(tail)=%d", len(src))
	}

	stateSize := len(smp.max) + len(smp.value)

	return stateSize, nil
}

func (smp *statsFieldMaxProcessor) needUpdateStateBytes(b []byte) bool {
	v := bytesutil.ToUnsafeString(b)
	return smp.needUpdateStateString(v)
}

func (smp *statsFieldMaxProcessor) needUpdateStateString(v string) bool {
	if v == "" {
		return false
	}
	return smp.max == "" || lessString(smp.max, v)
}

func (smp *statsFieldMaxProcessor) updateState(sm *statsFieldMax, v string, br *blockResult, rowIdx int) (int, error) {
	stateSizeIncrease := 0

	if !smp.needUpdateStateString(v) {
		// There is no need in updating state
		return stateSizeIncrease, nil
	}

	stateSizeIncrease -= len(smp.max)
	stateSizeIncrease += len(v)
	smp.max = strings.Clone(v)

	c := br.getColumnByName(sm.fieldName)
	value, err := c.getValueAtRow(br, rowIdx)
	if err != nil {
		return 0, err
	}
	stateSizeIncrease -= len(smp.value)
	stateSizeIncrease += len(value)
	smp.value = strings.Clone(value)

	return stateSizeIncrease, nil
}

func (smp *statsFieldMaxProcessor) finalizeStats(_ statsFunc, dst []byte, _ <-chan struct{}) []byte {
	return append(dst, smp.value...)
}

func parseStatsFieldMax(lex *lexer) (statsFunc, error) {
	args, err := parseStatsFuncArgs(lex, "field_max")
	if err != nil {
		return nil, err
	}

	if len(args) != 2 {
		return nil, fmt.Errorf("unexpected number of arguments for 'field_max' func; got %d args; want 2; args=%q", len(args), args)
	}

	sm := &statsFieldMax{
		srcField:  args[0],
		fieldName: args[1],
	}
	return sm, nil
}
