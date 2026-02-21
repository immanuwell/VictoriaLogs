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

type statsFieldMin struct {
	srcField string

	fieldName string
}

func (sm *statsFieldMin) String() string {
	s := "field_min(" + quoteTokenIfNeeded(sm.srcField) + ", " + quoteTokenIfNeeded(sm.fieldName) + ")"
	return s
}

func (sm *statsFieldMin) updateNeededFields(pf *prefixfilter.Filter) {
	pf.AddAllowFilter(sm.fieldName)
	pf.AddAllowFilter(sm.srcField)
}

func (sm *statsFieldMin) newStatsProcessor(a *chunkedAllocator) statsProcessor {
	return a.newStatsFieldMinProcessor()
}

type statsFieldMinProcessor struct {
	min   string
	value string
}

func (smp *statsFieldMinProcessor) updateStatsForAllRows(sf statsFunc, br *blockResult) (int, error) {
	sm := sf.(*statsFieldMin)
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
		timestamp, ok := TryParseTimestampRFC3339Nano(smp.min)
		if !ok {
			timestamp = (1 << 63) - 1
		}
		minTimestamp, err := br.getMinTimestamp(timestamp)
		if err != nil {
			return 0, err
		}
		if minTimestamp >= timestamp {
			return stateSizeIncrease, nil
		}

		bb := bbPool.Get()
		defer bbPool.Put(bb)
		bb.B = marshalTimestampRFC3339NanoString(bb.B[:0], minTimestamp)
		v := bytesutil.ToUnsafeString(bb.B)
		state, err := smp.updateState(sm, v, br, 0)
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
		defer bbPool.Put(bb)
		bb.B = marshalUint64String(bb.B[:0], c.minValue)
		needUpdateState = smp.needUpdateStateBytes(bb.B)
	case valueTypeInt64:
		bb := bbPool.Get()
		defer bbPool.Put(bb)
		bb.B = marshalInt64String(bb.B[:0], int64(c.minValue))
		needUpdateState = smp.needUpdateStateBytes(bb.B)
	case valueTypeFloat64:
		f := math.Float64frombits(c.minValue)
		bb := bbPool.Get()
		defer bbPool.Put(bb)
		bb.B = marshalFloat64String(bb.B[:0], f)
		needUpdateState = smp.needUpdateStateBytes(bb.B)
	case valueTypeIPv4:
		bb := bbPool.Get()
		defer bbPool.Put(bb)
		bb.B = marshalIPv4String(bb.B[:0], uint32(c.minValue))
		needUpdateState = smp.needUpdateStateBytes(bb.B)
	case valueTypeTimestampISO8601:
		bb := bbPool.Get()
		defer bbPool.Put(bb)
		bb.B = marshalTimestampISO8601String(bb.B[:0], int64(c.minValue))
		needUpdateState = smp.needUpdateStateBytes(bb.B)
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

func (smp *statsFieldMinProcessor) updateStatsForRow(sf statsFunc, br *blockResult, rowIdx int) (int, error) {
	sm := sf.(*statsFieldMin)
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
			return 0, nil
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

func (smp *statsFieldMinProcessor) mergeState(_ *chunkedAllocator, _ statsFunc, sfp statsProcessor) {
	src := sfp.(*statsFieldMinProcessor)
	if smp.needUpdateStateString(src.min) {
		smp.min = src.min
		smp.value = src.value
	}
}

func (smp *statsFieldMinProcessor) exportState(dst []byte, _ <-chan struct{}) []byte {
	dst = encoding.MarshalBytes(dst, bytesutil.ToUnsafeBytes(smp.min))
	dst = encoding.MarshalBytes(dst, bytesutil.ToUnsafeBytes(smp.value))
	return dst
}

func (smp *statsFieldMinProcessor) importState(src []byte, _ <-chan struct{}) (int, error) {
	minValue, n := encoding.UnmarshalBytes(src)
	if n <= 0 {
		return 0, fmt.Errorf("cannot unmarshal minValue")
	}
	src = src[n:]
	smp.min = string(minValue)

	value, n := encoding.UnmarshalBytes(src)
	if n <= 0 {
		return 0, fmt.Errorf("cannot unmarshal value")
	}
	src = src[n:]
	smp.value = string(value)

	if len(src) > 0 {
		return 0, fmt.Errorf("unexpected non-empty tail; len(tail)=%d", len(src))
	}

	stateSize := len(smp.min) + len(smp.value)

	return stateSize, nil
}

func (smp *statsFieldMinProcessor) needUpdateStateBytes(b []byte) bool {
	v := bytesutil.ToUnsafeString(b)
	return smp.needUpdateStateString(v)
}

func (smp *statsFieldMinProcessor) needUpdateStateString(v string) bool {
	if v == "" {
		return false
	}
	return smp.min == "" || lessString(v, smp.min)
}

func (smp *statsFieldMinProcessor) updateState(sm *statsFieldMin, v string, br *blockResult, rowIdx int) (int, error) {
	stateSizeIncrease := 0

	if !smp.needUpdateStateString(v) {
		// There is no need in updating state
		return stateSizeIncrease, nil
	}

	stateSizeIncrease -= len(smp.min)
	stateSizeIncrease += len(v)
	smp.min = strings.Clone(v)

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

func (smp *statsFieldMinProcessor) finalizeStats(_ statsFunc, dst []byte, _ <-chan struct{}) []byte {
	return append(dst, smp.value...)
}

func parseStatsFieldMin(lex *lexer) (statsFunc, error) {
	args, err := parseStatsFuncArgs(lex, "field_min")
	if err != nil {
		return nil, err
	}

	if len(args) != 2 {
		return nil, fmt.Errorf("unexpected number of arguments for 'field_min' func; got %d args; want 2; args=%q", len(args), args)
	}

	sm := &statsFieldMin{
		srcField:  args[0],
		fieldName: args[1],
	}
	return sm, nil
}
