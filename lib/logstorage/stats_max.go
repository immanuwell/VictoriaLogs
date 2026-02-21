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

type statsMax struct {
	fieldFilters []string
}

func (sm *statsMax) String() string {
	return "max(" + fieldNamesString(sm.fieldFilters) + ")"
}

func (sm *statsMax) updateNeededFields(pf *prefixfilter.Filter) {
	pf.AddAllowFilters(sm.fieldFilters)
}

func (sm *statsMax) newStatsProcessor(a *chunkedAllocator) statsProcessor {
	return a.newStatsMaxProcessor()
}

type statsMaxProcessor struct {
	max      string
	hasItems bool
}

func (smp *statsMaxProcessor) updateStatsForAllRows(sf statsFunc, br *blockResult) (int, error) {
	sm := sf.(*statsMax)

	maxLen := len(smp.max)

	mc := getMatchingColumns(br, sm.fieldFilters)
	defer putMatchingColumns(mc)
	for _, c := range mc.cs {
		if err := smp.updateStateForColumn(br, c); err != nil {
			return 0, err
		}
	}

	return len(smp.max) - maxLen, nil
}

func (smp *statsMaxProcessor) updateStatsForRow(sf statsFunc, br *blockResult, rowIdx int) (int, error) {
	sm := sf.(*statsMax)

	maxLen := len(smp.max)

	mc := getMatchingColumns(br, sm.fieldFilters)
	defer putMatchingColumns(mc)
	for _, c := range mc.cs {
		v, err := c.getValueAtRow(br, rowIdx)
		if err != nil {
			return 0, err
		}
		smp.updateStateString(v)
	}

	return maxLen - len(smp.max), nil
}

func (smp *statsMaxProcessor) mergeState(_ *chunkedAllocator, _ statsFunc, sfp statsProcessor) {
	src := sfp.(*statsMaxProcessor)
	if src.hasItems {
		smp.updateStateString(src.max)
	}
}

func (smp *statsMaxProcessor) exportState(dst []byte, _ <-chan struct{}) []byte {
	if !smp.hasItems {
		dst = append(dst, 0)
		return dst
	}

	dst = append(dst, 1)
	dst = encoding.MarshalBytes(dst, bytesutil.ToUnsafeBytes(smp.max))
	return dst
}

func (smp *statsMaxProcessor) importState(src []byte, _ <-chan struct{}) (int, error) {
	if len(src) == 0 {
		return 0, fmt.Errorf("missing `hasItems`")
	}
	smp.hasItems = (src[0] == 1)
	src = src[1:]

	if smp.hasItems {
		maxValue, n := encoding.UnmarshalBytes(src)
		if n <= 0 {
			return 0, fmt.Errorf("cannot unmarshal max value")
		}
		smp.max = string(maxValue)
		src = src[n:]
	} else {
		smp.max = ""
	}

	if len(src) > 0 {
		return 0, fmt.Errorf("unexpected tail left after decoding max value; len(tail)=%d", len(src))
	}

	return len(smp.max), nil
}

func (smp *statsMaxProcessor) updateStateForColumn(br *blockResult, c *blockResultColumn) error {
	if c.isTime {
		timestamp, ok := TryParseTimestampRFC3339Nano(smp.max)
		if !ok {
			timestamp = -1 << 63
		}
		maxTimestamp, err := br.getMaxTimestamp(timestamp)
		if err != nil {
			return err
		}
		if maxTimestamp <= timestamp {
			return nil
		}

		bb := bbPool.Get()
		bb.B = marshalTimestampRFC3339NanoString(bb.B[:0], maxTimestamp)
		smp.updateStateBytes(bb.B)
		bbPool.Put(bb)

		return nil
	}
	if c.isConst {
		// Special case for const column
		v := c.valuesEncoded[0]
		smp.updateStateString(v)
		return nil
	}

	switch c.valueType {
	case valueTypeString:
		valuesEncoded, err := c.getValuesEncoded(br)
		if err != nil {
			return err
		}
		for _, v := range valuesEncoded {
			smp.updateStateString(v)
		}
	case valueTypeDict:
		return c.forEachDictValue(br, func(v string) {
			smp.updateStateString(v)
		})
	case valueTypeUint8, valueTypeUint16, valueTypeUint32, valueTypeUint64:
		bb := bbPool.Get()
		defer bbPool.Put(bb)
		bb.B = marshalUint64String(bb.B[:0], c.maxValue)
		return smp.updateStateWithUpperBound(br, c, bb.B)
	case valueTypeInt64:
		bb := bbPool.Get()
		defer bbPool.Put(bb)
		bb.B = marshalInt64String(bb.B[:0], int64(c.maxValue))
		return smp.updateStateWithUpperBound(br, c, bb.B)
	case valueTypeFloat64:
		f := math.Float64frombits(c.maxValue)
		bb := bbPool.Get()
		defer bbPool.Put(bb)
		bb.B = marshalFloat64String(bb.B[:0], f)
		return smp.updateStateWithUpperBound(br, c, bb.B)
	case valueTypeIPv4:
		bb := bbPool.Get()
		defer bbPool.Put(bb)
		bb.B = marshalIPv4String(bb.B[:0], uint32(c.maxValue))
		return smp.updateStateWithUpperBound(br, c, bb.B)
	case valueTypeTimestampISO8601:
		bb := bbPool.Get()
		defer bbPool.Put(bb)
		bb.B = marshalTimestampISO8601String(bb.B[:0], int64(c.maxValue))
		return smp.updateStateWithUpperBound(br, c, bb.B)
	default:
		logger.Panicf("BUG: unknown valueType=%d", c.valueType)
	}
	return nil
}

func (smp *statsMaxProcessor) updateStateWithUpperBound(br *blockResult, c *blockResultColumn, upperBound []byte) error {
	upperBoundStr := bytesutil.ToUnsafeString(upperBound)
	if !smp.needsUpdateState(upperBoundStr) {
		return nil
	}
	if br.isFull() {
		smp.setState(upperBoundStr)
	} else {
		values, err := c.getValues(br)
		if err != nil {
			return err
		}
		for _, v := range values {
			smp.updateStateString(v)
		}
	}
	return nil
}

func (smp *statsMaxProcessor) updateStateBytes(b []byte) {
	v := bytesutil.ToUnsafeString(b)
	smp.updateStateString(v)
}

func (smp *statsMaxProcessor) updateStateString(v string) {
	if smp.needsUpdateState(v) {
		smp.setState(v)
	}
}

func (smp *statsMaxProcessor) setState(v string) {
	smp.max = strings.Clone(v)
	if !smp.hasItems {
		smp.hasItems = true
	}
}

func (smp *statsMaxProcessor) needsUpdateState(v string) bool {
	return !smp.hasItems || lessString(smp.max, v)
}

func (smp *statsMaxProcessor) finalizeStats(_ statsFunc, dst []byte, _ <-chan struct{}) []byte {
	return append(dst, smp.max...)
}

func parseStatsMax(lex *lexer) (statsFunc, error) {
	fieldFilters, err := parseStatsFuncFieldFilters(lex, "max")
	if err != nil {
		return nil, err
	}
	sm := &statsMax{
		fieldFilters: fieldFilters,
	}
	return sm, nil
}
