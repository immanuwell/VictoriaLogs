package logstorage

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/prefixfilter"
)

// filterTime filters by time.
//
// It is expressed as `_time:[start, end]` in LogsQL.
type filterTime struct {
	// mintimestamp is the minimum timestamp in nanoseconds to find
	minTimestamp int64

	// maxTimestamp is the maximum timestamp in nanoseconds to find
	maxTimestamp int64

	// stringRepr is string representation of the filter
	stringRepr string
}

func newFilterTime(minTimestamp, maxTimestamp int64, stringRepr string) *filterTime {
	return &filterTime{
		minTimestamp: minTimestamp,
		maxTimestamp: maxTimestamp,

		stringRepr: stringRepr,
	}
}

func (ft *filterTime) String() string {
	return "_time:" + ft.stringRepr
}

func (ft *filterTime) updateNeededFields(pf *prefixfilter.Filter) {
	pf.AddAllowFilter("_time")
}

func (ft *filterTime) matchRow(fields []Field) bool {
	v := getFieldValueByName(fields, "_time")
	return ft.matchTimestampString(v)
}

func (ft *filterTime) applyToBlockResult(br *blockResult, bm *bitmap) error {
	if ft.minTimestamp > ft.maxTimestamp {
		bm.resetBits()
		return nil
	}

	c := br.getColumnByName("_time")
	if c.isConst {
		v := c.valuesEncoded[0]
		if !ft.matchTimestampString(v) {
			bm.resetBits()
		}
		return nil
	}
	if c.isTime {
		timestamps, err := br.getTimestamps()
		if err != nil {
			return err
		}
		bm.forEachSetBit(func(idx int) bool {
			timestamp := timestamps[idx]
			return ft.matchTimestampValue(timestamp)
		})
		return nil
	}

	switch c.valueType {
	case valueTypeString:
		values, err := c.getValues(br)
		if err != nil {
			return err
		}
		bm.forEachSetBit(func(idx int) bool {
			v := values[idx]
			return ft.matchTimestampString(v)
		})
	case valueTypeDict:
		bb := bbPool.Get()
		defer bbPool.Put(bb)
		for _, v := range c.dictValues {
			c := byte(0)
			if ft.matchTimestampString(v) {
				c = 1
			}
			bb.B = append(bb.B, c)
		}
		valuesEncoded, err := c.getValuesEncoded(br)
		if err != nil {
			return err
		}
		bm.forEachSetBit(func(idx int) bool {
			n := valuesEncoded[idx][0]
			return bb.B[n] == 1
		})
	case valueTypeUint8:
		bm.resetBits()
	case valueTypeUint16:
		bm.resetBits()
	case valueTypeUint32:
		bm.resetBits()
	case valueTypeUint64:
		bm.resetBits()
	case valueTypeInt64:
		bm.resetBits()
	case valueTypeFloat64:
		bm.resetBits()
	case valueTypeIPv4:
		bm.resetBits()
	case valueTypeTimestampISO8601:
		valuesEncoded, err := c.getValuesEncoded(br)
		if err != nil {
			return err
		}
		bm.forEachSetBit(func(idx int) bool {
			v := valuesEncoded[idx]
			timestamp := unmarshalTimestampISO8601(v)
			return ft.matchTimestampValue(timestamp)
		})
	default:
		logger.Panicf("FATAL: unknown valueType=%d", c.valueType)
	}
	return nil
}

func (ft *filterTime) matchTimestampString(v string) bool {
	timestamp, ok := TryParseTimestampRFC3339Nano(v)
	if !ok {
		return false
	}
	return ft.matchTimestampValue(timestamp)
}

func (ft *filterTime) matchTimestampValue(timestamp int64) bool {
	return timestamp >= ft.minTimestamp && timestamp <= ft.maxTimestamp
}

func (ft *filterTime) applyToBlockSearch(bs *blockSearch, bm *bitmap) error {
	minTimestamp := ft.minTimestamp
	maxTimestamp := ft.maxTimestamp

	if minTimestamp > maxTimestamp {
		bm.resetBits()
		return nil
	}

	th := bs.bsw.bh.timestampsHeader
	if minTimestamp > th.maxTimestamp || maxTimestamp < th.minTimestamp {
		bm.resetBits()
		return nil
	}
	if minTimestamp <= th.minTimestamp && maxTimestamp >= th.maxTimestamp {
		return nil
	}

	timestamps, err := bs.getTimestamps()
	if err != nil {
		return err
	}
	bm.forEachSetBit(func(idx int) bool {
		ts := timestamps[idx]
		return ts >= minTimestamp && ts <= maxTimestamp
	})
	return nil
}
