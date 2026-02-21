package logstorage

import (
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/prefixfilter"
)

// filterWeekRange filters by week range.
//
// It is expressed as `_time:week_range[start, end] offset d` in LogsQL.
type filterWeekRange struct {
	// startDay is the starting day of the week.
	startDay time.Weekday

	// endDay is the ending day of the week.
	endDay time.Weekday

	// offset is the offset, which must be applied to _time before applying [start, end] filter to it.
	offset int64

	// stringRepr is string representation of the filter.
	stringRepr string
}

func newFilterWeekRange(startDay, endDay time.Weekday, offset int64, stringRepr string) *filterWeekRange {
	return &filterWeekRange{
		startDay:   startDay,
		endDay:     endDay,
		offset:     offset,
		stringRepr: stringRepr,
	}
}

func (fr *filterWeekRange) String() string {
	return "_time:week_range" + fr.stringRepr
}

func (fr *filterWeekRange) updateNeededFields(pf *prefixfilter.Filter) {
	pf.AddAllowFilter("_time")
}

func (fr *filterWeekRange) matchRow(fields []Field) bool {
	v := getFieldValueByName(fields, "_time")
	return fr.matchTimestampString(v)
}

func (fr *filterWeekRange) applyToBlockResult(br *blockResult, bm *bitmap) error {
	if fr.startDay > fr.endDay || fr.startDay > time.Saturday || fr.endDay < time.Monday {
		bm.resetBits()
		return nil
	}
	if fr.startDay <= time.Sunday && fr.endDay >= time.Saturday {
		return nil
	}

	c := br.getColumnByName("_time")
	if c.isConst {
		v := c.valuesEncoded[0]
		if !fr.matchTimestampString(v) {
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
			return fr.matchTimestampValue(timestamp)
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
			return fr.matchTimestampString(v)
		})
	case valueTypeDict:
		bb := bbPool.Get()
		defer bbPool.Put(bb)
		for _, v := range c.dictValues {
			c := byte(0)
			if fr.matchTimestampString(v) {
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
			return fr.matchTimestampValue(timestamp)
		})
	default:
		logger.Panicf("FATAL: unknown valueType=%d", c.valueType)
	}
	return nil
}

func (fr *filterWeekRange) matchTimestampString(v string) bool {
	timestamp, ok := TryParseTimestampRFC3339Nano(v)
	if !ok {
		return false
	}
	return fr.matchTimestampValue(timestamp)
}

func (fr *filterWeekRange) matchTimestampValue(timestamp int64) bool {
	d := fr.weekday(timestamp)
	return d >= fr.startDay && d <= fr.endDay
}

func (fr *filterWeekRange) weekday(timestamp int64) time.Weekday {
	timestamp = SubInt64NoOverflow(timestamp, -fr.offset)
	return time.Unix(0, timestamp).UTC().Weekday()
}

func (fr *filterWeekRange) applyToBlockSearch(bs *blockSearch, bm *bitmap) error {
	if fr.startDay > fr.endDay {
		bm.resetBits()
		return nil
	}
	if fr.startDay <= time.Sunday && fr.endDay >= time.Saturday {
		return nil
	}

	timestamps, err := bs.getTimestamps()
	if err != nil {
		return err
	}
	bm.forEachSetBit(func(idx int) bool {
		return fr.matchTimestampValue(timestamps[idx])
	})
	return nil
}
