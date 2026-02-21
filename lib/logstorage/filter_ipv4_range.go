package logstorage

import (
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// filterIPv4Range matches the given ipv4 range [minValue..maxValue].
//
// Example LogsQL: `ipv4_range(127.0.0.1, 127.0.0.255)`
type filterIPv4Range struct {
	minValue uint32
	maxValue uint32
}

func newFilterIPv4Range(fieldName string, minValue, maxValue uint32) *filterGeneric {
	fr := &filterIPv4Range{
		minValue: minValue,
		maxValue: maxValue,
	}
	return newFilterGeneric(fieldName, fr)
}

func (fr *filterIPv4Range) String() string {
	minValue := marshalIPv4String(nil, fr.minValue)
	maxValue := marshalIPv4String(nil, fr.maxValue)
	return fmt.Sprintf("ipv4_range(%s, %s)", minValue, maxValue)
}

func (fr *filterIPv4Range) matchRowByField(fields []Field, fieldName string) bool {
	v := getFieldValueByName(fields, fieldName)
	return matchIPv4Range(v, fr.minValue, fr.maxValue)
}

func (fr *filterIPv4Range) applyToBlockResultByField(br *blockResult, bm *bitmap, fieldName string) error {
	minValue := fr.minValue
	maxValue := fr.maxValue

	if minValue > maxValue {
		bm.resetBits()
		return nil
	}

	c := br.getColumnByName(fieldName)
	if c.isConst {
		v := c.valuesEncoded[0]
		if !matchIPv4Range(v, minValue, maxValue) {
			bm.resetBits()
		}
		return nil
	}
	if c.isTime {
		bm.resetBits()
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
			return matchIPv4Range(v, minValue, maxValue)
		})
	case valueTypeDict:
		bb := bbPool.Get()
		defer bbPool.Put(bb)
		for _, v := range c.dictValues {
			c := byte(0)
			if matchIPv4Range(v, minValue, maxValue) {
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
		valuesEncoded, err := c.getValuesEncoded(br)
		if err != nil {
			return err
		}
		bm.forEachSetBit(func(idx int) bool {
			ip := unmarshalIPv4(valuesEncoded[idx])
			return ip >= minValue && ip <= maxValue
		})
	case valueTypeTimestampISO8601:
		bm.resetBits()
	default:
		logger.Panicf("FATAL: unknown valueType=%d", c.valueType)
	}
	return nil
}

func (fr *filterIPv4Range) applyToBlockSearchByField(bs *blockSearch, bm *bitmap, fieldName string) error {
	minValue := fr.minValue
	maxValue := fr.maxValue

	if minValue > maxValue {
		bm.resetBits()
		return nil
	}

	v, err := bs.getConstColumnValue(fieldName)
	if err != nil || v != "" {
		if !matchIPv4Range(v, minValue, maxValue) {
			bm.resetBits()
		}
		return err
	}

	// Verify whether filter matches other columns
	ch, err := bs.getColumnHeader(fieldName)
	if err != nil || ch == nil {
		// Fast path - there are no matching columns.
		bm.resetBits()
		return err
	}

	switch ch.valueType {
	case valueTypeString:
		return matchStringByIPv4Range(bs, ch, bm, minValue, maxValue)
	case valueTypeDict:
		return matchValuesDictByIPv4Range(bs, ch, bm, minValue, maxValue)
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
		return matchIPv4ByRange(bs, ch, bm, minValue, maxValue)
	case valueTypeTimestampISO8601:
		bm.resetBits()
	default:
		logger.Panicf("FATAL: %s: unknown valueType=%d", bs.partPath(), ch.valueType)
	}
	return nil
}

func matchValuesDictByIPv4Range(bs *blockSearch, ch *columnHeader, bm *bitmap, minValue, maxValue uint32) error {
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	for _, v := range ch.valuesDict.values {
		c := byte(0)
		if matchIPv4Range(v, minValue, maxValue) {
			c = 1
		}
		bb.B = append(bb.B, c)
	}
	return matchEncodedValuesDict(bs, ch, bm, bb.B)
}

func matchStringByIPv4Range(bs *blockSearch, ch *columnHeader, bm *bitmap, minValue, maxValue uint32) error {
	return visitValues(bs, ch, bm, func(v string) bool {
		return matchIPv4Range(v, minValue, maxValue)
	})
}

func matchIPv4Range(s string, minValue, maxValue uint32) bool {
	n, ok := tryParseIPv4(s)
	if !ok {
		return false
	}
	return n >= minValue && n <= maxValue
}

func matchIPv4ByRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minValue, maxValue uint32) error {
	if ch.minValue > uint64(maxValue) || ch.maxValue < uint64(minValue) {
		bm.resetBits()
		return nil
	}

	return visitValues(bs, ch, bm, func(v string) bool {
		if len(v) != 4 {
			logger.Panicf("FATAL: %s: unexpected length for binary representation of IPv4: got %d; want 4", bs.partPath(), len(v))
		}
		n := unmarshalIPv4(v)
		return n >= minValue && n <= maxValue
	})
}
