package logstorage

import (
	"unicode/utf8"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// filterLenRange matches field values with the length in the given range [minLen, maxLen].
//
// Example LogsQL: `len_range(10, 20)`
type filterLenRange struct {
	minLen uint64
	maxLen uint64

	stringRepr string
}

func newFilterLenRange(fieldName string, minLen, maxLen uint64, stringRepr string) *filterGeneric {
	fr := &filterLenRange{
		minLen: minLen,
		maxLen: maxLen,

		stringRepr: stringRepr,
	}
	return newFilterGeneric(fieldName, fr)
}

func (fr *filterLenRange) String() string {
	return "len_range" + fr.stringRepr
}

func (fr *filterLenRange) matchRowByField(fields []Field, fieldName string) bool {
	v := getFieldValueByName(fields, fieldName)
	return matchLenRange(v, fr.minLen, fr.maxLen)
}

func (fr *filterLenRange) applyToBlockResultByField(br *blockResult, bm *bitmap, fieldName string) error {
	minLen := fr.minLen
	maxLen := fr.maxLen

	if minLen > maxLen {
		bm.resetBits()
		return nil
	}

	c := br.getColumnByName(fieldName)
	if c.isConst {
		v := c.valuesEncoded[0]
		if !matchLenRange(v, minLen, maxLen) {
			bm.resetBits()
		}
		return nil
	}
	if c.isTime {
		if err := matchColumnByLenRange(br, bm, c, minLen, maxLen); err != nil {
			return err
		}
	}

	switch c.valueType {
	case valueTypeString:
		return matchColumnByLenRange(br, bm, c, minLen, maxLen)
	case valueTypeDict:
		bb := bbPool.Get()
		defer bbPool.Put(bb)
		for _, v := range c.dictValues {
			c := byte(0)
			if matchLenRange(v, minLen, maxLen) {
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
		if minLen > 3 || maxLen == 0 {
			bm.resetBits()
			return nil
		}
		return matchColumnByLenRange(br, bm, c, minLen, maxLen)
	case valueTypeUint16:
		if minLen > 5 || maxLen == 0 {
			bm.resetBits()
			return nil
		}
		return matchColumnByLenRange(br, bm, c, minLen, maxLen)
	case valueTypeUint32:
		if minLen > 10 || maxLen == 0 {
			bm.resetBits()
			return nil
		}
		return matchColumnByLenRange(br, bm, c, minLen, maxLen)
	case valueTypeUint64:
		if minLen > 20 || maxLen == 0 {
			bm.resetBits()
			return nil
		}
		return matchColumnByLenRange(br, bm, c, minLen, maxLen)
	case valueTypeInt64:
		if minLen > 21 || maxLen == 0 {
			bm.resetBits()
			return nil
		}
		return matchColumnByLenRange(br, bm, c, minLen, maxLen)
	case valueTypeFloat64:
		if minLen > 24 || maxLen == 0 {
			bm.resetBits()
			return nil
		}
		return matchColumnByLenRange(br, bm, c, minLen, maxLen)
	case valueTypeIPv4:
		if minLen > uint64(len("255.255.255.255")) || maxLen < uint64(len("0.0.0.0")) {
			bm.resetBits()
			return nil
		}
		return matchColumnByLenRange(br, bm, c, minLen, maxLen)
	case valueTypeTimestampISO8601:
		matchTimestampISO8601ByLenRange(bm, minLen, maxLen)
	default:
		logger.Panicf("FATAL: unknown valueType=%d", c.valueType)
	}
	return nil
}

func matchColumnByLenRange(br *blockResult, bm *bitmap, c *blockResultColumn, minLen, maxLen uint64) error {
	values, err := c.getValues(br)
	if err != nil {
		return err
	}
	bm.forEachSetBit(func(idx int) bool {
		v := values[idx]
		return matchLenRange(v, minLen, maxLen)
	})
	return nil
}

func (fr *filterLenRange) applyToBlockSearchByField(bs *blockSearch, bm *bitmap, fieldName string) error {
	minLen := fr.minLen
	maxLen := fr.maxLen

	if minLen > maxLen {
		bm.resetBits()
		return nil
	}

	v, err := bs.getConstColumnValue(fieldName)
	if err != nil || v != "" {
		if !matchLenRange(v, minLen, maxLen) {
			bm.resetBits()
		}
		return err
	}

	// Verify whether filter matches other columns
	ch, err := bs.getColumnHeader(fieldName)
	if err != nil || ch == nil {
		// Fast path - there are no matching columns.
		if !matchLenRange("", minLen, maxLen) {
			bm.resetBits()
		}
		return err
	}

	switch ch.valueType {
	case valueTypeString:
		return matchStringByLenRange(bs, ch, bm, minLen, maxLen)
	case valueTypeDict:
		return matchValuesDictByLenRange(bs, ch, bm, minLen, maxLen)
	case valueTypeUint8:
		return matchUint8ByLenRange(bs, ch, bm, minLen, maxLen)
	case valueTypeUint16:
		return matchUint16ByLenRange(bs, ch, bm, minLen, maxLen)
	case valueTypeUint32:
		return matchUint32ByLenRange(bs, ch, bm, minLen, maxLen)
	case valueTypeUint64:
		return matchUint64ByLenRange(bs, ch, bm, minLen, maxLen)
	case valueTypeInt64:
		return matchInt64ByLenRange(bs, ch, bm, minLen, maxLen)
	case valueTypeFloat64:
		return matchFloat64ByLenRange(bs, ch, bm, minLen, maxLen)
	case valueTypeIPv4:
		return matchIPv4ByLenRange(bs, ch, bm, minLen, maxLen)
	case valueTypeTimestampISO8601:
		matchTimestampISO8601ByLenRange(bm, minLen, maxLen)
	default:
		logger.Panicf("FATAL: %s: unknown valueType=%d", bs.partPath(), ch.valueType)
	}
	return nil
}

func matchTimestampISO8601ByLenRange(bm *bitmap, minLen, maxLen uint64) {
	if minLen > uint64(len(iso8601Timestamp)) || maxLen < uint64(len(iso8601Timestamp)) {
		bm.resetBits()
		return
	}
}

func matchIPv4ByLenRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minLen, maxLen uint64) error {
	if minLen > uint64(len("255.255.255.255")) || maxLen < uint64(len("0.0.0.0")) {
		bm.resetBits()
		return nil
	}

	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		s := toIPv4String(bs, bb, v)
		return matchLenRange(s, minLen, maxLen)
	})
}

func matchFloat64ByLenRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minLen, maxLen uint64) error {
	if minLen > 24 || maxLen == 0 {
		bm.resetBits()
		return nil
	}

	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		s := toFloat64String(bs, bb, v)
		return matchLenRange(s, minLen, maxLen)
	})
}

func matchValuesDictByLenRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minLen, maxLen uint64) error {
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	for _, v := range ch.valuesDict.values {
		c := byte(0)
		if matchLenRange(v, minLen, maxLen) {
			c = 1
		}
		bb.B = append(bb.B, c)
	}
	return matchEncodedValuesDict(bs, ch, bm, bb.B)
}

func matchStringByLenRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minLen, maxLen uint64) error {
	return visitValues(bs, ch, bm, func(v string) bool {
		return matchLenRange(v, minLen, maxLen)
	})
}

func matchUint8ByLenRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minLen, maxLen uint64) error {
	if minLen > 3 || maxLen == 0 {
		bm.resetBits()
		return nil
	}
	if !matchMinMaxValueLen(ch, minLen, maxLen) {
		bm.resetBits()
		return nil
	}

	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		s := toUint8String(bs, bb, v)
		return matchLenRange(s, minLen, maxLen)
	})
}

func matchUint16ByLenRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minLen, maxLen uint64) error {
	if minLen > 5 || maxLen == 0 {
		bm.resetBits()
		return nil
	}
	if !matchMinMaxValueLen(ch, minLen, maxLen) {
		bm.resetBits()
		return nil
	}

	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		s := toUint16String(bs, bb, v)
		return matchLenRange(s, minLen, maxLen)
	})
}

func matchUint32ByLenRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minLen, maxLen uint64) error {
	if minLen > 10 || maxLen == 0 {
		bm.resetBits()
		return nil
	}
	if !matchMinMaxValueLen(ch, minLen, maxLen) {
		bm.resetBits()
		return nil
	}

	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		s := toUint32String(bs, bb, v)
		return matchLenRange(s, minLen, maxLen)
	})
}

func matchUint64ByLenRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minLen, maxLen uint64) error {
	if minLen > 20 || maxLen == 0 {
		bm.resetBits()
		return nil
	}
	if !matchMinMaxValueLen(ch, minLen, maxLen) {
		bm.resetBits()
		return nil
	}

	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		s := toUint64String(bs, bb, v)
		return matchLenRange(s, minLen, maxLen)
	})
}

func matchInt64ByLenRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minLen, maxLen uint64) error {
	if minLen > 21 || maxLen == 0 {
		bm.resetBits()
		return nil
	}

	bb := bbPool.Get()
	defer bbPool.Put(bb)

	bb.B = marshalInt64String(bb.B[:0], int64(ch.minValue))
	maxvLen := len(bb.B)
	bb.B = marshalInt64String(bb.B[:0], int64(ch.maxValue))
	if len(bb.B) > maxvLen {
		maxvLen = len(bb.B)
	}
	if uint64(maxvLen) < minLen {
		bm.resetBits()
		return nil
	}

	return visitValues(bs, ch, bm, func(v string) bool {
		s := toInt64String(bs, bb, v)
		return matchLenRange(s, minLen, maxLen)
	})
}

func matchLenRange(s string, minLen, maxLen uint64) bool {
	sLen := uint64(utf8.RuneCountInString(s))
	return sLen >= minLen && sLen <= maxLen
}

func matchMinMaxValueLen(ch *columnHeader, minLen, maxLen uint64) bool {
	bb := bbPool.Get()
	defer bbPool.Put(bb)

	bb.B = marshalUint64String(bb.B[:0], ch.minValue)
	if maxLen < uint64(len(bb.B)) {
		return false
	}
	bb.B = marshalUint64String(bb.B[:0], ch.maxValue)
	return minLen <= uint64(len(bb.B))
}
