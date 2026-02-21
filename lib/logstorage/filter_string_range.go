package logstorage

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

var maxStringRangeValue = string([]byte{255, 255, 255, 255})

// filterStringRange matches tie given string range [minValue..maxValue)
//
// Note that the minValue is included in the range, while the maxValue isn't included in the range.
// This simplifies querying distinct log sets with string_range(A, B), string_range(B, C), etc.
//
// Example LogsQL: `string_range(minValue, maxValue)`
type filterStringRange struct {
	minValue string
	maxValue string

	stringRepr string
}

func newFilterStringRange(fieldName, minValue, maxValue, stringRepr string) *filterGeneric {
	fr := &filterStringRange{
		minValue: minValue,
		maxValue: maxValue,

		stringRepr: stringRepr,
	}
	return newFilterGeneric(fieldName, fr)
}

func (fr *filterStringRange) String() string {
	return fr.stringRepr
}

func (fr *filterStringRange) matchRowByField(fields []Field, fieldName string) bool {
	v := getFieldValueByName(fields, fieldName)
	return matchStringRange(v, fr.minValue, fr.maxValue)
}

func (fr *filterStringRange) applyToBlockResultByField(br *blockResult, bm *bitmap, fieldName string) error {
	minValue := fr.minValue
	maxValue := fr.maxValue

	if minValue > maxValue {
		bm.resetBits()
		return nil
	}

	return applyToBlockResultGeneric(br, bm, fieldName, "", func(v, _ string) bool {
		return matchStringRange(v, minValue, maxValue)
	})
}

func (fr *filterStringRange) applyToBlockSearchByField(bs *blockSearch, bm *bitmap, fieldName string) error {
	minValue := fr.minValue
	maxValue := fr.maxValue

	if minValue > maxValue {
		bm.resetBits()
		return nil
	}

	v, err := bs.getConstColumnValue(fieldName)
	if err != nil || v != "" {
		if !matchStringRange(v, minValue, maxValue) {
			bm.resetBits()
		}
		return err
	}

	// Verify whether filter matches other columns
	ch, err := bs.getColumnHeader(fieldName)
	if err != nil || ch == nil {
		if !matchStringRange("", minValue, maxValue) {
			bm.resetBits()
		}
		return err
	}

	switch ch.valueType {
	case valueTypeString:
		return matchStringByStringRange(bs, ch, bm, minValue, maxValue)
	case valueTypeDict:
		return matchValuesDictByStringRange(bs, ch, bm, minValue, maxValue)
	case valueTypeUint8:
		return matchUint8ByStringRange(bs, ch, bm, minValue, maxValue)
	case valueTypeUint16:
		return matchUint16ByStringRange(bs, ch, bm, minValue, maxValue)
	case valueTypeUint32:
		return matchUint32ByStringRange(bs, ch, bm, minValue, maxValue)
	case valueTypeUint64:
		return matchUint64ByStringRange(bs, ch, bm, minValue, maxValue)
	case valueTypeInt64:
		return matchInt64ByStringRange(bs, ch, bm, minValue, maxValue)
	case valueTypeFloat64:
		return matchFloat64ByStringRange(bs, ch, bm, minValue, maxValue)
	case valueTypeIPv4:
		return matchIPv4ByStringRange(bs, ch, bm, minValue, maxValue)
	case valueTypeTimestampISO8601:
		return matchTimestampISO8601ByStringRange(bs, ch, bm, minValue, maxValue)
	default:
		logger.Panicf("FATAL: %s: unknown valueType=%d", bs.partPath(), ch.valueType)
	}
	return nil
}

func matchTimestampISO8601ByStringRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minValue, maxValue string) error {
	if minValue > "9" || maxValue < "0" {
		bm.resetBits()
		return nil
	}

	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		s := toTimestampISO8601String(bs, bb, v)
		return matchStringRange(s, minValue, maxValue)
	})
}

func matchIPv4ByStringRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minValue, maxValue string) error {
	if minValue > "9" || maxValue < "0" {
		bm.resetBits()
		return nil
	}

	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		s := toIPv4String(bs, bb, v)
		return matchStringRange(s, minValue, maxValue)
	})
}

func matchFloat64ByStringRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minValue, maxValue string) error {
	if minValue > "9" || maxValue < "+" {
		bm.resetBits()
		return nil
	}

	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		s := toFloat64String(bs, bb, v)
		return matchStringRange(s, minValue, maxValue)
	})
}

func matchValuesDictByStringRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minValue, maxValue string) error {
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	for _, v := range ch.valuesDict.values {
		c := byte(0)
		if matchStringRange(v, minValue, maxValue) {
			c = 1
		}
		bb.B = append(bb.B, c)
	}
	return matchEncodedValuesDict(bs, ch, bm, bb.B)
}

func matchStringByStringRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minValue, maxValue string) error {
	return visitValues(bs, ch, bm, func(v string) bool {
		return matchStringRange(v, minValue, maxValue)
	})
}

func matchUint8ByStringRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minValue, maxValue string) error {
	if minValue > "9" || maxValue < "0" {
		bm.resetBits()
		return nil
	}
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		s := toUint8String(bs, bb, v)
		return matchStringRange(s, minValue, maxValue)
	})
}

func matchUint16ByStringRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minValue, maxValue string) error {
	if minValue > "9" || maxValue < "0" {
		bm.resetBits()
		return nil
	}
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		s := toUint16String(bs, bb, v)
		return matchStringRange(s, minValue, maxValue)
	})
}

func matchUint32ByStringRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minValue, maxValue string) error {
	if minValue > "9" || maxValue < "0" {
		bm.resetBits()
		return nil
	}
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		s := toUint32String(bs, bb, v)
		return matchStringRange(s, minValue, maxValue)
	})
}

func matchUint64ByStringRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minValue, maxValue string) error {
	if minValue > "9" || maxValue < "0" {
		bm.resetBits()
		return nil
	}
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		s := toUint64String(bs, bb, v)
		return matchStringRange(s, minValue, maxValue)
	})
}

func matchInt64ByStringRange(bs *blockSearch, ch *columnHeader, bm *bitmap, minValue, maxValue string) error {
	if minValue != "-" && minValue > "9" || maxValue != "-" && maxValue < "0" {
		bm.resetBits()
		return nil
	}
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		s := toInt64String(bs, bb, v)
		return matchStringRange(s, minValue, maxValue)
	})
}

func matchStringRange(s, minValue, maxValue string) bool {
	// Do not use lessString() here, since string_range() filter
	// works on plain strings without additional magic.
	return s >= minValue && s < maxValue
}
