package logstorage

import (
	"fmt"
	"math"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// filterExact matches the exact value.
//
// Example LogsQL: `exact("foo bar")` of `="foo bar"
type filterExact struct {
	value string

	tokensOnce   sync.Once
	tokens       []string
	tokensHashes []uint64
}

func newFilterExact(fieldName, value string) *filterGeneric {
	fe := &filterExact{
		value: value,
	}
	return newFilterGeneric(fieldName, fe)
}

func (fe *filterExact) String() string {
	return fmt.Sprintf("=%s", quoteTokenIfNeeded(fe.value))
}

func (fe *filterExact) getTokens() []string {
	fe.tokensOnce.Do(fe.initTokens)
	return fe.tokens
}

func (fe *filterExact) getTokensHashes() []uint64 {
	fe.tokensOnce.Do(fe.initTokens)
	return fe.tokensHashes
}

func (fe *filterExact) initTokens() {
	fe.tokens = tokenizeStrings(nil, []string{fe.value})
	fe.tokensHashes = appendTokensHashes(nil, fe.tokens)
}

func (fe *filterExact) matchRowByField(fields []Field, fieldName string) bool {
	v := getFieldValueByName(fields, fieldName)
	return v == fe.value
}

func (fe *filterExact) applyToBlockResultByField(br *blockResult, bm *bitmap, fieldName string) error {
	value := fe.value

	c := br.getColumnByName(fieldName)
	if c.isConst {
		v := c.valuesEncoded[0]
		if v != value {
			bm.resetBits()
		}
		return nil
	}
	if c.isTime {
		return matchColumnByExactValue(br, bm, c, value)
	}

	switch c.valueType {
	case valueTypeString:
		return matchColumnByExactValue(br, bm, c, value)
	case valueTypeDict:
		bb := bbPool.Get()
		defer bbPool.Put(bb)
		for _, v := range c.dictValues {
			c := byte(0)
			if v == value {
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
		n, ok := tryParseUint64(value)
		if !ok || n >= (1<<8) {
			bm.resetBits()
			return nil
		}
		nNeeded := uint8(n)
		valuesEncoded, err := c.getValuesEncoded(br)
		if err != nil {
			return err
		}
		bm.forEachSetBit(func(idx int) bool {
			n := unmarshalUint8(valuesEncoded[idx])
			return n == nNeeded
		})
	case valueTypeUint16:
		n, ok := tryParseUint64(value)
		if !ok || n >= (1<<16) {
			bm.resetBits()
			return nil
		}
		nNeeded := uint16(n)
		valuesEncoded, err := c.getValuesEncoded(br)
		if err != nil {
			return err
		}
		bm.forEachSetBit(func(idx int) bool {
			n := unmarshalUint16(valuesEncoded[idx])
			return n == nNeeded
		})
	case valueTypeUint32:
		n, ok := tryParseUint64(value)
		if !ok || n >= (1<<32) {
			bm.resetBits()
			return nil
		}
		nNeeded := uint32(n)
		valuesEncoded, err := c.getValuesEncoded(br)
		if err != nil {
			return err
		}
		bm.forEachSetBit(func(idx int) bool {
			n := unmarshalUint32(valuesEncoded[idx])
			return n == nNeeded
		})
	case valueTypeUint64:
		nNeeded, ok := tryParseUint64(value)
		if !ok {
			bm.resetBits()
			return nil
		}
		valuesEncoded, err := c.getValuesEncoded(br)
		if err != nil {
			return err
		}
		bm.forEachSetBit(func(idx int) bool {
			n := unmarshalUint64(valuesEncoded[idx])
			return n == nNeeded
		})
	case valueTypeInt64:
		nNeeded, ok := tryParseInt64(value)
		if !ok {
			bm.resetBits()
			return nil
		}
		valuesEncoded, err := c.getValuesEncoded(br)
		if err != nil {
			return err
		}
		bm.forEachSetBit(func(idx int) bool {
			n := unmarshalInt64(valuesEncoded[idx])
			return n == nNeeded
		})
	case valueTypeFloat64:
		fNeeded, ok := tryParseFloat64Exact(value)
		if !ok {
			bm.resetBits()
			return nil
		}
		valuesEncoded, err := c.getValuesEncoded(br)
		if err != nil {
			return err
		}
		bm.forEachSetBit(func(idx int) bool {
			f := unmarshalFloat64(valuesEncoded[idx])
			return f == fNeeded
		})
	case valueTypeIPv4:
		ipNeeded, ok := tryParseIPv4(value)
		if !ok {
			bm.resetBits()
			return nil
		}
		valuesEncoded, err := c.getValuesEncoded(br)
		if err != nil {
			return err
		}
		bm.forEachSetBit(func(idx int) bool {
			ip := unmarshalIPv4(valuesEncoded[idx])
			return ip == ipNeeded
		})
	case valueTypeTimestampISO8601:
		timestampNeeded, ok := tryParseTimestampISO8601(value)
		if !ok {
			bm.resetBits()
			return nil
		}
		valuesEncoded, err := c.getValuesEncoded(br)
		if err != nil {
			return err
		}
		bm.forEachSetBit(func(idx int) bool {
			timestamp := unmarshalTimestampISO8601(valuesEncoded[idx])
			return timestamp == timestampNeeded
		})
	default:
		logger.Panicf("FATAL: unknown valueType=%d", c.valueType)
	}
	return nil
}

func matchColumnByExactValue(br *blockResult, bm *bitmap, c *blockResultColumn, value string) error {
	values, err := c.getValues(br)
	if err != nil {
		return err
	}
	bm.forEachSetBit(func(idx int) bool {
		return values[idx] == value
	})
	return nil
}

func (fe *filterExact) applyToBlockSearchByField(bs *blockSearch, bm *bitmap, fieldName string) error {
	value := fe.value

	v, err := bs.getConstColumnValue(fieldName)
	if err != nil || v != "" {
		if value != v {
			bm.resetBits()
		}
		return err
	}

	// Verify whether filter matches other columns
	ch, err := bs.getColumnHeader(fieldName)
	if err != nil || ch == nil {
		// Fast path - there are no matching columns.
		// It matches anything only for empty value.
		if value != "" {
			bm.resetBits()
		}
		return err
	}

	tokens := fe.getTokensHashes()

	switch ch.valueType {
	case valueTypeString:
		return matchStringByExactValue(bs, ch, bm, value, tokens)
	case valueTypeDict:
		return matchValuesDictByExactValue(bs, ch, bm, value)
	case valueTypeUint8:
		return matchUint8ByExactValue(bs, ch, bm, value, tokens)
	case valueTypeUint16:
		return matchUint16ByExactValue(bs, ch, bm, value, tokens)
	case valueTypeUint32:
		return matchUint32ByExactValue(bs, ch, bm, value, tokens)
	case valueTypeUint64:
		return matchUint64ByExactValue(bs, ch, bm, value, tokens)
	case valueTypeInt64:
		return matchInt64ByExactValue(bs, ch, bm, value, tokens)
	case valueTypeFloat64:
		return matchFloat64ByExactValue(bs, ch, bm, value, tokens)
	case valueTypeIPv4:
		return matchIPv4ByExactValue(bs, ch, bm, value, tokens)
	case valueTypeTimestampISO8601:
		return matchTimestampISO8601ByExactValue(bs, ch, bm, value, tokens)
	default:
		logger.Panicf("FATAL: %s: unknown valueType=%d", bs.partPath(), ch.valueType)
	}
	return nil
}

func matchTimestampISO8601ByExactValue(bs *blockSearch, ch *columnHeader, bm *bitmap, value string, tokens []uint64) error {
	n, ok := tryParseTimestampISO8601(value)
	if !ok || n < int64(ch.minValue) || n > int64(ch.maxValue) {
		bm.resetBits()
		return nil
	}
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	bb.B = encoding.MarshalUint64(bb.B, uint64(n))
	return matchBinaryValue(bs, ch, bm, bb.B, tokens)
}

func matchIPv4ByExactValue(bs *blockSearch, ch *columnHeader, bm *bitmap, value string, tokens []uint64) error {
	n, ok := tryParseIPv4(value)
	if !ok || uint64(n) < ch.minValue || uint64(n) > ch.maxValue {
		bm.resetBits()
		return nil
	}
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	bb.B = encoding.MarshalUint32(bb.B, n)
	return matchBinaryValue(bs, ch, bm, bb.B, tokens)
}

func matchFloat64ByExactValue(bs *blockSearch, ch *columnHeader, bm *bitmap, value string, tokens []uint64) error {
	f, ok := tryParseFloat64Exact(value)
	if !ok || f < math.Float64frombits(ch.minValue) || f > math.Float64frombits(ch.maxValue) {
		bm.resetBits()
		return nil
	}
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	bb.B = marshalFloat64(bb.B, f)
	return matchBinaryValue(bs, ch, bm, bb.B, tokens)
}

func matchValuesDictByExactValue(bs *blockSearch, ch *columnHeader, bm *bitmap, value string) error {
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	for _, v := range ch.valuesDict.values {
		c := byte(0)
		if v == value {
			c = 1
		}
		bb.B = append(bb.B, c)
	}
	return matchEncodedValuesDict(bs, ch, bm, bb.B)
}

func matchStringByExactValue(bs *blockSearch, ch *columnHeader, bm *bitmap, value string, tokens []uint64) error {
	matches, err := matchBloomFilterAllTokens(bs, ch, tokens)
	if err != nil || !matches {
		bm.resetBits()
		return err
	}
	return visitValues(bs, ch, bm, func(v string) bool {
		return v == value
	})
}

func matchUint8ByExactValue(bs *blockSearch, ch *columnHeader, bm *bitmap, value string, tokens []uint64) error {
	n, ok := tryParseUint64(value)
	if !ok || n < ch.minValue || n > ch.maxValue {
		bm.resetBits()
		return nil
	}
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	bb.B = append(bb.B, byte(n))
	return matchBinaryValue(bs, ch, bm, bb.B, tokens)
}

func matchUint16ByExactValue(bs *blockSearch, ch *columnHeader, bm *bitmap, value string, tokens []uint64) error {
	n, ok := tryParseUint64(value)
	if !ok || n < ch.minValue || n > ch.maxValue {
		bm.resetBits()
		return nil
	}
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	bb.B = encoding.MarshalUint16(bb.B, uint16(n))
	return matchBinaryValue(bs, ch, bm, bb.B, tokens)
}

func matchUint32ByExactValue(bs *blockSearch, ch *columnHeader, bm *bitmap, value string, tokens []uint64) error {
	n, ok := tryParseUint64(value)
	if !ok || n < ch.minValue || n > ch.maxValue {
		bm.resetBits()
		return nil
	}
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	bb.B = encoding.MarshalUint32(bb.B, uint32(n))
	return matchBinaryValue(bs, ch, bm, bb.B, tokens)
}

func matchUint64ByExactValue(bs *blockSearch, ch *columnHeader, bm *bitmap, value string, tokens []uint64) error {
	n, ok := tryParseUint64(value)
	if !ok || n < ch.minValue || n > ch.maxValue {
		bm.resetBits()
		return nil
	}
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	bb.B = encoding.MarshalUint64(bb.B, n)
	return matchBinaryValue(bs, ch, bm, bb.B, tokens)
}

func matchInt64ByExactValue(bs *blockSearch, ch *columnHeader, bm *bitmap, value string, tokens []uint64) error {
	n, ok := tryParseInt64(value)
	if !ok || n < int64(ch.minValue) || n > int64(ch.maxValue) {
		bm.resetBits()
		return nil
	}
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	bb.B = encoding.MarshalInt64(bb.B, n)
	return matchBinaryValue(bs, ch, bm, bb.B, tokens)
}

func matchBinaryValue(bs *blockSearch, ch *columnHeader, bm *bitmap, binValue []byte, tokens []uint64) error {
	matches, err := matchBloomFilterAllTokens(bs, ch, tokens)
	if err != nil || !matches {
		bm.resetBits()
		return err
	}
	return visitValues(bs, ch, bm, func(v string) bool {
		return v == string(binValue)
	})
}
