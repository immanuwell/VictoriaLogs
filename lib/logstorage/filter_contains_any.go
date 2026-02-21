package logstorage

import (
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// filterContainsAny matches any value from the values.
//
// Example LogsQL: `contains_any("foo", "bar baz")`
type filterContainsAny struct {
	values inValues
}

func newFilterContainsAnyValues(fieldName string, values []string) *filterGeneric {
	var fi filterContainsAny
	fi.values.values = values
	return newFilterGeneric(fieldName, &fi)
}

func (fi *filterContainsAny) String() string {
	args := fi.values.String()
	return fmt.Sprintf("contains_any(%s)", args)
}

func (fi *filterContainsAny) matchRowByField(fields []Field, fieldName string) bool {
	v := getFieldValueByName(fields, fieldName)
	return matchAnyPhrase(v, fi.values.values)
}

func (fi *filterContainsAny) applyToBlockResultByField(br *blockResult, bm *bitmap, fieldName string) error {
	if fi.values.isEmpty() {
		bm.resetBits()
		return nil
	}
	if fi.values.hasEmptyValue() {
		// Special case - empty value matches everything
		return nil
	}

	c := br.getColumnByName(fieldName)
	if c.isConst {
		v := c.valuesEncoded[0]
		if !matchAnyPhrase(v, fi.values.values) {
			bm.resetBits()
		}
		return nil
	}
	if c.isTime {
		return fi.matchColumnByStringValues(br, bm, c)
	}

	switch c.valueType {
	case valueTypeString:
		return fi.matchColumnByStringValues(br, bm, c)
	case valueTypeDict:
		phrases := fi.values.values
		bb := bbPool.Get()
		defer bbPool.Put(bb)
		for _, v := range c.dictValues {
			c := byte(0)
			if matchAnyPhrase(v, phrases) {
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
		binValues := fi.values.getUint8Values()
		return matchColumnByBinValues(br, bm, c, binValues)
	case valueTypeUint16:
		binValues := fi.values.getUint16Values()
		return matchColumnByBinValues(br, bm, c, binValues)
	case valueTypeUint32:
		binValues := fi.values.getUint32Values()
		return matchColumnByBinValues(br, bm, c, binValues)
	case valueTypeUint64:
		binValues := fi.values.getUint64Values()
		return matchColumnByBinValues(br, bm, c, binValues)
	case valueTypeInt64:
		return fi.matchColumnByStringValues(br, bm, c)
	case valueTypeFloat64:
		return fi.matchColumnByStringValues(br, bm, c)
	case valueTypeIPv4:
		return fi.matchColumnByStringValues(br, bm, c)
	case valueTypeTimestampISO8601:
		return fi.matchColumnByStringValues(br, bm, c)
	default:
		logger.Panicf("FATAL: unknown valueType=%d", c.valueType)
	}
	return nil
}

func (fi *filterContainsAny) matchColumnByStringValues(br *blockResult, bm *bitmap, c *blockResultColumn) error {
	phrases := fi.values.values
	values, err := c.getValues(br)
	if err != nil {
		return err
	}
	bm.forEachSetBit(func(idx int) bool {
		return matchAnyPhrase(values[idx], phrases)
	})
	return nil
}

func (fi *filterContainsAny) applyToBlockSearchByField(bs *blockSearch, bm *bitmap, fieldName string) error {
	if fi.values.isEmpty() {
		bm.resetBits()
		return nil
	}
	if fi.values.hasEmptyValue() {
		// Special case - empty value matches everything
		return nil
	}

	v, err := bs.getConstColumnValue(fieldName)
	if err != nil {
		return err
	}
	if v != "" {
		if !matchAnyPhrase(v, fi.values.values) {
			bm.resetBits()
		}
		return nil
	}

	// Verify whether filter matches other columns
	ch, err := bs.getColumnHeader(fieldName)
	if err != nil {
		return err
	}
	if ch == nil {
		// Fast path - there are no matching columns.
		// It matches anything only for empty phrase.
		if !matchAnyPhrase("", fi.values.values) {
			bm.resetBits()
		}
		return nil
	}

	commonTokens, tokenSets := fi.values.getTokensHashesAny()

	switch ch.valueType {
	case valueTypeString:
		return matchAnyPhraseString(bs, ch, bm, fi.values.values, commonTokens, tokenSets)
	case valueTypeDict:
		return matchAnyPhraseDict(bs, ch, bm, fi.values.values)
	case valueTypeUint8:
		binValues := fi.values.getUint8Values()
		return matchAnyValue(bs, ch, bm, binValues, commonTokens, tokenSets)
	case valueTypeUint16:
		binValues := fi.values.getUint16Values()
		return matchAnyValue(bs, ch, bm, binValues, commonTokens, tokenSets)
	case valueTypeUint32:
		binValues := fi.values.getUint32Values()
		return matchAnyValue(bs, ch, bm, binValues, commonTokens, tokenSets)
	case valueTypeUint64:
		binValues := fi.values.getUint64Values()
		return matchAnyValue(bs, ch, bm, binValues, commonTokens, tokenSets)
	case valueTypeInt64:
		return matchAnyPhraseInt64(bs, ch, bm, fi.values.values, commonTokens, tokenSets)
	case valueTypeFloat64:
		return matchAnyPhraseFloat64(bs, ch, bm, fi.values.values, commonTokens, tokenSets)
	case valueTypeIPv4:
		return matchAnyPhraseIPv4(bs, ch, bm, fi.values.values, commonTokens, tokenSets)
	case valueTypeTimestampISO8601:
		return matchAnyPhraseTimestampISO8601(bs, ch, bm, fi.values.values, commonTokens, tokenSets)
	default:
		logger.Panicf("FATAL: %s: unknown valueType=%d", bs.partPath(), ch.valueType)
	}
	return nil
}

func matchAnyPhraseString(bs *blockSearch, ch *columnHeader, bm *bitmap, phrases []string, commonTokens []uint64, tokenSets [][]uint64) error {
	if len(phrases) == 0 {
		bm.resetBits()
		return nil
	}
	matches, err := matchBloomFilterAllTokens(bs, ch, commonTokens)
	if err != nil || !matches {
		bm.resetBits()
		return err
	}

	return matchValuesAnyPhrase(bs, ch, bm, phrases, tokenSets, matchAnyPhrase)
}

func matchValuesAnyPhrase(bs *blockSearch, ch *columnHeader, bm *bitmap, phrases []string, tokenSets [][]uint64, matchPhraseFunc func(value string, phrases []string) bool) error {
	bf, err := bs.getBloomFilterForColumn(ch)
	if err != nil {
		return err
	}
	sb := getStringBucket()
	defer putStringBucket(sb)

	for i := range phrases {
		if bf.containsAll(tokenSets[i]) {
			sb.a = append(sb.a, phrases[i])
		}
	}
	if len(sb.a) > 0 {
		values, err := bs.getValuesForColumn(ch)
		if err != nil {
			return err
		}
		bm.forEachSetBit(func(idx int) bool {
			return matchPhraseFunc(values[idx], sb.a)
		})
	} else {
		bm.resetBits()
	}

	return nil
}

func matchAnyPhraseInt64(bs *blockSearch, ch *columnHeader, bm *bitmap, phrases []string, commonTokens []uint64, tokenSets [][]uint64) error {
	if len(phrases) == 0 {
		bm.resetBits()
		return nil
	}
	matches, err := matchBloomFilterAllTokens(bs, ch, commonTokens)
	if err != nil || !matches {
		bm.resetBits()
		return err
	}

	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return matchValuesAnyPhrase(bs, ch, bm, phrases, tokenSets, func(v string, phrases []string) bool {
		n := unmarshalInt64(v)
		bb.B = marshalInt64String(bb.B[:0], n)
		s := bytesutil.ToUnsafeString(bb.B)
		return matchAnyPhrase(s, phrases)
	})
}

func matchAnyPhraseFloat64(bs *blockSearch, ch *columnHeader, bm *bitmap, phrases []string, commonTokens []uint64, tokenSets [][]uint64) error {
	if len(phrases) == 0 {
		bm.resetBits()
		return nil
	}
	matches, err := matchBloomFilterAllTokens(bs, ch, commonTokens)
	if err != nil || !matches {
		bm.resetBits()
		return err
	}

	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return matchValuesAnyPhrase(bs, ch, bm, phrases, tokenSets, func(v string, phrases []string) bool {
		n := unmarshalFloat64(v)
		bb.B = marshalFloat64String(bb.B[:0], n)
		s := bytesutil.ToUnsafeString(bb.B)
		return matchAnyPhrase(s, phrases)
	})
}

func matchAnyPhraseIPv4(bs *blockSearch, ch *columnHeader, bm *bitmap, phrases []string, commonTokens []uint64, tokenSets [][]uint64) error {
	if len(phrases) == 0 {
		bm.resetBits()
		return nil
	}
	matches, err := matchBloomFilterAllTokens(bs, ch, commonTokens)
	if err != nil || !matches {
		bm.resetBits()
		return err
	}

	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return matchValuesAnyPhrase(bs, ch, bm, phrases, tokenSets, func(v string, phrases []string) bool {
		n := unmarshalIPv4(v)
		bb.B = marshalIPv4String(bb.B[:0], n)
		s := bytesutil.ToUnsafeString(bb.B)
		return matchAnyPhrase(s, phrases)
	})
}

func matchAnyPhraseTimestampISO8601(bs *blockSearch, ch *columnHeader, bm *bitmap, phrases []string, commonTokens []uint64, tokenSets [][]uint64) error {
	if len(phrases) == 0 {
		bm.resetBits()
		return nil
	}
	matches, err := matchBloomFilterAllTokens(bs, ch, commonTokens)
	if err != nil || !matches {
		bm.resetBits()
		return err
	}

	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return matchValuesAnyPhrase(bs, ch, bm, phrases, tokenSets, func(v string, phrases []string) bool {
		n := unmarshalTimestampISO8601(v)
		bb.B = marshalTimestampISO8601String(bb.B[:0], n)
		s := bytesutil.ToUnsafeString(bb.B)
		return matchAnyPhrase(s, phrases)
	})
}

func matchAnyPhraseDict(bs *blockSearch, ch *columnHeader, bm *bitmap, phrases []string) error {
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	for _, v := range ch.valuesDict.values {
		c := byte(0)
		if matchAnyPhrase(v, phrases) {
			c = 1
		}
		bb.B = append(bb.B, c)
	}
	return matchEncodedValuesDict(bs, ch, bm, bb.B)
}

func matchAnyPhrase(v string, phrases []string) bool {
	for _, phrase := range phrases {
		if matchPhrase(v, phrase) {
			return true
		}
	}
	return false
}
