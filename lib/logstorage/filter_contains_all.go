package logstorage

import (
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// filterContainsAll matches logs containing all the given values.
//
// Example LogsQL: `contains_all("foo", "bar baz")`
type filterContainsAll struct {
	values inValues
}

func newFilterContainsAllValues(fieldName string, values []string) *filterGeneric {
	var fi filterContainsAll
	fi.values.values = values
	return newFilterGeneric(fieldName, &fi)
}

func (fi *filterContainsAll) String() string {
	args := fi.values.String()
	return fmt.Sprintf("contains_all(%s)", args)
}

func (fi *filterContainsAll) matchRowByField(fields []Field, fieldName string) bool {
	v := getFieldValueByName(fields, fieldName)
	return matchAllPhrases(v, fi.values.values)
}

func (fi *filterContainsAll) applyToBlockResultByField(br *blockResult, bm *bitmap, fieldName string) error {
	if fi.values.isEmpty() || fi.values.isOnlyEmptyValue() {
		return nil
	}

	c := br.getColumnByName(fieldName)
	if c.isConst {
		v := c.valuesEncoded[0]
		if !matchAllPhrases(v, fi.values.values) {
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
			if matchAllPhrases(v, phrases) {
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
		nonEmptyValuesLen := fi.values.getNonEmptyValuesLen()
		return matchColumnByAllBinValues(br, bm, c, binValues, nonEmptyValuesLen)
	case valueTypeUint16:
		binValues := fi.values.getUint16Values()
		nonEmptyValuesLen := fi.values.getNonEmptyValuesLen()
		return matchColumnByAllBinValues(br, bm, c, binValues, nonEmptyValuesLen)
	case valueTypeUint32:
		binValues := fi.values.getUint32Values()
		nonEmptyValuesLen := fi.values.getNonEmptyValuesLen()
		return matchColumnByAllBinValues(br, bm, c, binValues, nonEmptyValuesLen)
	case valueTypeUint64:
		binValues := fi.values.getUint64Values()
		nonEmptyValuesLen := fi.values.getNonEmptyValuesLen()
		return matchColumnByAllBinValues(br, bm, c, binValues, nonEmptyValuesLen)
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

func matchColumnByAllBinValues(br *blockResult, bm *bitmap, c *blockResultColumn, binValues map[string]struct{}, nonEmptyValuesLen int) error {
	if nonEmptyValuesLen == 0 {
		return nil
	}
	if nonEmptyValuesLen != 1 || nonEmptyValuesLen != len(binValues) {
		bm.resetBits()
		return nil
	}
	binValue := ""
	for k := range binValues {
		binValue = k
	}

	valuesEncoded, err := c.getValuesEncoded(br)
	if err != nil {
		return err
	}
	bm.forEachSetBit(func(idx int) bool {
		return valuesEncoded[idx] == binValue
	})
	return nil
}

func (fi *filterContainsAll) matchColumnByStringValues(br *blockResult, bm *bitmap, c *blockResultColumn) error {
	phrases := fi.values.values
	values, err := c.getValues(br)
	if err != nil {
		return err
	}
	bm.forEachSetBit(func(idx int) bool {
		return matchAllPhrases(values[idx], phrases)
	})
	return nil
}

func (fi *filterContainsAll) applyToBlockSearchByField(bs *blockSearch, bm *bitmap, fieldName string) error {
	if fi.values.isEmpty() || fi.values.isOnlyEmptyValue() {
		return nil
	}

	v, err := bs.getConstColumnValue(fieldName)
	if err != nil {
		return err
	}
	if v != "" {
		if !matchAllPhrases(v, fi.values.values) {
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
		if !matchAllPhrases("", fi.values.values) {
			bm.resetBits()
		}
		return nil
	}

	tokens := fi.values.getTokensHashesAll()

	switch ch.valueType {
	case valueTypeString:
		return matchAllPhrasesString(bs, ch, bm, fi.values.values, tokens)
	case valueTypeDict:
		return matchAllPhrasesDict(bs, ch, bm, fi.values.values)
	case valueTypeUint8:
		binValues := fi.values.getUint8Values()
		nonEmptyValuesLen := fi.values.getNonEmptyValuesLen()
		return matchAllValues(bs, ch, bm, binValues, nonEmptyValuesLen, tokens)
	case valueTypeUint16:
		binValues := fi.values.getUint16Values()
		nonEmptyValuesLen := fi.values.getNonEmptyValuesLen()
		return matchAllValues(bs, ch, bm, binValues, nonEmptyValuesLen, tokens)
	case valueTypeUint32:
		binValues := fi.values.getUint32Values()
		nonEmptyValuesLen := fi.values.getNonEmptyValuesLen()
		return matchAllValues(bs, ch, bm, binValues, nonEmptyValuesLen, tokens)
	case valueTypeUint64:
		binValues := fi.values.getUint64Values()
		nonEmptyValuesLen := fi.values.getNonEmptyValuesLen()
		return matchAllValues(bs, ch, bm, binValues, nonEmptyValuesLen, tokens)
	case valueTypeInt64:
		return matchAllPhrasesInt64(bs, ch, bm, fi.values.values, tokens)
	case valueTypeFloat64:
		return matchAllPhrasesFloat64(bs, ch, bm, fi.values.values, tokens)
	case valueTypeIPv4:
		return matchAllPhrasesIPv4(bs, ch, bm, fi.values.values, tokens)
	case valueTypeTimestampISO8601:
		return matchAllPhrasesTimestampISO8601(bs, ch, bm, fi.values.values, tokens)
	default:
		logger.Panicf("FATAL: %s: unknown valueType=%d", bs.partPath(), ch.valueType)
	}
	return nil
}

func matchAllValues(bs *blockSearch, ch *columnHeader, bm *bitmap, binValues map[string]struct{}, nonEmptyValuesLen int, tokens []uint64) error {
	if nonEmptyValuesLen == 0 {
		return nil
	}
	if nonEmptyValuesLen != 1 || nonEmptyValuesLen != len(binValues) {
		bm.resetBits()
		return nil
	}

	matches, err := matchBloomFilterAllTokens(bs, ch, tokens)
	if err != nil || !matches {
		bm.resetBits()
		return err
	}

	binValue := ""
	for k := range binValues {
		binValue = k
	}
	return visitValues(bs, ch, bm, func(v string) bool {
		return v == binValue
	})
}

func matchAllPhrasesString(bs *blockSearch, ch *columnHeader, bm *bitmap, phrases []string, tokens []uint64) error {
	if len(phrases) == 0 {
		return nil
	}
	matches, err := matchBloomFilterAllTokens(bs, ch, tokens)
	if err != nil || !matches {
		bm.resetBits()
		return err
	}

	values, err := bs.getValuesForColumn(ch)
	if err != nil {
		return err
	}
	bm.forEachSetBit(func(idx int) bool {
		return matchAllPhrases(values[idx], phrases)
	})
	return nil
}

func matchAllPhrasesInt64(bs *blockSearch, ch *columnHeader, bm *bitmap, phrases []string, tokens []uint64) error {
	if len(phrases) == 0 {
		return nil
	}
	matches, err := matchBloomFilterAllTokens(bs, ch, tokens)
	if err != nil || !matches {
		bm.resetBits()
		return err
	}

	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		n := unmarshalInt64(v)
		bb.B = marshalInt64String(bb.B[:0], n)
		s := bytesutil.ToUnsafeString(bb.B)
		return matchAllPhrases(s, phrases)
	})
}

func matchAllPhrasesFloat64(bs *blockSearch, ch *columnHeader, bm *bitmap, phrases []string, tokens []uint64) error {
	if len(phrases) == 0 {
		return nil
	}
	matches, err := matchBloomFilterAllTokens(bs, ch, tokens)
	if err != nil || !matches {
		bm.resetBits()
		return err
	}

	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		n := unmarshalFloat64(v)
		bb.B = marshalFloat64String(bb.B[:0], n)
		s := bytesutil.ToUnsafeString(bb.B)
		return matchAllPhrases(s, phrases)
	})
}

func matchAllPhrasesIPv4(bs *blockSearch, ch *columnHeader, bm *bitmap, phrases []string, tokens []uint64) error {
	if len(phrases) == 0 {
		return nil
	}
	matches, err := matchBloomFilterAllTokens(bs, ch, tokens)
	if err != nil || !matches {
		bm.resetBits()
		return err
	}

	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		n := unmarshalIPv4(v)
		bb.B = marshalIPv4String(bb.B[:0], n)
		s := bytesutil.ToUnsafeString(bb.B)
		return matchAllPhrases(s, phrases)
	})
}

func matchAllPhrasesTimestampISO8601(bs *blockSearch, ch *columnHeader, bm *bitmap, phrases []string, tokens []uint64) error {
	if len(phrases) == 0 {
		return nil
	}
	matches, err := matchBloomFilterAllTokens(bs, ch, tokens)
	if err != nil || !matches {
		bm.resetBits()
		return err
	}

	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		n := unmarshalTimestampISO8601(v)
		bb.B = marshalTimestampISO8601String(bb.B[:0], n)
		s := bytesutil.ToUnsafeString(bb.B)
		return matchAllPhrases(s, phrases)
	})
}

func matchAllPhrasesDict(bs *blockSearch, ch *columnHeader, bm *bitmap, phrases []string) error {
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	for _, v := range ch.valuesDict.values {
		c := byte(0)
		if matchAllPhrases(v, phrases) {
			c = 1
		}
		bb.B = append(bb.B, c)
	}
	return matchEncodedValuesDict(bs, ch, bm, bb.B)
}

func matchAllPhrases(v string, phrases []string) bool {
	for _, phrase := range phrases {
		if phrase == "" {
			// Special case - empty phrase matches everything
			continue
		}
		if !matchPhrase(v, phrase) {
			return false
		}
	}
	return true
}
