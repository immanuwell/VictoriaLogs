package logstorage

import (
	"fmt"
	"slices"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// filterIn matches any exact value from the values map.
//
// Example LogsQL: `in("foo", "bar baz")`
type filterIn struct {
	values inValues
}

func newFilterInValues(fieldName string, values []string) *filterGeneric {
	var fi filterIn
	fi.values.values = values
	return newFilterGeneric(fieldName, &fi)
}

func (fi *filterIn) String() string {
	args := fi.values.String()
	return fmt.Sprintf("in(%s)", args)
}

func (fi *filterIn) matchRowByField(fields []Field, fieldName string) bool {
	v := getFieldValueByName(fields, fieldName)
	stringValues := fi.values.getStringValues()
	_, ok := stringValues[v]
	return ok
}

func (fi *filterIn) applyToBlockResultByField(br *blockResult, bm *bitmap, fieldName string) error {
	if fi.values.isEmpty() {
		bm.resetBits()
		return nil
	}

	c := br.getColumnByName(fieldName)
	if c.isConst {
		stringValues := fi.values.getStringValues()
		v := c.valuesEncoded[0]
		if _, ok := stringValues[v]; !ok {
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
		stringValues := fi.values.getStringValues()
		bb := bbPool.Get()
		defer bbPool.Put(bb)
		for _, v := range c.dictValues {
			c := byte(0)
			if _, ok := stringValues[v]; ok {
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
		binValues := fi.values.getInt64Values()
		return matchColumnByBinValues(br, bm, c, binValues)
	case valueTypeFloat64:
		binValues := fi.values.getFloat64Values()
		return matchColumnByBinValues(br, bm, c, binValues)
	case valueTypeIPv4:
		binValues := fi.values.getIPv4Values()
		return matchColumnByBinValues(br, bm, c, binValues)
	case valueTypeTimestampISO8601:
		binValues := fi.values.getTimestampISO8601Values()
		return matchColumnByBinValues(br, bm, c, binValues)
	default:
		logger.Panicf("FATAL: unknown valueType=%d", c.valueType)
	}
	return nil
}

func (fi *filterIn) matchColumnByStringValues(br *blockResult, bm *bitmap, c *blockResultColumn) error {
	stringValues := fi.values.getStringValues()
	values, err := c.getValues(br)
	if err != nil {
		return err
	}
	bm.forEachSetBit(func(idx int) bool {
		v := values[idx]
		_, ok := stringValues[v]
		return ok
	})
	return nil
}

func matchColumnByBinValues(br *blockResult, bm *bitmap, c *blockResultColumn, binValues map[string]struct{}) error {
	if len(binValues) == 0 {
		bm.resetBits()
		return nil
	}
	valuesEncoded, err := c.getValuesEncoded(br)
	if err != nil {
		return err
	}
	bm.forEachSetBit(func(idx int) bool {
		v := valuesEncoded[idx]
		_, ok := binValues[v]
		return ok
	})
	return nil
}

func (fi *filterIn) applyToBlockSearchByField(bs *blockSearch, bm *bitmap, fieldName string) error {
	if fi.values.isEmpty() {
		bm.resetBits()
		return nil
	}

	v, err := bs.getConstColumnValue(fieldName)
	if err != nil || v != "" {
		stringValues := fi.values.getStringValues()
		if _, ok := stringValues[v]; !ok {
			bm.resetBits()
		}
		return err
	}

	// Verify whether filter matches other columns
	ch, err := bs.getColumnHeader(fieldName)
	if err != nil || ch == nil {
		// Fast path - there are no matching columns.
		// It matches anything only for empty phrase.
		stringValues := fi.values.getStringValues()
		if _, ok := stringValues[""]; !ok {
			bm.resetBits()
		}
		return err
	}

	commonTokens, tokenSets := fi.values.getTokensHashesAny()

	switch ch.valueType {
	case valueTypeString:
		stringValues := fi.values.getStringValues()
		return matchAnyValue(bs, ch, bm, stringValues, commonTokens, tokenSets)
	case valueTypeDict:
		stringValues := fi.values.getStringValues()
		return matchValuesDictByAnyValue(bs, ch, bm, stringValues)
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
		binValues := fi.values.getInt64Values()
		return matchAnyValue(bs, ch, bm, binValues, commonTokens, tokenSets)
	case valueTypeFloat64:
		binValues := fi.values.getFloat64Values()
		return matchAnyValue(bs, ch, bm, binValues, commonTokens, tokenSets)
	case valueTypeIPv4:
		binValues := fi.values.getIPv4Values()
		return matchAnyValue(bs, ch, bm, binValues, commonTokens, tokenSets)
	case valueTypeTimestampISO8601:
		binValues := fi.values.getTimestampISO8601Values()
		return matchAnyValue(bs, ch, bm, binValues, commonTokens, tokenSets)
	default:
		logger.Panicf("FATAL: %s: unknown valueType=%d", bs.partPath(), ch.valueType)
	}
	return nil
}

func matchAnyValue(bs *blockSearch, ch *columnHeader, bm *bitmap, binValues map[string]struct{}, commonTokens []uint64, tokenSets [][]uint64) error {
	if len(binValues) == 0 {
		bm.resetBits()
		return nil
	}
	matches, err := matchBloomFilterAnyTokenSet(bs, ch, commonTokens, tokenSets)
	if err != nil || !matches {
		bm.resetBits()
		return err
	}
	return visitValues(bs, ch, bm, func(v string) bool {
		_, ok := binValues[v]
		return ok
	})
}

func matchBloomFilterAnyTokenSet(bs *blockSearch, ch *columnHeader, commonTokens []uint64, tokenSets [][]uint64) (bool, error) {
	matches, err := matchBloomFilterAllTokens(bs, ch, commonTokens)
	if err != nil || !matches {
		return false, nil
	}
	if len(tokenSets) > maxTokenSetsToInit || uint64(len(tokenSets)) > 10*bs.bsw.bh.rowsCount {
		// It is faster to match every row in the block against all the values
		// instead of using bloom filter for too big number of tokenSets.
		return true, nil
	}
	bf, err := bs.getBloomFilterForColumn(ch)
	if err != nil {
		return false, err
	}
	return slices.ContainsFunc(tokenSets, bf.containsAll), nil
}

// It is faster to match every row in the block instead of checking too big number of tokenSets against bloom filter.
const maxTokenSetsToInit = 1000

func matchValuesDictByAnyValue(bs *blockSearch, ch *columnHeader, bm *bitmap, values map[string]struct{}) error {
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	for _, v := range ch.valuesDict.values {
		c := byte(0)
		if _, ok := values[v]; ok {
			c = 1
		}
		bb.B = append(bb.B, c)
	}
	return matchEncodedValuesDict(bs, ch, bm, bb.B)
}
