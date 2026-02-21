package logstorage

import (
	"bytes"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// filterPhrase filters field entries by phrase match (aka full text search).
//
// A phrase consists of any number of words with delimiters between them.
//
// An empty phrase matches only an empty string.
// A single-word phrase is the simplest LogsQL query: `word`
//
// Multi-word phrase is expressed as `"word1 ... wordN"` in LogsQL.
//
// A special case `""` matches any log entry without the given `fieldName` field.
type filterPhrase struct {
	phrase string

	tokensOnce   sync.Once
	tokens       []string
	tokensHashes []uint64
}

func newFilterPhrase(fieldName, phrase string) *filterGeneric {
	fp := &filterPhrase{
		phrase: phrase,
	}
	return newFilterGeneric(fieldName, fp)
}

func (fp *filterPhrase) String() string {
	return quoteTokenIfNeeded(fp.phrase)
}

func (fp *filterPhrase) getTokens() []string {
	fp.tokensOnce.Do(fp.initTokens)
	return fp.tokens
}

func (fp *filterPhrase) getTokensHashes() []uint64 {
	fp.tokensOnce.Do(fp.initTokens)
	return fp.tokensHashes
}

func (fp *filterPhrase) initTokens() {
	fp.tokens = tokenizeStrings(nil, []string{fp.phrase})
	fp.tokensHashes = appendTokensHashes(nil, fp.tokens)
}

func (fp *filterPhrase) matchRowByField(fields []Field, fieldName string) bool {
	v := getFieldValueByName(fields, fieldName)
	return matchPhrase(v, fp.phrase)
}

func (fp *filterPhrase) applyToBlockResultByField(br *blockResult, bm *bitmap, fieldName string) error {
	return applyToBlockResultGeneric(br, bm, fieldName, fp.phrase, matchPhrase)
}

func (fp *filterPhrase) applyToBlockSearchByField(bs *blockSearch, bm *bitmap, fieldName string) error {
	phrase := fp.phrase

	// Verify whether fp matches const column
	v, err := bs.getConstColumnValue(fieldName)
	if err != nil || v != "" {
		if !matchPhrase(v, phrase) {
			bm.resetBits()
		}
		return err
	}

	// Verify whether fp matches other columns
	ch, err := bs.getColumnHeader(fieldName)
	if err != nil || ch == nil {
		// Fast path - there are no matching columns.
		// It matches anything only for empty phrase.
		if len(phrase) > 0 {
			bm.resetBits()
		}
		return err
	}

	tokens := fp.getTokensHashes()

	switch ch.valueType {
	case valueTypeString:
		return matchStringByPhrase(bs, ch, bm, phrase, tokens)
	case valueTypeDict:
		return matchValuesDictByPhrase(bs, ch, bm, phrase)
	case valueTypeUint8:
		return matchUint8ByExactValue(bs, ch, bm, phrase, tokens)
	case valueTypeUint16:
		return matchUint16ByExactValue(bs, ch, bm, phrase, tokens)
	case valueTypeUint32:
		return matchUint32ByExactValue(bs, ch, bm, phrase, tokens)
	case valueTypeUint64:
		return matchUint64ByExactValue(bs, ch, bm, phrase, tokens)
	case valueTypeInt64:
		return matchInt64ByExactValue(bs, ch, bm, phrase, tokens)
	case valueTypeFloat64:
		return matchFloat64ByPhrase(bs, ch, bm, phrase, tokens)
	case valueTypeIPv4:
		return matchIPv4ByPhrase(bs, ch, bm, phrase, tokens)
	case valueTypeTimestampISO8601:
		return matchTimestampISO8601ByPhrase(bs, ch, bm, phrase, tokens)
	default:
		logger.Panicf("FATAL: %s: unknown valueType=%d", bs.partPath(), ch.valueType)
	}
	return nil
}

func matchTimestampISO8601ByPhrase(bs *blockSearch, ch *columnHeader, bm *bitmap, phrase string, tokens []uint64) error {
	_, ok := tryParseTimestampISO8601(phrase)
	if ok {
		// Fast path - the phrase contains complete timestamp, so we can use exact search
		return matchTimestampISO8601ByExactValue(bs, ch, bm, phrase, tokens)
	}

	// Slow path - the phrase contains incomplete timestamp. Search over string representation of the timestamp.
	matches, err := matchBloomFilterAllTokens(bs, ch, tokens)
	if err != nil || !matches {
		bm.resetBits()
		return nil
	}

	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		s := toTimestampISO8601String(bs, bb, v)
		return matchPhrase(s, phrase)
	})
}

func matchIPv4ByPhrase(bs *blockSearch, ch *columnHeader, bm *bitmap, phrase string, tokens []uint64) error {
	_, ok := tryParseIPv4(phrase)
	if ok {
		// Fast path - phrase contains the full IP address, so we can use exact matching
		return matchIPv4ByExactValue(bs, ch, bm, phrase, tokens)
	}

	// Slow path - the phrase may contain a part of IP address. For example, `1.23` should match `1.23.4.5` and `4.1.23.54`.
	// We cannot compare binary representation of ip address and need converting
	// the ip to string before searching for prefix there.
	matches, err := matchBloomFilterAllTokens(bs, ch, tokens)
	if err != nil || !matches {
		bm.resetBits()
		return err
	}

	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		s := toIPv4String(bs, bb, v)
		return matchPhrase(s, phrase)
	})
}

func matchFloat64ByPhrase(bs *blockSearch, ch *columnHeader, bm *bitmap, phrase string, tokens []uint64) error {
	// The phrase may contain a part of the floating-point number.
	// For example, `foo:"123"` must match `123`, `123.456` and `-0.123`.
	// This means we cannot search in binary representation of floating-point numbers.
	// Instead, we need searching for the whole phrase in string representation
	// of floating-point numbers :(
	_, ok := tryParseFloat64Exact(phrase)
	if !ok && phrase != "." && phrase != "+" && phrase != "-" {
		bm.resetBits()
		return nil
	}
	if n := strings.IndexByte(phrase, '.'); n > 0 && n < len(phrase)-1 {
		// Fast path - the phrase contains the exact floating-point number, so we can use exact search
		return matchFloat64ByExactValue(bs, ch, bm, phrase, tokens)
	}
	matches, err := matchBloomFilterAllTokens(bs, ch, tokens)
	if err != nil || !matches {
		bm.resetBits()
		return err
	}

	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		s := toFloat64String(bs, bb, v)
		return matchPhrase(s, phrase)
	})
}

func matchValuesDictByPhrase(bs *blockSearch, ch *columnHeader, bm *bitmap, phrase string) error {
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	for _, v := range ch.valuesDict.values {
		c := byte(0)
		if matchPhrase(v, phrase) {
			c = 1
		}
		bb.B = append(bb.B, c)
	}
	return matchEncodedValuesDict(bs, ch, bm, bb.B)
}

func matchStringByPhrase(bs *blockSearch, ch *columnHeader, bm *bitmap, phrase string, tokens []uint64) error {
	matches, err := matchBloomFilterAllTokens(bs, ch, tokens)
	if err != nil || !matches {
		bm.resetBits()
		return err
	}
	return visitValues(bs, ch, bm, func(v string) bool {
		return matchPhrase(v, phrase)
	})
}

func matchPhrase(s, phrase string) bool {
	if len(phrase) == 0 {
		// Special case - empty phrase matches only empty string.
		return len(s) == 0
	}
	n := getPhrasePos(s, phrase)
	return n >= 0
}

func getPhrasePos(s, phrase string) int {
	if len(phrase) == 0 {
		return 0
	}
	if len(phrase) > len(s) {
		return -1
	}

	r := rune(phrase[0])
	if r >= utf8.RuneSelf {
		r, _ = utf8.DecodeRuneInString(phrase)
	}
	startsWithToken := isTokenRune(r)

	r = rune(phrase[len(phrase)-1])
	if r >= utf8.RuneSelf {
		r, _ = utf8.DecodeLastRuneInString(phrase)
	}
	endsWithToken := isTokenRune(r)

	pos := 0
	for {
		n := strings.Index(s[pos:], phrase)
		if n < 0 {
			return -1
		}
		pos += n
		// Make sure that the found phrase contains non-token chars at the beginning and at the end
		if startsWithToken && pos > 0 {
			r := rune(s[pos-1])
			if r >= utf8.RuneSelf {
				r, _ = utf8.DecodeLastRuneInString(s[:pos])
			}
			if r == utf8.RuneError || isTokenRune(r) {
				pos++
				continue
			}
		}
		if endsWithToken && pos+len(phrase) < len(s) {
			r := rune(s[pos+len(phrase)])
			if r >= utf8.RuneSelf {
				r, _ = utf8.DecodeRuneInString(s[pos+len(phrase):])
			}
			if r == utf8.RuneError || isTokenRune(r) {
				pos++
				continue
			}
		}
		return pos
	}
}

func matchEncodedValuesDict(bs *blockSearch, ch *columnHeader, bm *bitmap, encodedValues []byte) error {
	if bytes.IndexByte(encodedValues, 1) < 0 {
		// Fast path - the phrase is missing in the valuesDict
		bm.resetBits()
		return nil
	}
	// Slow path - iterate over values
	return visitValues(bs, ch, bm, func(v string) bool {
		if len(v) != 1 {
			logger.Panicf("FATAL: %s: unexpected length for dict value: got %d; want 1", bs.partPath(), len(v))
		}
		idx := v[0]
		if int(idx) >= len(encodedValues) {
			logger.Panicf("FATAL: %s: too big index for dict value; got %d; must be smaller than %d", bs.partPath(), idx, len(encodedValues))
		}
		return encodedValues[idx] == 1
	})
}

func visitValues(bs *blockSearch, ch *columnHeader, bm *bitmap, f func(value string) bool) error {
	if bm.isZero() {
		// Fast path - nothing to visit
		return nil
	}
	values, err := bs.getValuesForColumn(ch)
	if err != nil {
		return err
	}
	bm.forEachSetBit(func(idx int) bool {
		return f(values[idx])
	})
	return nil
}

func matchBloomFilterAllTokens(bs *blockSearch, ch *columnHeader, tokens []uint64) (bool, error) {
	if len(tokens) == 0 {
		return true, nil
	}
	bf, err := bs.getBloomFilterForColumn(ch)
	if err != nil {
		return false, err
	}
	return bf.containsAll(tokens), nil
}

func toFloat64String(bs *blockSearch, bb *bytesutil.ByteBuffer, v string) string {
	if len(v) != 8 {
		logger.Panicf("FATAL: %s: unexpected length for binary representation of floating-point number: got %d; want 8", bs.partPath(), len(v))
	}
	f := unmarshalFloat64(v)
	bb.B = marshalFloat64String(bb.B[:0], f)
	return bytesutil.ToUnsafeString(bb.B)
}

func toIPv4String(bs *blockSearch, bb *bytesutil.ByteBuffer, v string) string {
	if len(v) != 4 {
		logger.Panicf("FATAL: %s: unexpected length for binary representation of IPv4: got %d; want 4", bs.partPath(), len(v))
	}
	ip := unmarshalIPv4(v)
	bb.B = marshalIPv4String(bb.B[:0], ip)
	return bytesutil.ToUnsafeString(bb.B)
}

func toTimestampISO8601String(bs *blockSearch, bb *bytesutil.ByteBuffer, v string) string {
	if len(v) != 8 {
		logger.Panicf("FATAL: %s: unexpected length for binary representation of ISO8601 timestamp: got %d; want 8", bs.partPath(), len(v))
	}
	timestamp := unmarshalTimestampISO8601(v)
	bb.B = marshalTimestampISO8601String(bb.B[:0], timestamp)
	return bytesutil.ToUnsafeString(bb.B)
}

func applyToBlockResultGeneric(br *blockResult, bm *bitmap, fieldName, phrase string, matchFunc func(v, phrase string) bool) error {
	c := br.getColumnByName(fieldName)
	if c.isConst {
		v := c.valuesEncoded[0]
		if !matchFunc(v, phrase) {
			bm.resetBits()
		}
		return nil
	}
	if c.isTime {
		return matchColumnByPhraseGeneric(br, bm, c, phrase, matchFunc)
	}

	switch c.valueType {
	case valueTypeString:
		return matchColumnByPhraseGeneric(br, bm, c, phrase, matchFunc)
	case valueTypeDict:
		bb := bbPool.Get()
		defer bbPool.Put(bb)
		for _, v := range c.dictValues {
			c := byte(0)
			if matchFunc(v, phrase) {
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
		n, ok := tryParseUint64(phrase)
		if !ok || n >= (1<<8) {
			bm.resetBits()
			return nil
		}
		return matchColumnByPhraseGeneric(br, bm, c, phrase, matchFunc)
	case valueTypeUint16:
		n, ok := tryParseUint64(phrase)
		if !ok || n >= (1<<16) {
			bm.resetBits()
			return nil
		}
		return matchColumnByPhraseGeneric(br, bm, c, phrase, matchFunc)
	case valueTypeUint32:
		n, ok := tryParseUint64(phrase)
		if !ok || n >= (1<<32) {
			bm.resetBits()
			return nil
		}
		return matchColumnByPhraseGeneric(br, bm, c, phrase, matchFunc)
	case valueTypeUint64:
		_, ok := tryParseUint64(phrase)
		if !ok {
			bm.resetBits()
			return nil
		}
		return matchColumnByPhraseGeneric(br, bm, c, phrase, matchFunc)
	case valueTypeInt64:
		_, ok := tryParseInt64(phrase)
		if !ok {
			bm.resetBits()
			return nil
		}
		return matchColumnByPhraseGeneric(br, bm, c, phrase, matchFunc)
	case valueTypeFloat64:
		return matchColumnByPhraseGeneric(br, bm, c, phrase, matchFunc)
	case valueTypeIPv4:
		return matchColumnByPhraseGeneric(br, bm, c, phrase, matchFunc)
	case valueTypeTimestampISO8601:
		return matchColumnByPhraseGeneric(br, bm, c, phrase, matchFunc)
	default:
		logger.Panicf("FATAL: unknown valueType=%d", c.valueType)
	}
	return nil
}

func matchColumnByPhraseGeneric(br *blockResult, bm *bitmap, c *blockResultColumn, phrase string, matchFunc func(v, phrase string) bool) error {
	values, err := c.getValues(br)
	if err != nil {
		return err
	}
	bm.forEachSetBit(func(idx int) bool {
		return matchFunc(values[idx], phrase)
	})
	return nil
}
