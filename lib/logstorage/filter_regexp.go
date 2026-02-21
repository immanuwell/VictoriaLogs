package logstorage

import (
	"fmt"
	"sync"
	"unicode/utf8"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/regexutil"
)

// filterRegexp matches the given regexp
//
// Example LogsQL: `re("regexp")`
type filterRegexp struct {
	re *regexutil.Regex

	tokensOnce   sync.Once
	tokens       []string
	tokensHashes []uint64
}

func newFilterRegexp(fieldName string, re *regexutil.Regex) *filterGeneric {
	fp := &filterRegexp{
		re: re,
	}
	return newFilterGeneric(fieldName, fp)
}

func (fr *filterRegexp) String() string {
	return fmt.Sprintf("~%s", quoteTokenIfNeeded(fr.re.String()))
}

func (fr *filterRegexp) getTokens() []string {
	fr.tokensOnce.Do(fr.initTokens)
	return fr.tokens
}

func (fr *filterRegexp) getTokensHashes() []uint64 {
	fr.tokensOnce.Do(fr.initTokens)
	return fr.tokensHashes
}

func (fr *filterRegexp) initTokens() {
	literals := fr.re.GetLiterals()
	for i, literal := range literals {
		literals[i] = skipFirstLastToken(literal)
	}
	fr.tokens = tokenizeStrings(nil, literals)
	fr.tokensHashes = appendTokensHashes(nil, fr.tokens)
}

func skipFirstLastToken(s string) string {
	s = skipFirstToken(s)
	s = skipLastToken(s)
	return s
}

func skipFirstToken(s string) string {
	for {
		r, runeSize := utf8.DecodeRuneInString(s)
		if !isTokenRune(r) {
			return s
		}
		s = s[runeSize:]
	}
}

func skipLastToken(s string) string {
	for {
		r, runeSize := utf8.DecodeLastRuneInString(s)
		if !isTokenRune(r) {
			return s
		}
		s = s[:len(s)-runeSize]
	}
}

func (fr *filterRegexp) matchRowByField(fields []Field, fieldName string) bool {
	v := getFieldValueByName(fields, fieldName)
	return fr.re.MatchString(v)
}

func (fr *filterRegexp) applyToBlockResultByField(br *blockResult, bm *bitmap, fieldName string) error {
	re := fr.re
	return applyToBlockResultGeneric(br, bm, fieldName, "", func(v, _ string) bool {
		return re.MatchString(v)
	})
}

func (fr *filterRegexp) applyToBlockSearchByField(bs *blockSearch, bm *bitmap, fieldName string) error {
	re := fr.re

	// Verify whether filter matches const column
	v, err := bs.getConstColumnValue(fieldName)
	if err != nil || v != "" {
		if !re.MatchString(v) {
			bm.resetBits()
		}
		return err
	}

	// Verify whether filter matches other columns
	ch, err := bs.getColumnHeader(fieldName)
	if err != nil || ch == nil {
		// Fast path - there are no matching columns.
		if !re.MatchString("") {
			bm.resetBits()
		}
		return err
	}

	tokens := fr.getTokensHashes()

	switch ch.valueType {
	case valueTypeString:
		return matchStringByRegexp(bs, ch, bm, re, tokens)
	case valueTypeDict:
		return matchValuesDictByRegexp(bs, ch, bm, re)
	case valueTypeUint8:
		return matchUint8ByRegexp(bs, ch, bm, re, tokens)
	case valueTypeUint16:
		return matchUint16ByRegexp(bs, ch, bm, re, tokens)
	case valueTypeUint32:
		return matchUint32ByRegexp(bs, ch, bm, re, tokens)
	case valueTypeUint64:
		return matchUint64ByRegexp(bs, ch, bm, re, tokens)
	case valueTypeInt64:
		return matchInt64ByRegexp(bs, ch, bm, re, tokens)
	case valueTypeFloat64:
		return matchFloat64ByRegexp(bs, ch, bm, re, tokens)
	case valueTypeIPv4:
		return matchIPv4ByRegexp(bs, ch, bm, re, tokens)
	case valueTypeTimestampISO8601:
		return matchTimestampISO8601ByRegexp(bs, ch, bm, re, tokens)
	default:
		logger.Panicf("FATAL: %s: unknown valueType=%d", bs.partPath(), ch.valueType)
	}
	return nil
}

func matchTimestampISO8601ByRegexp(bs *blockSearch, ch *columnHeader, bm *bitmap, re *regexutil.Regex, tokens []uint64) error {
	matches, err := matchBloomFilterAllTokens(bs, ch, tokens)
	if err != nil || !matches {
		bm.resetBits()
		return err
	}
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		s := toTimestampISO8601String(bs, bb, v)
		return re.MatchString(s)
	})
}

func matchIPv4ByRegexp(bs *blockSearch, ch *columnHeader, bm *bitmap, re *regexutil.Regex, tokens []uint64) error {
	matches, err := matchBloomFilterAllTokens(bs, ch, tokens)
	if err != nil || !matches {
		bm.resetBits()
		return err
	}
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		s := toIPv4String(bs, bb, v)
		return re.MatchString(s)
	})
}

func matchFloat64ByRegexp(bs *blockSearch, ch *columnHeader, bm *bitmap, re *regexutil.Regex, tokens []uint64) error {
	matches, err := matchBloomFilterAllTokens(bs, ch, tokens)
	if err != nil || !matches {
		bm.resetBits()
		return err
	}
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		s := toFloat64String(bs, bb, v)
		return re.MatchString(s)
	})
}

func matchValuesDictByRegexp(bs *blockSearch, ch *columnHeader, bm *bitmap, re *regexutil.Regex) error {
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	for _, v := range ch.valuesDict.values {
		c := byte(0)
		if re.MatchString(v) {
			c = 1
		}
		bb.B = append(bb.B, c)
	}
	return matchEncodedValuesDict(bs, ch, bm, bb.B)
}

func matchStringByRegexp(bs *blockSearch, ch *columnHeader, bm *bitmap, re *regexutil.Regex, tokens []uint64) error {
	matches, err := matchBloomFilterAllTokens(bs, ch, tokens)
	if err != nil || !matches {
		bm.resetBits()
		return err
	}
	return visitValues(bs, ch, bm, func(v string) bool {
		return re.MatchString(v)
	})
}

func matchUint8ByRegexp(bs *blockSearch, ch *columnHeader, bm *bitmap, re *regexutil.Regex, tokens []uint64) error {
	matches, err := matchBloomFilterAllTokens(bs, ch, tokens)
	if err != nil || !matches {
		bm.resetBits()
		return err
	}
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		s := toUint8String(bs, bb, v)
		return re.MatchString(s)
	})
}

func matchUint16ByRegexp(bs *blockSearch, ch *columnHeader, bm *bitmap, re *regexutil.Regex, tokens []uint64) error {
	matches, err := matchBloomFilterAllTokens(bs, ch, tokens)
	if err != nil || !matches {
		bm.resetBits()
		return err
	}
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		s := toUint16String(bs, bb, v)
		return re.MatchString(s)
	})
}

func matchUint32ByRegexp(bs *blockSearch, ch *columnHeader, bm *bitmap, re *regexutil.Regex, tokens []uint64) error {
	matches, err := matchBloomFilterAllTokens(bs, ch, tokens)
	if err != nil || !matches {
		bm.resetBits()
		return err
	}
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		s := toUint32String(bs, bb, v)
		return re.MatchString(s)
	})
}

func matchUint64ByRegexp(bs *blockSearch, ch *columnHeader, bm *bitmap, re *regexutil.Regex, tokens []uint64) error {
	matches, err := matchBloomFilterAllTokens(bs, ch, tokens)
	if err != nil || !matches {
		bm.resetBits()
		return err
	}
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		s := toUint64String(bs, bb, v)
		return re.MatchString(s)
	})
}

func matchInt64ByRegexp(bs *blockSearch, ch *columnHeader, bm *bitmap, re *regexutil.Regex, tokens []uint64) error {
	matches, err := matchBloomFilterAllTokens(bs, ch, tokens)
	if err != nil || !matches {
		bm.resetBits()
		return err
	}
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	return visitValues(bs, ch, bm, func(v string) bool {
		s := toInt64String(bs, bb, v)
		return re.MatchString(s)
	})
}
