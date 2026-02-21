package logstorage

import (
	"fmt"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/stringsutil"
)

// filterAnyCasePhrase filters field entries by case-insensitive phrase match.
//
// An example LogsQL query: `i(word)` or `i("word1 ... wordN")`
type filterAnyCasePhrase struct {
	phrase string

	phraseLowercaseOnce sync.Once
	phraseLowercase     string

	phraseUppercaseOnce sync.Once
	phraseUppercase     string

	tokensOnce            sync.Once
	tokensHashes          []uint64
	tokensHashesUppercase []uint64
}

func newFilterAnyCasePhrase(fieldName, phrase string) *filterGeneric {
	fp := &filterAnyCasePhrase{
		phrase: phrase,
	}
	return newFilterGeneric(fieldName, fp)
}

func (fp *filterAnyCasePhrase) String() string {
	return fmt.Sprintf("i(%s)", quoteTokenIfNeeded(fp.phrase))
}

func (fp *filterAnyCasePhrase) getTokensHashes() []uint64 {
	fp.tokensOnce.Do(fp.initTokens)
	return fp.tokensHashes
}

func (fp *filterAnyCasePhrase) getTokensHashesUppercase() []uint64 {
	fp.tokensOnce.Do(fp.initTokens)
	return fp.tokensHashesUppercase
}

func (fp *filterAnyCasePhrase) initTokens() {
	tokens := tokenizeStrings(nil, []string{fp.phrase})
	fp.tokensHashes = appendTokensHashes(nil, tokens)

	tokensUppercase := make([]string, len(tokens))
	for i, token := range tokens {
		tokensUppercase[i] = strings.ToUpper(token)
	}
	fp.tokensHashesUppercase = appendTokensHashes(nil, tokensUppercase)
}

func (fp *filterAnyCasePhrase) getPhraseLowercase() string {
	fp.phraseLowercaseOnce.Do(fp.initPhraseLowercase)
	return fp.phraseLowercase
}

func (fp *filterAnyCasePhrase) initPhraseLowercase() {
	fp.phraseLowercase = strings.ToLower(fp.phrase)
}

func (fp *filterAnyCasePhrase) getPhraseUppercase() string {
	fp.phraseUppercaseOnce.Do(fp.initPhraseUppercase)
	return fp.phraseUppercase
}

func (fp *filterAnyCasePhrase) initPhraseUppercase() {
	fp.phraseUppercase = strings.ToUpper(fp.phrase)
}

func (fp *filterAnyCasePhrase) matchRowByField(fields []Field, fieldName string) bool {
	v := getFieldValueByName(fields, fieldName)
	phraseLowercase := fp.getPhraseLowercase()
	return matchAnyCasePhrase(v, phraseLowercase)
}

func (fp *filterAnyCasePhrase) applyToBlockResultByField(br *blockResult, bm *bitmap, fieldName string) error {
	phraseLowercase := fp.getPhraseLowercase()
	return applyToBlockResultGeneric(br, bm, fieldName, phraseLowercase, matchAnyCasePhrase)
}

func (fp *filterAnyCasePhrase) applyToBlockSearchByField(bs *blockSearch, bm *bitmap, fieldName string) error {
	phraseLowercase := fp.getPhraseLowercase()

	// Verify whether fp matches const column
	v, err := bs.getConstColumnValue(fieldName)
	if err != nil || v != "" {
		if !matchAnyCasePhrase(v, phraseLowercase) {
			bm.resetBits()
		}
		return err
	}

	// Verify whether fp matches other columns
	ch, err := bs.getColumnHeader(fieldName)
	if err != nil || ch == nil {
		// Fast path - there are no matching columns.
		// It matches anything only for empty phrase.
		if len(phraseLowercase) > 0 {
			bm.resetBits()
		}
		return err
	}

	tokens := fp.getTokensHashes()

	switch ch.valueType {
	case valueTypeString:
		return matchStringByAnyCasePhrase(bs, ch, bm, phraseLowercase)
	case valueTypeDict:
		return matchValuesDictByAnyCasePhrase(bs, ch, bm, phraseLowercase)
	case valueTypeUint8:
		return matchUint8ByExactValue(bs, ch, bm, phraseLowercase, tokens)
	case valueTypeUint16:
		return matchUint16ByExactValue(bs, ch, bm, phraseLowercase, tokens)
	case valueTypeUint32:
		return matchUint32ByExactValue(bs, ch, bm, phraseLowercase, tokens)
	case valueTypeUint64:
		return matchUint64ByExactValue(bs, ch, bm, phraseLowercase, tokens)
	case valueTypeInt64:
		return matchInt64ByExactValue(bs, ch, bm, phraseLowercase, tokens)
	case valueTypeFloat64:
		return matchFloat64ByPhrase(bs, ch, bm, phraseLowercase, tokens)
	case valueTypeIPv4:
		return matchIPv4ByPhrase(bs, ch, bm, phraseLowercase, tokens)
	case valueTypeTimestampISO8601:
		phraseUppercase := fp.getPhraseUppercase()
		tokensUppercase := fp.getTokensHashesUppercase()
		return matchTimestampISO8601ByPhrase(bs, ch, bm, phraseUppercase, tokensUppercase)
	default:
		logger.Panicf("FATAL: %s: unknown valueType=%d", bs.partPath(), ch.valueType)
	}
	return nil
}

func matchValuesDictByAnyCasePhrase(bs *blockSearch, ch *columnHeader, bm *bitmap, phraseLowercase string) error {
	bb := bbPool.Get()
	defer bbPool.Put(bb)
	for _, v := range ch.valuesDict.values {
		c := byte(0)
		if matchAnyCasePhrase(v, phraseLowercase) {
			c = 1
		}
		bb.B = append(bb.B, c)
	}
	return matchEncodedValuesDict(bs, ch, bm, bb.B)
}

func matchStringByAnyCasePhrase(bs *blockSearch, ch *columnHeader, bm *bitmap, phraseLowercase string) error {
	return visitValues(bs, ch, bm, func(v string) bool {
		return matchAnyCasePhrase(v, phraseLowercase)
	})
}

func matchAnyCasePhrase(s, phraseLowercase string) bool {
	if len(phraseLowercase) == 0 {
		// Special case - empty phrase matches only empty string.
		return len(s) == 0
	}
	if len(phraseLowercase) > len(s) {
		return false
	}

	if isASCIILowercase(s) {
		// Fast path - s is in lowercase
		return matchPhrase(s, phraseLowercase)
	}

	// Slow path - convert s to lowercase before matching
	bb := bbPool.Get()
	bb.B = stringsutil.AppendLowercase(bb.B, s)
	sLowercase := bytesutil.ToUnsafeString(bb.B)
	ok := matchPhrase(sLowercase, phraseLowercase)
	bbPool.Put(bb)

	return ok
}

func isASCIILowercase(s string) bool {
	for i := range len(s) {
		c := s[i]
		if c >= utf8.RuneSelf || (c >= 'A' && c <= 'Z') {
			return false
		}
	}
	return true
}
