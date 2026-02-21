package logstorage

import (
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/valyala/fastjson"
)

// filterJSONArrayContainsAny matches if the JSON array in the given field contains the given value.
//
// Example LogsQL: `tags:json_array_contains_any("prod","dev")`
type filterJSONArrayContainsAny struct {
	values []string

	tokensOnce    sync.Once
	tokenss       [][]string
	tokensHashess [][]uint64
}

func newFilterJSONArrayContainsAny(fieldName string, values []string) *filterGeneric {
	fa := &filterJSONArrayContainsAny{
		values: values,
	}
	return newFilterGeneric(fieldName, fa)
}

func (fa *filterJSONArrayContainsAny) getTokenss() [][]string {
	fa.tokensOnce.Do(fa.initTokens)
	return fa.tokenss
}

func (fa *filterJSONArrayContainsAny) getTokensHashes() [][]uint64 {
	fa.tokensOnce.Do(fa.initTokens)
	return fa.tokensHashess
}

func (fa *filterJSONArrayContainsAny) initTokens() {
	tokenss := make([][]string, len(fa.values))
	for i, v := range fa.values {
		tokenss[i] = tokenizeStrings(nil, []string{v})
	}
	fa.tokenss = tokenss

	tokensHashess := make([][]uint64, len(tokenss))
	for i, tokens := range tokenss {
		tokensHashess[i] = appendTokensHashes(nil, tokens)
	}
	fa.tokensHashess = tokensHashess
}

func (fa *filterJSONArrayContainsAny) String() string {
	a := make([]string, len(fa.values))
	for i, v := range fa.values {
		a[i] = quoteTokenIfNeeded(v)
	}
	args := strings.Join(a, ",")
	return fmt.Sprintf("json_array_contains_any(%s)", args)
}

func (fa *filterJSONArrayContainsAny) matchRowByField(fields []Field, fieldName string) bool {
	tokenss := fa.getTokenss()

	v := getFieldValueByName(fields, fieldName)
	return matchJSONArrayContainsAny(v, fa.values, tokenss)
}

func (fa *filterJSONArrayContainsAny) applyToBlockResultByField(br *blockResult, bm *bitmap, fieldName string) error {
	tokenss := fa.getTokenss()

	c := br.getColumnByName(fieldName)
	if c.isConst {
		v := c.valuesEncoded[0]
		if !matchJSONArrayContainsAny(v, fa.values, tokenss) {
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
			return matchJSONArrayContainsAny(v, fa.values, tokenss)
		})
	case valueTypeDict:
		bb := bbPool.Get()
		defer bbPool.Put(bb)
		for _, v := range c.dictValues {
			c := byte(0)
			if matchJSONArrayContainsAny(v, fa.values, tokenss) {
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
	default:
		bm.resetBits()
	}
	return nil
}

func (fa *filterJSONArrayContainsAny) applyToBlockSearchByField(bs *blockSearch, bm *bitmap, fieldName string) error {
	tokenss := fa.getTokenss()

	v, err := bs.getConstColumnValue(fieldName)
	if err != nil {
		return err
	}
	if v != "" {
		if !matchJSONArrayContainsAny(v, fa.values, tokenss) {
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
		bm.resetBits()
		return nil
	}

	switch ch.valueType {
	case valueTypeString:
		tokensHashess := fa.getTokensHashes()
		matches, err := matchAnyTokensHashess(bs, ch, tokensHashess)
		if !matches || err != nil {
			bm.resetBits()
			return err
		}
		return visitValues(bs, ch, bm, func(v string) bool {
			return matchJSONArrayContainsAny(v, fa.values, tokenss)
		})
	case valueTypeDict:
		bb := bbPool.Get()
		defer bbPool.Put(bb)
		for _, v := range ch.valuesDict.values {
			c := byte(0)
			if matchJSONArrayContainsAny(v, fa.values, tokenss) {
				c = 1
			}
			bb.B = append(bb.B, c)
		}
		return matchEncodedValuesDict(bs, ch, bm, bb.B)
	default:
		bm.resetBits()
	}
	return nil
}

func matchAnyTokensHashess(bs *blockSearch, ch *columnHeader, tokensHashess [][]uint64) (bool, error) {
	for _, tokensHashes := range tokensHashess {
		matches, err := matchBloomFilterAllTokens(bs, ch, tokensHashes)
		if err != nil {
			return false, err
		}
		if matches {
			return true, nil
		}
	}
	return false, nil
}

func matchJSONArrayContainsAny(s string, values []string, tokenss [][]string) bool {
	if s == "" {
		// Fast path for empty strings.
		return false
	}

	s = trimJSONWhitespace(s)

	if !strings.HasPrefix(s, "[") {
		// Fast path - s is not a JSON array.
		return false
	}

	if !matchAnyTokenss(s, tokenss) {
		// Fast path - s doesn't contain any of the given values.
		return false
	}

	// Slow path - parse JSON array at s and search for matching values.
	p := jspp.Get()
	defer jspp.Put(p)

	v, err := p.Parse(s)
	if err != nil {
		return false
	}
	if v.Type() != fastjson.TypeArray {
		return false
	}
	jsa, err := v.Array()
	if err != nil {
		logger.Panicf("BUG: v.Array() mustn't return error; got %s", err)
	}

	for _, e := range jsa {
		// We only support checking against string representation of values in the array.
		switch e.Type() {
		case fastjson.TypeString:
			b, err := e.StringBytes()
			if err != nil {
				logger.Panicf("BUG: e.StringBytes() mustn't return error; got %s", err)
			}
			bs := bytesutil.ToUnsafeString(b)
			if slices.Contains(values, bs) {
				return true
			}
		case fastjson.TypeNumber, fastjson.TypeTrue, fastjson.TypeFalse, fastjson.TypeNull:
			bb := bbPool.Get()
			bb.B = e.MarshalTo(bb.B[:0])
			bs := bytesutil.ToUnsafeString(bb.B)
			ok := slices.Contains(values, bs)
			bbPool.Put(bb)
			if ok {
				return true
			}
		}
	}

	return false
}

func matchAnyTokenss(s string, tokenss [][]string) bool {
	for _, tokens := range tokenss {
		if matchAllSubstrings(s, tokens) {
			return true
		}
	}
	return false
}

func matchAllSubstrings(s string, tokens []string) bool {
	for _, token := range tokens {
		if !strings.Contains(s, token) {
			return false
		}
	}
	return true
}

func trimJSONWhitespace(s string) string {
	// trim whitespace prefix
	for len(s) > 0 {
		c := s[0]
		if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
			break
		}
		s = s[1:]
	}

	// trim whitespace suffix
	for len(s) > 0 {
		c := s[len(s)-1]
		if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
			break
		}
		s = s[:len(s)-1]
	}

	return s
}
