package logstorage

import (
	"strings"
	"sync"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/prefixfilter"
)

// filterOr contains filters joined by OR operator.
//
// It is expressed as `f1 OR f2 ... OR fN` in LogsQL.
type filterOr struct {
	filters []filter

	byFieldTokensOnce sync.Once
	byFieldTokens     []fieldTokens
}

func newFilterOr(filters []filter) *filterOr {
	return &filterOr{
		filters: filters,
	}
}

func (fo *filterOr) String() string {
	filters := fo.filters
	a := make([]string, len(filters))
	for i, f := range filters {
		s := f.String()
		a[i] = s
	}
	return strings.Join(a, " or ")
}

func (fo *filterOr) updateNeededFields(pf *prefixfilter.Filter) {
	for _, f := range fo.filters {
		f.updateNeededFields(pf)
	}
}

func (fo *filterOr) matchRow(fields []Field) bool {
	for _, f := range fo.filters {
		if f.matchRow(fields) {
			return true
		}
	}
	return false
}

func (fo *filterOr) applyToBlockResult(br *blockResult, bm *bitmap) error {
	bmResult := getBitmap(bm.bitsLen)
	bmTmp := getBitmap(bm.bitsLen)
	defer putBitmap(bmTmp)
	defer putBitmap(bmResult)
	bmResult.copyFrom(bm)
	for _, f := range fo.filters {
		bmTmp.copyFrom(bmResult)
		if err := f.applyToBlockResult(br, bmTmp); err != nil {
			return err
		}
		bmResult.andNot(bmTmp)
		if bmResult.isZero() {
			return nil
		}
	}
	bm.andNot(bmResult)
	return nil
}

func (fo *filterOr) applyToBlockSearch(bs *blockSearch, bm *bitmap) error {
	matches, err := fo.matchBloomFilters(bs)
	if err != nil || !matches {
		// Fast path - fo doesn't match bloom filters.
		bm.resetBits()
		return err
	}

	bmResult := getBitmap(bm.bitsLen)
	bmTmp := getBitmap(bm.bitsLen)
	defer putBitmap(bmTmp)
	defer putBitmap(bmResult)

	bmResult.copyFrom(bm)
	for _, f := range fo.filters {
		bmTmp.copyFrom(bmResult)
		if err := f.applyToBlockSearch(bs, bmTmp); err != nil {
			return err
		}
		bmResult.andNot(bmTmp)
		if bmResult.isZero() {
			return nil
		}
	}
	bm.andNot(bmResult)
	return nil
}

func (fo *filterOr) matchBloomFilters(bs *blockSearch) (bool, error) {
	byFieldTokens := fo.getByFieldTokens()
	if len(byFieldTokens) == 0 {
		return true, nil
	}

	for _, ft := range byFieldTokens {
		fieldName := ft.field
		tokens := ft.tokens

		v, err := bs.getConstColumnValue(fieldName)
		if err != nil {
			return false, err
		}
		if v != "" {
			if matchStringByAllTokens(v, tokens) {
				return true, nil
			}
			continue
		}

		ch, err := bs.getColumnHeader(fieldName)
		if err != nil {
			return false, err
		}
		if ch == nil {
			continue
		}

		if ch.valueType == valueTypeDict {
			if matchDictValuesByAllTokens(ch.valuesDict.values, tokens) {
				return true, nil
			}
			continue
		}
		if matches, err := matchBloomFilterAllTokens(bs, ch, ft.tokensHashes); err != nil {
			return false, err
		} else if matches {
			return true, nil
		}
	}

	return false, nil
}

func (fo *filterOr) getByFieldTokens() []fieldTokens {
	fo.byFieldTokensOnce.Do(fo.initByFieldTokens)
	return fo.byFieldTokens
}

func (fo *filterOr) initByFieldTokens() {
	fo.byFieldTokens = getCommonTokensForOrFilters(fo.filters)
}

func getCommonTokensForOrFilters(filters []filter) []fieldTokens {
	m := make(map[string][][]string)
	var fieldNames []string

	mergeFieldTokens := func(fieldName string, tokens []string) {
		if len(tokens) == 0 {
			return
		}

		fieldName = getCanonicalColumnName(fieldName)
		if _, ok := m[fieldName]; !ok {
			fieldNames = append(fieldNames, fieldName)
		}
		m[fieldName] = append(m[fieldName], tokens)
	}

	for _, f := range filters {
		switch t := f.(type) {
		case *filterGeneric:
			if !t.isWildcard {
				tokens := t.getTokens()
				mergeFieldTokens(t.fieldName, tokens)
			}
		case *filterAnd:
			bfts := t.getByFieldTokens()
			for _, bft := range bfts {
				mergeFieldTokens(bft.field, bft.tokens)
			}
		default:
			// Cannot extract tokens from this filter. This means that it is impossible to extract common tokens from OR filters.
			return nil
		}
	}

	var byFieldTokens []fieldTokens
	for _, fieldName := range fieldNames {
		tokenss := m[fieldName]
		if len(tokenss) != len(filters) {
			// The filter for the given fieldName is missing in some OR filters,
			// so it is impossible to extract common tokens from these filters.
			continue
		}
		commonTokens := getCommonTokens(tokenss)
		if len(commonTokens) == 0 {
			continue
		}
		byFieldTokens = append(byFieldTokens, fieldTokens{
			field:        fieldName,
			tokens:       commonTokens,
			tokensHashes: appendTokensHashes(nil, commonTokens),
		})
	}

	return byFieldTokens
}
