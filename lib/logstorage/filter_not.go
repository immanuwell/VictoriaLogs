package logstorage

import (
	"github.com/VictoriaMetrics/VictoriaLogs/lib/prefixfilter"
)

// filterNot negates the filter.
//
// It is expressed as `NOT f` or `!f` in LogsQL.
type filterNot struct {
	f filter
}

func newFilterNot(f filter) *filterNot {
	return &filterNot{
		f: f,
	}
}

func (fn *filterNot) String() string {
	s := fn.f.String()
	switch fn.f.(type) {
	case *filterAnd, *filterOr:
		return "!(" + s + ")"
	default:
		return "!" + s
	}
}

func (fn *filterNot) updateNeededFields(pf *prefixfilter.Filter) {
	fn.f.updateNeededFields(pf)
}

func (fn *filterNot) matchRow(fields []Field) bool {
	return !fn.f.matchRow(fields)
}

func (fn *filterNot) applyToBlockResult(br *blockResult, bm *bitmap) error {
	// Minimize the number of rows to check by the filter by applying it
	// only to the rows, which match the bm, e.g. they may change the bm result.
	bmTmp := getBitmap(bm.bitsLen)
	defer putBitmap(bmTmp)
	bmTmp.copyFrom(bm)
	if err := fn.f.applyToBlockResult(br, bmTmp); err != nil {
		return err
	}
	bm.andNot(bmTmp)
	return nil
}

func (fn *filterNot) applyToBlockSearch(bs *blockSearch, bm *bitmap) error {
	// Minimize the number of rows to check by the filter by applying it
	// only to the rows, which match the bm, e.g. they may change the bm result.
	bmTmp := getBitmap(bm.bitsLen)
	defer putBitmap(bmTmp)
	bmTmp.copyFrom(bm)
	if err := fn.f.applyToBlockSearch(bs, bmTmp); err != nil {
		return err
	}
	bm.andNot(bmTmp)
	return nil
}
