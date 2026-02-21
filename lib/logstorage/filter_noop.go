package logstorage

import (
	"github.com/VictoriaMetrics/VictoriaLogs/lib/prefixfilter"
)

// filterNoop does nothing
type filterNoop struct {
}

func newFilterNoop() *filterNoop {
	return &noopFilter
}

var noopFilter filterNoop

func (fn *filterNoop) String() string {
	return "*"
}

func (fn *filterNoop) updateNeededFields(_ *prefixfilter.Filter) {
	// nothing to do
}

func (fn *filterNoop) matchRow(fields []Field) bool {
	return true
}

func (fn *filterNoop) applyToBlockResult(_ *blockResult, _ *bitmap) error {
	return nil
}

func (fn *filterNoop) applyToBlockSearch(_ *blockSearch, _ *bitmap) error {
	return nil
}
