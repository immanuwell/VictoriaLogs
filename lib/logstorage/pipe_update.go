package logstorage

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/atomicutil"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/prefixfilter"
)

func updateNeededFieldsForUpdatePipe(pf *prefixfilter.Filter, field string, iff *ifFilter) {
	if iff != nil && pf.MatchString(field) {
		pf.AddAllowFilters(iff.allowFilters)
	}
}

// shouldDenyOverwrittenField reports whether planner can safely avoid reading
// the original value for an overwritten field.
func shouldDenyOverwrittenField(iff *ifFilter, keepOriginalFields, skipEmptyResults bool) bool {
	return iff == nil && !keepOriginalFields && !skipEmptyResults
}

func newPipeUpdateProcessor(cancel func(error), updateFunc func(a *arena, v string) string, ppNext pipeProcessor, field string, iff *ifFilter) pipeProcessor {
	return &pipeUpdateProcessor{
		updateFunc: updateFunc,

		field: field,
		iff:   iff,

		ppNext: ppNext,
		cancel: cancel,
	}
}

type pipeUpdateProcessor struct {
	updateFunc func(a *arena, v string) string

	field string
	iff   *ifFilter

	ppNext pipeProcessor

	shards atomicutil.Slice[pipeUpdateProcessorShard]
	cancel func(error)
}

type pipeUpdateProcessorShard struct {
	bm bitmap

	rc resultColumn
	a  arena
}

func (pup *pipeUpdateProcessor) writeBlock(workerID uint, br *blockResult) {
	if br.rowsLen == 0 {
		return
	}

	shard := pup.shards.Get(workerID)

	bm := &shard.bm
	if iff := pup.iff; iff != nil {
		bm.init(br.rowsLen)
		bm.setBits()
		if err := iff.f.applyToBlockResult(br, bm); err != nil {
			pup.cancel(err)
			return
		}
		if bm.isZero() {
			pup.ppNext.writeBlock(workerID, br)
			return
		}
	}

	shard.rc.name = pup.field

	c := br.getColumnByName(pup.field)
	values, err := c.getValues(br)
	if err != nil {
		pup.cancel(err)
		return
	}

	needUpdates := true
	vPrev := ""
	vNew := ""
	for rowIdx, v := range values {
		if pup.iff == nil || bm.isSetBit(rowIdx) {
			if needUpdates || vPrev != v {
				vPrev = v
				needUpdates = false

				vNew = pup.updateFunc(&shard.a, v)
			}
			shard.rc.addValue(vNew)
		} else {
			shard.rc.addValue(v)
		}
	}

	br.addResultColumn(shard.rc)
	pup.ppNext.writeBlock(workerID, br)

	shard.rc.reset()
	shard.a.reset()
}

func (pup *pipeUpdateProcessor) flush() {}
