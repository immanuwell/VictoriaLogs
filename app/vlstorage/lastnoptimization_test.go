package vlstorage

import "testing"

func TestHasDuplicateTimestampsAtLastNRange(t *testing.T) {
	f := func(timestamps []int64, offset, rowsNeeded uint64, resultExpected bool) {
		t.Helper()

		rows := make([]logRow, len(timestamps))
		for i, timestamp := range timestamps {
			rows[i].timestamp = timestamp
		}

		result := hasDuplicateTimestampsAtLastNRange(rows, offset, rowsNeeded)
		if result != resultExpected {
			t.Fatalf("unexpected result; got %v; want %v", result, resultExpected)
		}
	}

	f([]int64{10, 9, 8, 7}, 0, 3, false)
	f([]int64{10, 10, 9, 8}, 0, 3, true)
	f([]int64{10, 9, 9, 8}, 0, 3, true)
	f([]int64{10, 9, 8, 8}, 0, 3, true)
	f([]int64{10, 10, 9, 8}, 1, 3, true)
	f([]int64{10, 9, 8, 7}, 1, 3, false)
}
