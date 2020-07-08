package tile

import (
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/apache/arrow/go/arrow/array"
)

// SeriesBlockFrame contains either all raw values
// for a given series in a block if the frame size
// was not specified, or the number of values
// that fall into the next sequential frame
// for a series in the block given the progression
// through each time series from the query Start time.
// e.g. with 10minute frame size that aligns with the
// query start, each series will return
// 12 frames in a two hour block.
type SeriesBlockFrame struct {
	// FrameStart is start of frame.
	FrameStart xtime.UnixNano
	// FrameEnd is end of frame.
	FrameEnd xtime.UnixNano
	// DatapointRecord is the apache arrow datapoint record.
	record *datapointRecord
}

func (f *SeriesBlockFrame) release() {
	f.record.release()
}

func (f *SeriesBlockFrame) reset(start xtime.UnixNano, end xtime.UnixNano) {
	f.release()
	f.FrameStart = start
	f.FrameEnd = end
}

// Values returns values for the record in a float64 arrow array.
func (f *SeriesBlockFrame) Values() *array.Float64 {
	return f.record.values()
}

// Timestamps returns timestamps for the record in an int64 arrow array.
func (f *SeriesBlockFrame) Timestamps() *array.Int64 {
	return f.record.timestamps()
}
