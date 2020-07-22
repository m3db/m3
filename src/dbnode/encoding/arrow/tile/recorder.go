package tile

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/m3db/m3/src/dbnode/ts"
	xtime "github.com/m3db/m3/src/x/time"
)

const (
	valIdx  = 0
	timeIdx = 1
)

// datapointRecorder records datapoints.
type datapointRecorder struct {
	builder     *array.RecordBuilder
	units       *unitRecorder
	annotations *annotationRecorder
}

func (r *datapointRecorder) record(dp ts.Datapoint, u xtime.Unit, a ts.Annotation) {
	valFieldBuilder := r.builder.Field(valIdx).(*array.Float64Builder)
	timeFieldBuilder := r.builder.Field(timeIdx).(*array.Int64Builder)
	valFieldBuilder.Append(dp.Value)
	timeFieldBuilder.Append(int64(dp.TimestampNanos))

	r.units.record(u)
	r.annotations.record(a)
}

func newDatapointRecorder(pool memory.Allocator) *datapointRecorder {
	fields := make([]arrow.Field, 2)
	fields[valIdx] = arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Float64}
	fields[timeIdx] = arrow.Field{Name: "time", Type: arrow.PrimitiveTypes.Int64}

	schema := arrow.NewSchema(
		[]arrow.Field{
			arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Float64},
			arrow.Field{Name: "time", Type: arrow.PrimitiveTypes.Int64},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	return &datapointRecorder{
		builder:     b,
		units:       newUnitRecorder(),
		annotations: newAnnotationRecorder(),
	}
}

func (r *datapointRecorder) release() {
	r.units.reset()
	r.annotations.reset()
	r.builder.Release()
}

// NB: caller must release record.
func (r *datapointRecorder) updateRecord(rec *datapointRecord) {
	rec.Record = r.builder.NewRecord()
	rec.units = r.units
	rec.annotations = r.annotations

	// rec.tags
}

type datapointRecord struct {
	array.Record
	units       *unitRecorder
	annotations *annotationRecorder
}

func (r *datapointRecord) values() *array.Float64 {
	return r.Columns()[valIdx].(*array.Float64)
}

func (r *datapointRecord) timestamps() *array.Int64 {
	return r.Columns()[timeIdx].(*array.Int64)
}

func (r *datapointRecord) release() {
	if r == nil {
		return
	}

	if r.Record != nil {
		r.Record.Release()
	}

	if r.units != nil {
		r.units.reset()
	}

	if r.annotations != nil {
		r.annotations.reset()
	}

	r.Record = nil
}

func newDatapointRecord() *datapointRecord {
	return &datapointRecord{}
}
