package tile

import (
	"github.com/m3db/m3/src/dbnode/encoding/arrow/base"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

const (
	valIdx  = 0
	timeIdx = 1
)

type datapointRecorder struct {
	builder *array.RecordBuilder
}

func (r *datapointRecorder) appendPoints(dps ...base.Datapoint) {
	valFieldBuilder := r.builder.Field(valIdx).(*array.Float64Builder)
	timeFieldBuilder := r.builder.Field(timeIdx).(*array.Int64Builder)

	for _, dp := range dps {
		valFieldBuilder.Append(dp.Value)
		timeFieldBuilder.Append(int64(dp.Timestamp))
	}
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
		builder: b,
	}
}

func (r *datapointRecorder) release() {
	r.builder.Release()
}

type datapointRecord struct {
	array.Record
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

	r.Record = nil
}

// NB: caller must release record.
func (r *datapointRecorder) updateRecord(rec *datapointRecord) {
	rec.Record = r.builder.NewRecord()
}

func newDatapointRecord() *datapointRecord {
	return &datapointRecord{}
}
