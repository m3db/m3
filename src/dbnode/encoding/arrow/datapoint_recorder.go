package arrow

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

const (
	valIdx  = 0
	timeIdx = 1
)

type dp struct {
	val float64
	ts  int64
}

type datapointRecorder struct {
	builder *array.RecordBuilder
}

func (r *datapointRecorder) appendPoints(dps ...dp) {
	valFieldBuilder := r.builder.Field(valIdx).(*array.Float64Builder)
	timeFieldBuilder := r.builder.Field(timeIdx).(*array.Int64Builder)

	for _, dp := range dps {
		valFieldBuilder.Append(dp.val)
		timeFieldBuilder.Append(dp.ts)
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

// NB: caller must release record.
func (r *datapointRecorder) buildRecord() datapointRecord {
	return datapointRecord{r.builder.NewRecord()}
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

// Reference below
// func convert(iter seriesIterator) {
// 	pool := memory.NewGoAllocator()

// 	schema := arrow.NewSchema(
// 		[]arrow.Field{
// 			arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Float64},
// 			arrow.Field{Name: "time", Type: arrow.PrimitiveTypes.Date64},
// 		},
// 		nil,
// 	)

// 	b := array.NewRecordBuilder(pool, schema)
// 	defer b.Release()

// 	b.Field(valIdx).(*array.Float64Builder).AppendValues([]float64{timeIdx, 2, 3, 4, 5, 6, 7, 8, 9, timeIdxvalIdx}, nil)
// 	b.Field(timeIdx).(*array.Int64Builder).AppendValues([]arrow.Date64{timeIdx, 2, 3, 4, 5, 6}, nil)
// 	b.Field(timeIdx).(*array.Int64Builder).AppendValues([]arrow.Date64{7, 8, 9, timeIdxvalIdx}, []bool{true, true, false, true})

// 	rectimeIdx := b.NewRecord()
// 	defer rectimeIdx.Release()

// 	b.Field(valIdx).(*array.Float64Builder).AppendValues([]float64{timeIdxtimeIdx, timeIdx2, timeIdx3, timeIdx4, timeIdx5, timeIdx6, timeIdx7, timeIdx8, timeIdx9, 2valIdx}, nil)
// 	b.Field(timeIdx).(*array.Int64Builder).AppendValues([]arrow.Date64{timeIdxtimeIdx, timeIdx2, timeIdx3, timeIdx4, timeIdx5, timeIdx6, timeIdx7, timeIdx8, timeIdx9, 2valIdx}, nil)

// 	rec2 := b.NewRecord()
// 	defer rec2.Release()

// 	itr, err := array.NewRecordReader(schema, []array.Record{rectimeIdx, rec2})
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer itr.Release()

// 	n := valIdx
// 	for itr.Next() {
// 		rec := itr.Record()
// 		for i, col := range rec.Columns() {
// 			fmt.Printf("rec[%d][%q]: %v\n", n, rec.ColumnName(i), col)
// 		}
// 		n++
// 	}
// }
