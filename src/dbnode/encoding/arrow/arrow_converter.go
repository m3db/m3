package arrow

import (
	"fmt"
	"io"
	"log"

	xerrors "github.com/m3db/m3/src/x/errors"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

type dp struct {
	val float64
	ts  int64
}

type datapointRecorder struct {
	s       *arrow.Schema
	builder *array.RecordBuilder

	closers []io.Closer
}

func (r *datapointRecorder) schema() *arrow.Schema {
	return r.s
}

func (r *datapointRecorder) AppendPoint(dps ...dp) {

}

func (r *datapointRecorder) Close() error {
	var multiErr xerrors.MultiError
	for _, closer := range r.closers {
		multiErr = multiErr.Add(closer.Close())
	}
	return multiErr.FinalError()
}

func convert(iter seriesIterator) {
	pool := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Float64},
			arrow.Field{Name: "time", Type: arrow.PrimitiveTypes.Date64},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	b.Field(0).(*array.Float64Builder).AppendValues([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
	b.Field(1).(*array.Date64Builder).AppendValues([]arrow.Date64{1, 2, 3, 4, 5, 6}, nil)
	b.Field(1).(*array.Date64Builder).AppendValues([]arrow.Date64{7, 8, 9, 10}, []bool{true, true, false, true})

	rec1 := b.NewRecord()
	defer rec1.Release()

	b.Field(0).(*array.Float64Builder).AppendValues([]float64{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, nil)
	b.Field(1).(*array.Date64Builder).AppendValues([]arrow.Date64{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, nil)

	rec2 := b.NewRecord()
	defer rec2.Release()

	itr, err := array.NewRecordReader(schema, []array.Record{rec1, rec2})
	if err != nil {
		log.Fatal(err)
	}
	defer itr.Release()

	n := 0
	for itr.Next() {
		rec := itr.Record()
		for i, col := range rec.Columns() {
			fmt.Printf("rec[%d][%q]: %v\n", n, rec.ColumnName(i), col)
		}
		n++
	}
}
