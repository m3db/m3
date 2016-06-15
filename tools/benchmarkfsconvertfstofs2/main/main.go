package main

import (
	"container/heap"
	"flag"
	"os"

	"code.uber.internal/infra/memtsdb/benchmark/fs"
	"code.uber.internal/infra/memtsdb/benchmark/fs2"
	"code.uber.internal/infra/memtsdb/x/logging"
)

var log = logging.SimpleLogger

var (
	inputFile       = flag.String("inputFile", "", "test input file")
	outputIndexFile = flag.String("outputIndexFile", "tsdb-index.db", "output index file")
	outputDataFile  = flag.String("outputDataFile", "tsdb-data.db", "output data file")
)

func main() {
	flag.Parse()

	if len(*inputFile) == 0 || len(*outputIndexFile) == 0 || len(*outputDataFile) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	log.Infof("creating input reader")
	reader, err := fs.NewReader(*inputFile)
	if err != nil {
		log.Fatalf("unable to create a new input reader: %v", err)
	}

	writer, err := fs2.NewWriter(*outputIndexFile, *outputDataFile)
	if err != nil {
		log.Fatalf("unable to create a new output writer: %v", err)
	}

	iter := reader.Iter()

	var entries byTimestampAsc
	heap.Init(&entries)

	log.Infof("reading input")
	for iter.Next() {
		value := iter.Value()
		for _, datapoint := range value.Values {
			id := value.Id
			heap.Push(&entries, &entry{&id, datapoint.Timestamp, datapoint.Value})
		}
	}
	if err := iter.Err(); err != nil {
		log.Fatalf("error reading: %v", err)
	}

	log.Infof("writing time ordered output")
	total := entries.Len()
	for i := 0; i < total; i++ {
		e := heap.Pop(&entries).(*entry)
		writer.Write(*e.id, e.timestamp, e.value)
	}
	if err := writer.Close(); err != nil {
		log.Fatalf("failed to close writer: %v", err)
	}
	log.Infof("done")
}

type entry struct {
	id        *string
	timestamp int64
	value     float64
}

// Implements heap.Interface
type byTimestampAsc []*entry

func (v byTimestampAsc) Len() int {
	return len(v)
}

func (v byTimestampAsc) Less(lhs, rhs int) bool {
	return v[lhs].timestamp < v[rhs].timestamp
}

func (v byTimestampAsc) Swap(lhs, rhs int) {
	v[lhs], v[rhs] = v[rhs], v[lhs]
}

func (v *byTimestampAsc) Push(el interface{}) {
	*v = append(*v, el.(*entry))
}

func (v *byTimestampAsc) Pop() interface{} {
	old := *v
	n := len(old)
	if n == 0 {
		return nil
	}
	x := old[n-1]
	*v = old[:n-1]
	return x
}
