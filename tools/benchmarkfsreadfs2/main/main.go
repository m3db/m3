package main

import (
	"flag"
	"os"

	"code.uber.internal/infra/memtsdb/benchmark/fs2"
	"code.uber.internal/infra/memtsdb/x/logging"
)

var log = logging.SimpleLogger

var (
	indexFile = flag.String("indexFile", "", "input index file")
	dataFile  = flag.String("dataFile", "", "input data file")
)

func main() {
	flag.Parse()

	if len(*indexFile) == 0 || len(*dataFile) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	log.Infof("creating input reader")
	reader, err := fs2.NewReader(*indexFile, *dataFile)
	if err != nil {
		log.Fatalf("unable to create a new input reader: %v", err)
	}

	defer reader.Close()

	iter, err := reader.Iter()
	if err != nil {
		log.Fatalf("unable to create iterator: %v", err)
	}

	log.Infof("reading input")
	for iter.Next() {
		id, ts, value := iter.Value()
		log.Infof("timestamp: %16d, value: %.10f, id: %s", ts, value, id)
	}
	if err := iter.Err(); err != nil {
		log.Fatalf("error reading: %v", err)
	}
	log.Infof("done")
}
