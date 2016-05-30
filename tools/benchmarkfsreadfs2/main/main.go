package main

import (
	"flag"
	"os"

	"code.uber.internal/infra/memtsdb/benchmark/fs2"
	log "github.com/Sirupsen/logrus"
)

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

	indexFd, err := os.Open(*indexFile)
	if err != nil {
		log.Fatalf("could not open index file: %v", *indexFile)
	}

	defer indexFd.Close()

	dataFd, err := os.Open(*dataFile)
	if err != nil {
		log.Fatalf("could not open data file: %v", *dataFile)
	}

	defer dataFd.Close()

	log.Infof("creating input reader")
	reader, err := fs2.NewReader(indexFd, dataFd)
	if err != nil {
		log.Fatalf("unable to create a new input reader: %v", err)
	}

	iter := reader.Iter()

	log.Infof("reading input")
	for iter.Next() {
		id, ts, value := iter.Value()
		log.Printf("timestamp: %16d, value: %.10f, id: %s", ts, value, id)
	}
	if err := iter.Err(); err != nil {
		log.Fatalf("error reading: %v", err)
	}
	log.Infof("done")
}
