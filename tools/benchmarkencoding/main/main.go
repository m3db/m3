package main

import (
	"flag"
	"os"
	"time"

	"code.uber.internal/infra/memtsdb/encoding/tsz"
	log "github.com/Sirupsen/logrus"
)

var (
	inputFile        = flag.String("inputFile", "", "test input file")
	inputTimeUnit    = flag.String("inputTimeUnit", "", "time unit of the benchmark input, e.g., 1ms")
	inputStart       = flag.String("inputStart", "", "start time of the benchmark input, e.g. 2016-04-26T16:00:00-04:00")
	windowSize       = flag.String("windowSize", "", "window size, e.g., 2h")
	encodingTimeUnit = flag.String("encodingTimeUnit", "", "time unit of the encoder and the decoder, e.g., 1s")
)

func main() {
	flag.Parse()

	if len(*inputFile) == 0 || len(*inputStart) == 0 || len(*inputTimeUnit) == 0 || len(*windowSize) == 0 || len(*encodingTimeUnit) == 0 {
		flag.PrintDefaults()
		os.Exit(1)
	}

	start, err := time.Parse(time.RFC3339, *inputStart)
	if err != nil {
		log.Fatalf("invalid start time %s: %v", *inputStart, err)
	}

	ws, err := time.ParseDuration(*windowSize)
	if err != nil {
		log.Fatalf("invalid window size %s: %v", *windowSize, err)
	}

	itu, err := time.ParseDuration(*inputTimeUnit)
	if err != nil {
		log.Fatalf("invalid input time unit %s: %v", *inputTimeUnit, err)
	}

	etu, err := time.ParseDuration(*encodingTimeUnit)
	if err != nil {
		log.Fatalf("invalid encoding time unit %s: %v", *encodingTimeUnit, err)
	}

	log.Infof("input=%s, start=%v, windowSize=%v, inputTimeUnit=%v, encodingTimeUnit=%v", *inputFile, start, ws, itu, etu)

	enc := tsz.NewEncoder(start, etu)
	dec := tsz.NewDecoder(etu)
	bench, err := newBenchmark(*inputFile, start, ws, itu, enc, dec)
	if err != nil {
		log.Fatalf("unable to create benchmark: %v", err)
	}

	log.Info("start benchmarking")
	bench.Run()

	bench.Report()
}
