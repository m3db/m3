// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/m3db/m3db/src/query/benchmark/common"
	"github.com/m3db/m3db/src/query/util/logging"

	"go.uber.org/zap"
)

var (
	cardinality bool

	regenerateData bool
	inputFile      string

	dataDir  string
	dataFile string

	workers    int
	batch      int
	memprofile bool
	cpuprofile bool

	writeEndpoint string
)

func init() {
	flag.BoolVar(&cardinality, "cardinality", false, "calculate cardinality only")

	flag.BoolVar(&regenerateData, "regenerateData", false, "regenerate data")
	flag.StringVar(&inputFile, "inputFile", "benchmark_opentsdb", "input file")

	flag.StringVar(&dataDir, "dir", "prom", "folder containing data for benchmark")
	flag.StringVar(&dataFile, "dataFile", "benchmark_prom", "prefix for benchmark files")

	flag.IntVar(&workers, "workers", 2, "Number of parallel requests to make.")
	flag.IntVar(&batch, "batch", 5000, "Batch Size")

	flag.BoolVar(&memprofile, "memprofile", false, "Enable memory profile")
	flag.BoolVar(&cpuprofile, "cpuprofile", false, "Enable cpu profile")
	flag.StringVar(&writeEndpoint, "writeEndpoint", "http://localhost:7201/api/v1/prom/remote/write", "Write endpoint for m3coordinator")
	flag.Parse()
}

var (
	logger *zap.Logger
)

func main() {
	logging.InitWithCores(nil)
	ctx := context.Background()
	logger = logging.WithContext(ctx)
	defer logger.Sync()

	if cardinality {
		logger.Info("Calculating cardinality only")
		cardinality, err := calculateCardinality(inputFile, logger)
		if err != nil {
			logger.Fatal("cannot get cardinality", zap.Any("err", err))
			return
		}
		logger.Info("Cardinality", zap.String("dataFile", dataFile), zap.Int("cardinality", cardinality))
		return
	}

	metricsToWrite := 0
	if regenerateData {
		os.RemoveAll(dataDir)

		lines, err := convertToProm(inputFile, dataDir, dataFile, workers, batch, logger)
		if err != nil {
			logger.Fatal("cannot convert to prom", zap.Any("err", err))
			return
		}
		metricsToWrite = lines
	}
	logger.Info("Benchmarking writes on m3coordinator over http endpoint...")
	err := benchmarkCoordinator(metricsToWrite)
	if err != nil {
		logger.Fatal("cannot benchmark coordinator to prom", zap.Any("err", err))

		return
	}
}

func benchmarkCoordinator(metricsToWrite int) error {
	files, err := ioutil.ReadDir(dataDir)

	if err != nil {
		return err
	}

	workerBatches := make(map[int]int)

	actualFiles := 0

	for _, f := range files {
		name := f.Name()
		dataFileWithSeperator := fmt.Sprintf("%s_", dataFile)
		if strings.HasPrefix(name, dataFileWithSeperator) {
			workerFiles := name[len(dataFileWithSeperator):]

			split := strings.Split(workerFiles, "_")
			if len(split) != 2 {
				logger.Info("bad format", zap.String("fileName", name))
				continue
			}

			worker, err := strconv.Atoi(split[0])
			if err != nil {
				return err
			}
			workerBatches[worker]++
			actualFiles++
		}
	}

	itemsWritten := metricsToWrite
	if metricsToWrite == 0 {
		itemsWritten = batch * actualFiles
	}

	numBatches := ceilDivision(actualFiles, workers)

	start := time.Now()
	logger.Info(fmt.Sprintf("Benchmarking %d batches, with %d metrics a batch, across %d workers  (%d total metrics)\nStarted benchmark at: %s",
		numBatches, batch, workers, itemsWritten, start.Format(time.StampMilli)))

	wg := new(sync.WaitGroup)
	wg.Add(len(workerBatches))

	success := make(chan int)

	for worker, batches := range workerBatches {
		worker, batches := worker, batches
		go func(chan<- int) {
			defer wg.Done()
			for batchNumber := 0; batchNumber < batches; batchNumber++ {
				filePath := getFilePath(dataDir, dataFile, worker, batchNumber)
				err = writeToCoordinator(filePath)
				if err != nil {
					fmt.Println(err)
					success <- 0
					break
				} else {
					success <- batch
				}
			}
		}(success)
	}
	final := make(chan int)

	go func(success <-chan int, final chan<- int) {
		count := 0
		for c := range success {
			count = count + c
		}
		final <- count
	}(success, final)

	wg.Wait()
	close(success)
	actualWritten := <-final

	end := time.Now()
	logger.Info("Finished benchmark at:", zap.String("timestamp", start.Format(time.StampMilli)))
	took := end.Sub(start)
	rate := float64(actualWritten) / took.Seconds()
	perWorker := rate / float64(workers)

	logger.Info(fmt.Sprintf("loaded %d items in %fsec with %d workers (mean values rate %f/sec); per worker %f/sec",
		actualWritten, took.Seconds(), workers, rate, perWorker))

	return nil
}

func writeToCoordinator(fileName string) error {
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer file.Close()

	r, err := common.PostEncodedSnappy(writeEndpoint, file)
	if err != nil {
		return err
	}
	if r.StatusCode != 200 {
		b := make([]byte, r.ContentLength)
		r.Body.Read(b)
		r.Body.Close()
		return fmt.Errorf("bad response \ncode:%d\nbody:%s", r.StatusCode, b)
	}
	return nil
}
