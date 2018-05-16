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
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/m3db/m3db/src/coordinator/benchmark/common"
)

var (
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
	flag.BoolVar(&regenerateData, "regenerateData", false, "regenerate data")
	flag.StringVar(&inputFile, "inputFile", "benchmark_opentsdb", "input file")

	flag.StringVar(&dataDir, "dir", "prom", "folder containing data for benchmark")
	flag.StringVar(&dataFile, "dataFile", "benchmark_prom_", "prefix for benchmark files")

	flag.IntVar(&workers, "workers", 2, "Number of parallel requests to make.")
	flag.IntVar(&batch, "batch", 5000, "Batch Size")

	flag.BoolVar(&memprofile, "memprofile", false, "Enable memory profile")
	flag.BoolVar(&cpuprofile, "cpuprofile", false, "Enable cpu profile")
	flag.StringVar(&writeEndpoint, "writeEndpoint", "http://localhost:7201/api/v1/prom/remote/write", "Write endpoint for m3coordinator")
	flag.Parse()
}

func main() {
	metricsToWrite := 0
	if regenerateData {

		os.RemoveAll(dataDir)

		lines, err := convertToProm(inputFile, dataDir, dataFile, workers, batch)
		if err != nil {
			fmt.Println(err)
			return
		}
		metricsToWrite = lines
	}

	log.Println("Benchmarking writes on m3coordinator over http endpoint...")
	err := benchmarkCoordinator(metricsToWrite)
	if err != nil {
		fmt.Println(err)
	}
}

func benchmarkCoordinator(metricsToWrite int) error {
	files, err := ioutil.ReadDir(dataDir)

	if err != nil {
		return err
	}

	workerBatches := make(map[int]int)

	for _, f := range files {
		name := f.Name()
		if strings.HasPrefix(name, dataFile) {
			workerFiles := name[len(dataFile):]

			split := strings.Split(workerFiles, "_")
			if len(split) != 2 {
				fmt.Println("bad format:", name)
				continue
			}

			worker, err := strconv.Atoi(split[0])
			if err != nil {
				return err
			}
			workerBatches[worker]++
		}
	}

	wg := new(sync.WaitGroup)
	wg.Add(len(workerBatches))
	itemsWritten := metricsToWrite
	if metricsToWrite == 0 {
		itemsWritten = batch * len(files)
	}

	start := time.Now()
	log.Println("Started benchmark at:", start.Format(time.StampMilli))

	for worker, batches := range workerBatches {
		worker, batches := worker, batches
		go func() {
			defer wg.Done()
			for batchNumber := 0; batchNumber < batches; batchNumber++ {
				err = writeToCoordinator(fmt.Sprintf("%s/%s%d_%d", dataDir, dataFile, worker, batchNumber))
				if err != nil {
					fmt.Println(err)
					break
				}
			}
		}()
	}

	wg.Wait()

	end := time.Now()
	log.Println("Finished benchmark at:", start.Format(time.StampMilli))
	took := end.Sub(start)
	rate := float64(itemsWritten) / took.Seconds()
	perWorker := rate / float64(workers)

	log.Printf("loaded %d items in %fsec with %d workers (mean values rate %f/sec); per worker %f/sec\n", itemsWritten, took.Seconds(), workers, rate, perWorker)

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
