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
	"bytes"
	"flag"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3db/src/cmd/services/m3coordinator/config"
	"github.com/m3db/m3db/src/coordinator/benchmark/common"
	"github.com/m3db/m3db/src/dbnode/client"
	xconfig "github.com/m3db/m3x/config"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/pkg/profile"
)

var (
	m3dbClientCfg string
	dataFile      string
	workers       int
	batch         int
	namespace     string
	address       string
	benchmarkers  string
	memprofile    bool
	cpuprofile    bool

	writeEndpoint string
	coordinator   bool

	configLoadOpts = xconfig.Options{
		DisableUnmarshalStrict: false,
		DisableValidate:        false,
	}
)

func init() {
	flag.StringVar(&m3dbClientCfg, "m3db-client-config", "configs/benchmark.yml", "used to create m3db client session")
	flag.StringVar(&dataFile, "data-file", "data.json", "input data for benchmark")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")
	flag.IntVar(&batch, "batch", 5000, "Batch Size")
	flag.StringVar(&namespace, "namespace", "metrics", "M3DB namespace where to store result metrics")
	flag.StringVar(&address, "address", "localhost:8888", "Address to expose benchmarker health and stats")
	flag.StringVar(&benchmarkers, "benchmarkers", "localhost:8888", "Comma separated host:ports addresses of benchmarkers to coordinate")
	flag.BoolVar(&memprofile, "memprofile", false, "Enable memory profile")
	flag.BoolVar(&cpuprofile, "cpuprofile", false, "Enable cpu profile")
	flag.StringVar(&writeEndpoint, "writeEndpoint", "http://localhost:7201/api/v1/prom/remote/write", "Write endpoint for m3coordinator")
	flag.BoolVar(&coordinator, "coordinator", false, "Benchmark through coordinator rather than m3db directly")
	flag.Parse()
}

func main() {
	if coordinator {
		log.Println("benchmarking writes on m3coordinator over http endpoint...")
		benchmarkCoordinator()
	} else {
		log.Println("benchmarking writes on m3db...")
		benchmarkM3DB()
	}
}

func benchmarkM3DB() {
	//Setup
	metrics := make([]*common.M3Metric, 0, common.MetricsLen)
	common.ConvertToM3(dataFile, workers, func(m *common.M3Metric) {
		metrics = append(metrics, m)
	})

	ch := make(chan *common.M3Metric, workers)
	inputDone := make(chan struct{})
	wg := new(sync.WaitGroup)

	var cfg config.Configuration
	if err := xconfig.LoadFile(&cfg, m3dbClientCfg, configLoadOpts); err != nil {
		log.Fatalf("unable to load %s: %v", m3dbClientCfg, err)
	}

	if len(cfg.Clusters) != 1 {
		log.Fatal("invalid config, expected single cluster definition")
	}

	m3dbClient, err := cfg.Clusters[0].Client.NewClient(client.ConfigurationParameters{}, func(v client.Options) client.Options {
		return v.SetWriteBatchSize(batch).SetWriteOpPoolSize(batch * 2)
	})
	if err != nil {
		log.Fatalf("unable to create m3db client, got error %v\n", err)
	}

	session, err := m3dbClient.NewSession()
	if err != nil {
		log.Fatalf("unable to create m3db client session, got error %v\n", err)
	}

	workerFunction := func() {
		wg.Add(1)
		writeToM3DB(session, ch)
		wg.Done()
	}

	appendReadCount := func() int {
		var items int
		for _, query := range metrics {
			ch <- query
			items++
		}
		close(inputDone)
		return items
	}

	cleanup := func() {
		<-inputDone
		close(ch)
		wg.Wait()
		if err := session.Close(); err != nil {
			log.Fatalf("unable to close m3db client session, got error %v\n", err)
		}
	}

	genericBenchmarker(workerFunction, appendReadCount, cleanup)
}

func benchmarkCoordinator() {
	// Setup
	metrics := make([]*bytes.Reader, 0, common.MetricsLen/batch)
	common.ConvertToProm(dataFile, workers, batch, func(m *bytes.Reader) {
		metrics = append(metrics, m)
	})

	ch := make(chan *bytes.Reader, workers)
	wg := new(sync.WaitGroup)

	workerFunction := func() {
		wg.Add(1)
		writeToCoordinator(ch)
		wg.Done()
	}

	inputDone := make(chan struct{})
	appendReadCount := func() int {
		var items int
		for _, query := range metrics {
			ch <- query
			items++
		}
		close(inputDone)
		return items
	}

	cleanup := func() {
		<-inputDone
		close(ch)
		wg.Wait()
	}
	genericBenchmarker(workerFunction, appendReadCount, cleanup)
}

func genericBenchmarker(workerFunction func(), appendReadCount func() int, cleanup func()) {
	if cpuprofile {
		p := profile.Start(profile.CPUProfile)
		defer p.Stop()
		if memprofile {
			log.Println("cannot have both cpu and mem profile active at once; defaulting to cpu profiling")
		}
	} else if memprofile {
		p := profile.Start(profile.MemProfile)
		defer p.Stop()
	}

	// send over http
	var waitForInit sync.WaitGroup
	for i := 0; i < workers; i++ {
		waitForInit.Add(1)
		go func() {
			waitForInit.Done()
			workerFunction()
		}()
	}

	log.Println("waiting for workers to spin up...")
	waitForInit.Wait()

	b := &benchmarker{address: address, benchmarkers: benchmarkers}
	go b.serve()
	log.Println("waiting for other benchmarkers to spin up...")
	b.waitForBenchmarkers()

	var (
		start          = time.Now()
		itemsRead      = appendReadCount()
		endNanosAtomic int64
	)
	log.Println("started benchmark at:", start.Format(time.StampMilli))
	go func() {
		for {
			time.Sleep(time.Second)
			if v := atomic.LoadInt64(&endNanosAtomic); v > 0 {
				stat.setRunTimeMs(int64(time.Unix(0, v).Sub(start) / time.Millisecond))
			} else {
				stat.setRunTimeMs(int64(time.Since(start) / time.Millisecond))
			}
		}
	}()

	cleanup()

	end := time.Now()
	log.Println("finished benchmark at:", start.Format(time.StampMilli))
	took := end.Sub(start)
	atomic.StoreInt64(&endNanosAtomic, end.UnixNano())
	rate := float64(itemsRead) / took.Seconds()
	perWorker := rate / float64(workers)

	log.Printf("loaded %d items in %fsec with %d workers (mean values rate %f/sec); per worker %f/sec\n", itemsRead, took.Seconds(), workers, rate, perWorker)
}

func writeToCoordinator(ch <-chan *bytes.Reader) {
	for query := range ch {
		if r, err := common.PostEncodedSnappy(writeEndpoint, query); err != nil {
			log.Println(err)
		} else {
			if r.StatusCode != 200 {
				b := make([]byte, r.ContentLength)
				r.Body.Read(b)
				r.Body.Close()
				log.Println(string(b))
			}
			stat.incWrites()
		}
	}
}

func writeToM3DB(session client.Session, ch <-chan *common.M3Metric) {
	var itemsWritten int
	namespaceID := ident.StringID(namespace)
	for query := range ch {
		id := ident.StringID(query.ID)
		if err := session.Write(namespaceID, id, query.Time, query.Value, xtime.Millisecond, nil); err != nil {
			log.Println(err)
		} else {
			stat.incWrites()
		}
		id.Finalize()
		if itemsWritten > 0 && itemsWritten%10000 == 0 {
			log.Println(itemsWritten)
		}
		itemsWritten++
	}
	namespaceID.Finalize()
}
