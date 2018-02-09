package main

import (
	"flag"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3coordinator/benchmark/common"
	"github.com/m3db/m3coordinator/services/m3coordinator/config"

	"github.com/m3db/m3db/client"
	xconfig "github.com/m3db/m3x/config"
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

	wg           sync.WaitGroup
	inputDone    chan struct{}
	itemsWritten chan int
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
	flag.Parse()
}

func main() {
	metrics := make([]*common.M3Metric, 0, common.MetricsLen)
	common.ConvertToM3(dataFile, workers, func(m *common.M3Metric) {
		metrics = append(metrics, m)
	})
	ch := make(chan *common.M3Metric, workers)
	inputDone = make(chan struct{})

	var cfg config.Configuration
	if err := xconfig.LoadFile(&cfg, m3dbClientCfg); err != nil {
		log.Fatalf("Unable to load %s: %v", m3dbClientCfg, err)
	}

	m3dbClientOpts := cfg.M3DBClientCfg
	m3dbClient, err := m3dbClientOpts.NewClient(client.ConfigurationParameters{}, func(v client.Options) client.Options {
		return v.SetWriteBatchSize(batch).SetWriteOpPoolSize(batch * 2)
	})
	if err != nil {
		log.Fatalf("Unable to create m3db client, got error %v\n", err)
	}

	session, err := m3dbClient.NewSession()
	if err != nil {
		log.Fatalf("Unable to create m3db client session, got error %v\n", err)
	}

	if cpuprofile {
		p := profile.Start(profile.CPUProfile)
		defer p.Stop()
	}

	if memprofile {
		p := profile.Start(profile.MemProfile)
		defer p.Stop()
	}

	itemsWritten = make(chan int)
	var waitForInit sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		waitForInit.Add(1)
		go func() {
			waitForInit.Done()
			writeToM3DB(session, ch, itemsWritten)
		}()
	}

	fmt.Printf("waiting for workers to spin up...\n")
	waitForInit.Wait()
	fmt.Printf("done\n")

	b := &benchmarker{address: address, benchmarkers: benchmarkers}
	go b.serve()
	fmt.Printf("waiting for other benchmarkers to spin up...\n")
	b.waitForBenchmarkers()
	fmt.Printf("done\n")

	var (
		start          = time.Now()
		itemsRead      = addMetricsToChan(ch, metrics)
		endNanosAtomic int64
	)
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

	<-inputDone
	close(ch)

	wg.Wait()
	sum := 0
	for i := 0; i < workers; i++ {
		sum += <-itemsWritten
	}

	end := time.Now()
	took := end.Sub(start)
	atomic.StoreInt64(&endNanosAtomic, end.UnixNano())
	rate := float64(itemsRead) / took.Seconds()

	fmt.Printf("loaded %d items in %fsec with %d workers (mean values rate %f/sec)\n", itemsRead, took.Seconds(), workers, rate)

	if err := session.Close(); err != nil {
		log.Fatalf("Unable to close m3db client session, got error %v\n", err)
	}

	select {}
}

func addMetricsToChan(ch chan *common.M3Metric, wq []*common.M3Metric) int {
	var items int
	for _, query := range wq {
		ch <- query
		items++
	}
	close(inputDone)
	return items
}

func writeToM3DB(session client.Session, ch chan *common.M3Metric, itemsWrittenCh chan int) {
	var itemsWritten int
	for query := range ch {
		id := query.ID
		if err := session.Write(namespace, id, query.Time, query.Value, xtime.Millisecond, nil); err != nil {
			fmt.Println(err)
		} else {
			stat.incWrites()
		}
		if itemsWritten > 0 && itemsWritten%10000 == 0 {
			fmt.Println(itemsWritten)
		}
		itemsWritten++
	}
	wg.Done()
	itemsWrittenCh <- itemsWritten
}
