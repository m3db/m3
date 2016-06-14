package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"code.uber.internal/infra/memtsdb"
	"code.uber.internal/infra/memtsdb/benchmark/fs2"
	"code.uber.internal/infra/memtsdb/bootstrap"
	"code.uber.internal/infra/memtsdb/context"
	"code.uber.internal/infra/memtsdb/services/m3dbnode/serve"
	"code.uber.internal/infra/memtsdb/services/m3dbnode/serve/httpjson"
	"code.uber.internal/infra/memtsdb/services/m3dbnode/serve/tchannelthrift"
	"code.uber.internal/infra/memtsdb/sharding"
	"code.uber.internal/infra/memtsdb/storage"
	xtime "code.uber.internal/infra/memtsdb/x/time"

	"github.com/spaolacci/murmur3"
	"github.com/uber-common/bark"
)

var (
	indexFileArg    = flag.String("indexFile", "", "input index file")
	dataFileArg     = flag.String("dataFile", "", "input data file")
	repeatArg       = flag.Int("repeat", 0, "repeat the test data by appending again with forward shifted timestamps, must be >= 0")
	amplifyArg      = flag.Int("amplify", 1, "amplify the test data series IDs by, must be >= 1")
	tchannelAddrArg = flag.String("tchanneladdr", "0.0.0.0:9000", "TChannel server address")
	httpAddrArg     = flag.String("httpaddr", "0.0.0.0:9001", "HTTP server address")
	debugAddrArg    = flag.String("debugaddr", "0.0.0.0:9002", "Debug pprof over HTTP server address")

	timeLock       sync.RWMutex
	overriddenTime *time.Time
	nowFn          = func() time.Time {
		timeLock.RLock()
		if overriddenTime == nil {
			timeLock.RUnlock()
			return time.Now()
		}
		value := *overriddenTime
		timeLock.RUnlock()
		return value
	}
	overriddenAtFn = func() string {
		timeLock.RLock()
		value := overriddenTime
		timeLock.RUnlock()
		if value == nil {
			return "none"
		}
		return fmt.Sprintf("%v", *value)
	}
	setTimeFn = func(t time.Time) {
		var newlyOverriddenTime time.Time
		if overriddenTime == nil || !t.Equal(*overriddenTime) {
			timeLock.Lock()
			newlyOverriddenTime = t
			overriddenTime = &newlyOverriddenTime
			timeLock.Unlock()
		}
	}
	restoreTimeFn = func() {
		timeLock.Lock()
		overriddenTime = nil
		timeLock.Unlock()
	}
)

func main() {
	flag.Parse()

	if *indexFileArg == "" ||
		*dataFileArg == "" ||
		*repeatArg < 0 ||
		*amplifyArg < 1 ||
		*tchannelAddrArg == "" ||
		*httpAddrArg == "" {
		flag.Usage()
		os.Exit(1)
	}

	indexFile := *indexFileArg
	dataFile := *dataFileArg
	repeat := *repeatArg
	amplify := *amplifyArg
	tchannelAddr := *tchannelAddrArg
	httpAddr := *httpAddrArg
	debugAddr := *debugAddrArg

	// NB(r): decrease GOGC as it uses a large overhead of memory the
	// higher the percentage is and for our workload where the vast majority
	// is pooled (no allocs) this is just a large wasted memory overhead.
	debug.SetGCPercent(30)

	poolingBufferBucketAllocSize := 256
	poolingSeries := 550000 * amplify // 550k * amplify

	var opts memtsdb.DatabaseOptions
	opts = storage.NewDatabaseOptions().
		NowFn(nowFn).
		BufferFuture(10*time.Minute).
		BufferPast(10*time.Minute).
		BufferDrain(10*time.Minute).
		EncodingTszPooled(poolingBufferBucketAllocSize, poolingSeries).
		NewBootstrapFn(func() memtsdb.Bootstrap {
		return bootstrap.NewNoOpBootstrapProcess(opts)
	})

	log := opts.GetLogger()

	db := newDatabase(log, opts)
	defer db.Close()

	tchannelthriftClose, err := tchannelthrift.NewServer(db, tchannelAddr, nil).ListenAndServe()
	if err != nil {
		log.Fatalf("could not open tchannelthrift interface: %v", err)
	}
	defer tchannelthriftClose()
	log.Infof("tchannelthrift: listening on %v", tchannelAddr)

	httpjsonClose, err := httpjson.NewServer(db, httpAddr, nil).ListenAndServe()
	if err != nil {
		log.Fatalf("could not open httpjson interface: %v", err)
	}
	defer httpjsonClose()
	log.Infof("httpjson: listening on %v", httpAddr)

	debugClose, err := debugListenAndServe(debugAddr)
	if err != nil {
		log.Fatalf("could not open debug interface: %v", err)
	}
	defer debugClose()
	log.Infof("debug listening on %v", debugAddr)

	log.Infof("ingesting data")
	var wg sync.WaitGroup
	var ingested bool
	var l sync.RWMutex

	var ingestFrom, ingestTo time.Time
	var series, datapoints int

	signalCh := make(chan os.Signal)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	ingestCh := make(chan struct{}, 2)

	go func() {
		log.Infof("caught interrupt: %v", <-signalCh)

		// Early break ingestion if still going
		ingestCh <- struct{}{}

		// Second signal is to terminate
		log.Fatalf("fatal interrupt: %v", <-signalCh)
	}()

	wg.Add(1)
	go func() {
		ingestFrom, ingestTo, series, datapoints = ingestAll(log, db, indexFile, dataFile, repeat, amplify, ingestCh)
		l.Lock()
		defer l.Unlock()
		ingested = true
		ingestCh <- struct{}{}
		wg.Done()
	}()
	go func() {
		done := func() bool {
			l.RLock()
			defer l.RUnlock()
			return ingested
		}
		for !done() {
			if at := overriddenAtFn(); at != "none" {
				log.Infof("currently overridden time: %v", at)
			}
			time.Sleep(15 * time.Second)
		}
	}()

	wg.Wait()
	restoreTimeFn()
	log.Infof("ingested all data")
	log.Infof("ingestFrom: %v", ingestFrom)
	log.Infof("ingestTo: %v", ingestTo)
	log.Infof("series: %v", series)
	log.Infof("datapoints: %v", datapoints)

	finishedAt := time.Now()

	var mem runtime.MemStats
	stats := make(map[string]uint64)
	for {
		// Force memory to be returned before taking stats for the first minute
		if time.Now().Sub(finishedAt) < time.Minute {
			log.Infof("freeing memory to OS")
			debug.FreeOSMemory()
		}

		time.Sleep(10 * time.Second)

		if time.Now().Sub(finishedAt) < 5*time.Minute {
			log.Infof("reading stats")
			runtime.ReadMemStats(&mem)
			stats["Alloc"] = mem.Alloc
			stats["TotalAlloc"] = mem.TotalAlloc
			stats["Sys"] = mem.Sys
			stats["Lookups"] = mem.Lookups
			stats["Mallocs"] = mem.Mallocs
			stats["Frees"] = mem.Frees
			stats["HeapAlloc"] = mem.HeapAlloc
			stats["HeapSys"] = mem.HeapSys
			stats["HeapIdle"] = mem.HeapIdle
			stats["HeapInuse"] = mem.HeapInuse
			stats["HeapReleased"] = mem.HeapReleased
			stats["HeapObjects"] = mem.HeapObjects
			stats["StackInuse"] = mem.StackInuse
			stats["StackSys"] = mem.StackSys
			stats["MSpanInuse"] = mem.MSpanInuse
			stats["MSpanSys"] = mem.MSpanSys
			stats["MCacheInuse"] = mem.MCacheInuse
			stats["MCacheSys"] = mem.MCacheSys
			stats["BuckHashSys"] = mem.BuckHashSys
			stats["GCSys"] = mem.GCSys
			stats["OtherSys"] = mem.OtherSys

			log.Infof("stats: %v", stats)
		}
	}
}

func newDatabase(log bark.Logger, opts memtsdb.DatabaseOptions) storage.Database {
	shards := uint32(1024)
	shardingScheme, err := sharding.NewShardScheme(0, shards-1, func(id string) uint32 {
		return murmur3.Sum32([]byte(id)) % shards
	})
	if err != nil {
		log.Fatalf("could not create sharding scheme: %v", err)
	}

	db := storage.NewDatabase(shardingScheme.All(), opts)
	if err := db.Open(); err != nil {
		log.Fatalf("could not open database: %v", err)
	}

	if err := db.Bootstrap(nowFn()); err != nil {
		log.Fatalf("could not bootstrap database: %v", err)
	}

	return db
}

func ingestAll(
	log bark.Logger,
	db storage.Database,
	indexFile, dataFile string,
	repeat int, amplify int,
	doneCh <-chan struct{},
) (time.Time, time.Time, int, int) {
	var (
		repeatOffset time.Duration
		t            time.Time
		start        time.Time
		first        = true
		series       = 0
		datapoints   = int32(0)
	)

	log.Infof("reading series index")
	reader, err := fs2.NewReader(indexFile, dataFile)
	if err != nil {
		log.Fatalf("unable to create a new input reader: %v", err)
	}

	iterIDs, err := reader.IterIDs()
	if err != nil {
		log.Fatalf("failed to open ID iterator: %v", err)
	}
	for iterIDs.Next() {
		series++
	}
	if err := iterIDs.Err(); err != nil {
		log.Fatalf("error reading IDs: %v", err)
	}

	log.Infof("read series index: %d series", series)

	var breakOut int32

	go func() {
		<-doneCh
		log.Infof("ingestion done signal sent")
		atomic.AddInt32(&breakOut, 1)
	}()

	type work struct {
		id        string
		timestamp time.Time
		value     float64
	}

	numWorkers := runtime.NumCPU()

	workQueue := make(chan work, numWorkers)

	amplifySuffixes := make([]string, amplify)
	for i := 0; i < amplify; i++ {
		amplifySuffixes[i] = fmt.Sprintf(".amplified.%d", i)
	}

	for i := 0; i < numWorkers; i++ {
		go func() {
			var w work
			var ok bool
			ctx := context.NewContext()
			for {
				w, ok = <-workQueue
				if !ok {
					return
				}

				for i := 0; i < amplify; i++ {
					entryID := w.id
					if i > 0 {
						entryID = entryID + amplifySuffixes[i]
					}

					ctx.Reset()
					if err := db.Write(ctx, entryID, w.timestamp, w.value, xtime.Second, nil); err != nil {
						log.Fatalf("failed to write entry: %v", err)
					} else {
						atomic.AddInt32(&datapoints, 1)
					}
					ctx.Close()
				}
			}
		}()
	}

	seriesTarget := series * amplify
	log.Infof("beginning to ingest data: %d series (%d amplified)", seriesTarget, seriesTarget-series)

	var n int64
	iters := 1 + repeat
	for i := 0; i < iters && atomic.LoadInt32(&breakOut) == 0; i++ {
		iter, err := reader.Iter()
		if err != nil {
			log.Fatalf("failed to open iterator: %v", err)
		}

		var w work
		var ts int64
		for iter.Next() && atomic.LoadInt32(&breakOut) == 0 {
			w.id, ts, w.value = iter.Value()
			t = xtime.FromNormalizedTime(ts, time.Millisecond).Add(repeatOffset)
			setTimeFn(t)
			w.timestamp = t

			if first {
				first = false
				start = t
			}

			workQueue <- w
			n++
		}

		if err := iter.Err(); err != nil {
			log.Fatalf("error reading: %v", err)
		}

		if !first {
			// If ingested, add repeat offset for next iteration
			repeatOffset = t.Sub(start) + (10 * time.Second)
		}
	}

	close(workQueue)

	reader.Close()

	if overriddenTime != nil {
		log.Infof("overridden time is at: %v", *overriddenTime)
	}

	return start, t, series, int(datapoints)
}

func debugListenAndServe(debugAddr string) (serve.Close, error) {
	mux := http.NewServeMux()
	mux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	mux.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
	mux.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
	mux.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))
	mux.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))

	listener, err := net.Listen("tcp", debugAddr)
	if err != nil {
		return nil, err
	}

	server := http.Server{Handler: mux}
	go func() {
		server.Serve(listener)
	}()

	return func() {
		listener.Close()
	}, nil
}
