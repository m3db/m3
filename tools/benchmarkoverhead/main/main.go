package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"code.uber.internal/infra/memtsdb/benchmark/fs2"
	"code.uber.internal/infra/memtsdb/encoding"
	"code.uber.internal/infra/memtsdb/encoding/tsz"
	"code.uber.internal/infra/memtsdb/services/mdbnode/serve"
	"code.uber.internal/infra/memtsdb/services/mdbnode/serve/httpjson"
	"code.uber.internal/infra/memtsdb/services/mdbnode/serve/tchannelthrift"
	"code.uber.internal/infra/memtsdb/sharding"
	"code.uber.internal/infra/memtsdb/storage"
	xtime "code.uber.internal/infra/memtsdb/x/time"

	log "github.com/Sirupsen/logrus"
	"github.com/spaolacci/murmur3"
)

var (
	indexFileArg    = flag.String("indexFile", "", "input index file")
	dataFileArg     = flag.String("dataFile", "", "input data file")
	repeatArg       = flag.Int("repeat", 0, "repeat the test data ingested by appending again with shifted forward timestamps")
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

	if *indexFileArg == "" || *dataFileArg == "" || *repeatArg < 0 || *tchannelAddrArg == "" || *httpAddrArg == "" {
		flag.Usage()
		os.Exit(1)
	}

	indexFile := *indexFileArg
	dataFile := *dataFileArg
	repeat := *repeatArg
	tchannelAddr := *tchannelAddrArg
	httpAddr := *httpAddrArg
	debugAddr := *debugAddrArg

	db := newDatabase()
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

	wg.Add(1)
	go func() {
		ingestFrom, ingestTo, series, datapoints = ingestAll(db, indexFile, dataFile, repeat)
		l.Lock()
		defer l.Unlock()
		ingested = true
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
			time.Sleep(time.Minute)
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
	for {
		// Force memory to be returned before taking stats for the first minute
		if time.Now().Sub(finishedAt) < time.Minute {
			log.Infof("freeing memory to OS")
			debug.FreeOSMemory()
		}

		time.Sleep(10 * time.Second)

		if time.Now().Sub(finishedAt) < 5*time.Minute {
			log.Infof("reading stats")
			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			log.Infof("stats.Alloc: %v", mem.Alloc)
			log.Infof("stats.HeapAlloc: %v", mem.HeapAlloc)
			log.Infof("stats.StackInuse: %v", mem.StackInuse)
		}
	}
}

func newDatabase() storage.Database {
	shards := uint32(1024)
	shardingScheme, err := sharding.NewShardScheme(0, shards-1, func(id string) uint32 {
		return murmur3.Sum32([]byte(id)) % shards
	})
	if err != nil {
		log.Fatalf("could not create sharding scheme: %v", err)
	}

	options := tsz.NewOptions()
	newEncoderFn := func(start time.Time, bytes []byte) encoding.Encoder {
		return tsz.NewEncoder(start, bytes, options)
	}
	newDecoderFn := func() encoding.Decoder {
		return tsz.NewDecoder(options)
	}
	opts := storage.NewDatabaseOptions().
		BlockSize(2 * time.Hour).
		BufferResolution(1 * time.Second).
		NewEncoderFn(newEncoderFn).
		NewDecoderFn(newDecoderFn).
		NowFn(nowFn).
		BufferFuture(1 * time.Minute).
		BufferPast(10 * time.Minute).
		BufferFlush(1 * time.Minute)

	db := storage.NewDatabase(shardingScheme.All(), opts)
	if err := db.Open(); err != nil {
		log.Fatalf("could not open database: %v", err)
	}

	return db
}

func ingestAll(
	db storage.Database, indexFile, dataFile string,
	repeat int,
) (time.Time, time.Time, int, int) {
	var (
		repeatOffset time.Duration
		first        bool
		start        time.Time
		t            time.Time
		series       = make(map[string]struct{})
		datapoints   = 0
	)

	iters := 1 + repeat
	for i := 0; i < iters; i++ {
		indexFd, err := os.Open(indexFile)
		if err != nil {
			log.Fatalf("could not open index file: %v", indexFile)
		}

		dataFd, err := os.Open(dataFile)
		if err != nil {
			log.Fatalf("could not open data file: %v", dataFile)
		}

		reader, err := fs2.NewReader(indexFd, dataFd)
		if err != nil {
			log.Fatalf("unable to create a new input reader: %v", err)
		}

		iter := reader.Iter()

		var (
			id    string
			ts    int64
			value float64
		)
		first = true
		for iter.Next() {
			id, ts, value = iter.Value()
			t = xtime.FromNormalizedTime(ts, time.Millisecond).Add(repeatOffset)
			setTimeFn(t)

			if first {
				first = false
				start = t
			}

			if _, ok := series[id]; !ok {
				series[id] = struct{}{}
			}

			if err := db.Write(id, t, value, xtime.Second, nil); err != nil {
				log.Fatalf("failed to write entry: %v", err)
			} else {
				datapoints++
			}
		}
		if err := iter.Err(); err != nil {
			log.Fatalf("error reading: %v", err)
		}

		indexFd.Close()
		dataFd.Close()

		if !first {
			// If ingested, add repeat offset for next iteration
			repeatOffset = t.Sub(start) + (10 * time.Second)
		}
	}

	if overriddenTime != nil {
		log.Infof("overridden time is at: %v", *overriddenTime)
	}

	return start, t, len(series), datapoints
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
