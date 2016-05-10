package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"time"

	"code.uber.internal/infra/memtsdb"
	"code.uber.internal/infra/memtsdb/node/benchmark/bench"
	"code.uber.internal/infra/memtsdb/node/benchmark/benchgrpc"
	"code.uber.internal/infra/memtsdb/node/benchmark/benchtchannel"
	"code.uber.internal/infra/memtsdb/node/benchmark/benchtchannelgogoprotobuf"

	log "github.com/Sirupsen/logrus"
)

type benchmarkFn func(ready chan<- struct{}, start <-chan struct{}, done chan<- []error)

type keyValue struct {
	key   string
	value string
}

type describeBenchmarkFn func() []keyValue

type resultsFn func(name string, t time.Duration, errs []error)

var (
	reqArg                       = flag.Int("req", 32, "number of requests")
	reqMaxArg                    = flag.Int("reqMax", 0, "number of requests max")
	reqIncFactorArg              = flag.Int("reqIncFactor", 4, "number of requests increase factor")
	idsArg                       = flag.Int("ids", 100, "number of IDs per request")
	idsMaxArg                    = flag.Int("idsMax", 0, "number of IDs per request max")
	idsIncFactorArg              = flag.Int("idsIncFactor", 10, "number of IDs per request increase factor")
	totalConcurrencyArg          = flag.Int("totalConcurrency", 1000000 /* C1M */, "total concurrency start value")
	totalConcurrencyMaxArg       = flag.Int("totalConcurrencyMax", 0, "total concurrency max value")
	totalConcurrencyIncFactorArg = flag.Int("totalConcurrencyIncFactor", 2, "total concurrency increase factor")
	connPerHostArg               = flag.Int("connPerHost", 1, "connections per host start value")
	connPerHostMaxArg            = flag.Int("connPerHostMax", 0, "connections per host max value")
	connPerHostIncFactorArg      = flag.Int("connPerHostIncFactor", 2, "connections per host increase factor")
	batchLenArg                  = flag.Int("batchLen", 16, "batch size start value")
	batchLenMaxArg               = flag.Int("batchLenMax", 0, "batch size max value")
	batchLenIncFactorArg         = flag.Int("batchLenIncFactor", 2, "batch size increase factor")
	resultsFileArg               = flag.String("resultsFile", "", "results file, defaults to results-benchmark-client-<time>.csv")
	grpcAddrArg                  = flag.String("grpcaddr", "127.0.0.1:8888", "benchmark GRPC server address")
	tchannelAddrArg              = flag.String("tchanneladdr", "127.0.0.1:8889", "benchmark TChannel server address")
	tchannelGogopbAddrArg        = flag.String("tchannelgogopbaddr", "127.0.0.1:8890", "benchmark TChannel gogoprotobuf server address")
)

func main() {
	flag.Parse()

	if *reqArg <= 0 ||
		*reqMaxArg < 0 ||
		*reqIncFactorArg <= 0 ||
		*idsArg <= 0 ||
		*idsMaxArg < 0 ||
		*idsIncFactorArg <= 0 ||
		*totalConcurrencyArg <= 0 ||
		*totalConcurrencyMaxArg < 0 ||
		*totalConcurrencyIncFactorArg <= 0 ||
		*connPerHostArg <= 0 ||
		*connPerHostMaxArg < 0 ||
		*connPerHostIncFactorArg <= 0 ||
		*batchLenArg <= 0 ||
		*batchLenMaxArg < 0 ||
		*batchLenIncFactorArg <= 0 ||
		*grpcAddrArg == "" ||
		*tchannelAddrArg == "" ||
		*tchannelGogopbAddrArg == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	if *reqMaxArg == 0 {
		*reqMaxArg = *reqArg
	}
	if *idsMaxArg == 0 {
		*idsMaxArg = *idsArg
	}
	if *totalConcurrencyMaxArg == 0 {
		*totalConcurrencyMaxArg = *totalConcurrencyArg
	}
	if *connPerHostMaxArg == 0 {
		*connPerHostMaxArg = *connPerHostArg
	}
	if *batchLenMaxArg == 0 {
		*batchLenMaxArg = *batchLenArg
	}
	if *resultsFileArg == "" {
		*resultsFileArg = fmt.Sprintf(
			"results-benchmark-client-%s.csv",
			strings.Replace(time.Now().Format(time.RFC3339), ":", "_", -1))
	}

	req := *reqArg
	reqMax := *reqMaxArg
	reqIncFactor := *reqIncFactorArg
	ids := *idsArg
	idsMax := *idsMaxArg
	idsIncFactor := *idsIncFactorArg
	totalConcurrency := *totalConcurrencyArg
	totalConcurrencyMax := *totalConcurrencyMaxArg
	totalConcurrencyIncFactor := *totalConcurrencyIncFactorArg
	connPerHost := *connPerHostArg
	connPerHostMax := *connPerHostMaxArg
	connPerHostIncFactor := *connPerHostIncFactorArg
	batchLen := *batchLenArg
	batchLenMax := *batchLenMaxArg
	batchLenIncFactor := *batchLenIncFactorArg
	resultsFile := *resultsFileArg
	grpcAddr := *grpcAddrArg
	tchannelAddr := *tchannelAddrArg
	tchannelGogopbAddr := *tchannelGogopbAddrArg

	debugAddr := "0.0.0.0:8886"
	log.Infof("started debug server: %v", debugAddr)
	go func() {
		log.Infof("debug server error: %v", http.ListenAndServe(debugAddr, nil))
	}()

	fd := openOutFile(resultsFile)
	fd.WriteString("name,n,idsLen,connectionsPerHost,concurrency,batchLen,totalTimeSeconds\n")

	defer func() {
		if err := fd.Close(); err != nil {
			log.Fatalf("failed to close results file: %v", err)
		}
		log.Infof("wrote results file: %s", resultsFile)
	}()

	for idsLen := ids; idsLen <= idsMax; idsLen *= idsIncFactor {
		var benchIDs []string
		for i := 0; i < idsLen; i++ {
			benchIDs = append(benchIDs, fmt.Sprintf("foo.bar.baz.%d", i))
		}

		requests := []bench.RequestDescription{
			{
				StartUnixMs: memtsdb.ToNormalizedTime(time.Now(), time.Millisecond),
				EndUnixMs:   memtsdb.ToNormalizedTime(time.Now().Add(3*time.Hour), time.Millisecond),
				IDs:         benchIDs,
			},
			{
				StartUnixMs: memtsdb.ToNormalizedTime(time.Now(), time.Millisecond),
				EndUnixMs:   memtsdb.ToNormalizedTime(time.Now().Add(6*time.Hour), time.Millisecond),
				IDs:         benchIDs,
			},
			{
				StartUnixMs: memtsdb.ToNormalizedTime(time.Now(), time.Millisecond),
				EndUnixMs:   memtsdb.ToNormalizedTime(time.Now().Add(12*time.Hour), time.Millisecond),
				IDs:         benchIDs,
			},
		}

		newGenerator := func() bench.RequestGenerator {
			var i int64
			requestsLen := int64(len(requests))
			return func() *bench.RequestDescription {
				j := i
				i++
				return &requests[j%requestsLen]
			}
		}

		for n := req; n <= reqMax; n *= reqIncFactor {
			for conns := connPerHost; conns <= connPerHostMax; conns *= connPerHostIncFactor {
				connectionsPerHost := conns

				for conc := totalConcurrency; conc <= totalConcurrencyMax; conc *= totalConcurrencyIncFactor {
					concurrency := conc

					// NB(r): GRPC_Fetch is just simply too slow at higher volume

					// runBenchmark("GRPC_Fetch", func(r chan<- struct{}, s <-chan struct{}, d chan<- []error) {
					// 	benchgrpc.BenchmarkGRPCFetch(grpcAddr, n, conns, conc, newGenerator(), r, s, d)
					// }, func() []keyValue {
					// 	return []keyValue{
					// 		keyValue{"n", fmt.Sprintf("%d", n)},
					// 		keyValue{"idsLen", fmt.Sprintf("%d", idsLen)},
					// 		keyValue{"connectionsPerHost", fmt.Sprintf("%d", connectionsPerHost)},
					// 		keyValue{"concurrency", fmt.Sprintf("%d", concurrency)},
					// 	}
					// }, func(name string, t time.Duration, errs []error) {
					// 	fd.WriteString(fmt.Sprintf(
					// 		"%s,%d,%d,%d,%d,%d,%.10f\n",
					// 		name, n, idsLen, connectionsPerHost, concurrency, 0,
					// 		float64(t)/float64(time.Second)))
					// })

					runBenchmark("GRPC_FetchStream", func(r chan<- struct{}, s <-chan struct{}, d chan<- []error) {
						benchgrpc.BenchmarkGRPCFetchStream(grpcAddr, n, conns, conc, newGenerator(), r, s, d)
					}, func() []keyValue {
						return []keyValue{
							{"n", fmt.Sprintf("%d", n)},
							{"idsLen", fmt.Sprintf("%d", idsLen)},
							{"connectionsPerHost", fmt.Sprintf("%d", connectionsPerHost)},
							{"concurrency", fmt.Sprintf("%d", concurrency)},
						}
					}, func(name string, t time.Duration, errs []error) {
						fd.WriteString(fmt.Sprintf(
							"%s,%d,%d,%d,%d,%d,%.10f\n",
							name, n, idsLen, connectionsPerHost, concurrency, 0,
							float64(t)/float64(time.Second)))
					})

					for b := batchLen; b <= batchLenMax; b *= batchLenIncFactor {
						runBenchmark(fmt.Sprintf("GRPC_FetchBatch_%d", b), func(r chan<- struct{}, s <-chan struct{}, d chan<- []error) {
							benchgrpc.BenchmarkGRPCFetchBatch(grpcAddr, n, conns, conc, b, newGenerator(), r, s, d)
						}, func() []keyValue {
							return []keyValue{
								{"n", fmt.Sprintf("%d", n)},
								{"idsLen", fmt.Sprintf("%d", idsLen)},
								{"connectionsPerHost", fmt.Sprintf("%d", connectionsPerHost)},
								{"concurrency", fmt.Sprintf("%d", concurrency)},
								{"batchLen", fmt.Sprintf("%d", b)},
							}
						}, func(name string, t time.Duration, errs []error) {
							fd.WriteString(fmt.Sprintf(
								"%s,%d,%d,%d,%d,%d,%.10f\n",
								name, n, idsLen, connectionsPerHost, concurrency, b,
								float64(t)/float64(time.Second)))
						})
					}

					// NB(r): GRPC_FetchBatchStream seems to have issues during high throughput tests
					//
					// for b := batchLen; b <= batchLenMax; b *= batchLenIncFactor {
					// 	runBenchmark("GRPC_FetchBatchStream", func(r chan<- struct{}, s <-chan struct{}, d chan<- []error) {
					// 		benchgrpc.BenchmarkGRPCFetchBatchStream(grpcAddr, n, conns, conc, b, newGenerator(), r, s, d)
					// 	}, func() []keyValue {
					// 		return []keyValue{
					// 			keyValue{"n", fmt.Sprintf("%d", n)},
					// 			keyValue{"idsLen", fmt.Sprintf("%d", idsLen)},
					// 			keyValue{"connectionsPerHost", fmt.Sprintf("%d", connectionsPerHost)},
					// 			keyValue{"concurrency", fmt.Sprintf("%d", concurrency)},
					// 			keyValue{"batchLen", fmt.Sprintf("%d", b)},
					// 		}
					// 	}, func(name string, t time.Duration, errs []error) {
					// 		fd.WriteString(fmt.Sprintf(
					// 			"%s,%d,%d,%d,%d,%d,%.10f\n",
					// 			name, n, idsLen, connectionsPerHost, concurrency, b,
					// 			float64(t)/float64(time.Second)))
					// 	})
					// }

					runBenchmark("TChannel_Fetch", func(r chan<- struct{}, s <-chan struct{}, d chan<- []error) {
						benchtchannel.BenchmarkTChannelFetch(tchannelAddr, n, conns, conc, newGenerator(), r, s, d)
					}, func() []keyValue {
						return []keyValue{
							{"n", fmt.Sprintf("%d", n)},
							{"idsLen", fmt.Sprintf("%d", idsLen)},
							{"connectionsPerHost", fmt.Sprintf("%d", connectionsPerHost)},
							{"concurrency", fmt.Sprintf("%d", concurrency)},
						}
					}, func(name string, t time.Duration, errs []error) {
						fd.WriteString(fmt.Sprintf(
							"%s,%d,%d,%d,%d,%d,%.10f\n",
							name, n, idsLen, connectionsPerHost, concurrency, 0,
							float64(t)/float64(time.Second)))
					})

					for b := batchLen; b <= batchLenMax; b *= batchLenIncFactor {
						runBenchmark(fmt.Sprintf("TChannel_FetchBatch_%d", b), func(r chan<- struct{}, s <-chan struct{}, d chan<- []error) {
							benchtchannel.BenchmarkTChannelFetchBatch(tchannelAddr, n, conns, conc, b, newGenerator(), r, s, d)
						}, func() []keyValue {
							return []keyValue{
								{"n", fmt.Sprintf("%d", n)},
								{"idsLen", fmt.Sprintf("%d", idsLen)},
								{"connectionsPerHost", fmt.Sprintf("%d", connectionsPerHost)},
								{"concurrency", fmt.Sprintf("%d", concurrency)},
								{"batchLen", fmt.Sprintf("%d", b)},
							}
						}, func(name string, t time.Duration, errs []error) {
							fd.WriteString(fmt.Sprintf(
								"%s,%d,%d,%d,%d,%d,%.10f\n",
								name, n, idsLen, connectionsPerHost, concurrency, b,
								float64(t)/float64(time.Second)))
						})
					}

					runBenchmark("TChannelGogoprotobuf_Fetch", func(r chan<- struct{}, s <-chan struct{}, d chan<- []error) {
						benchtchannelgogoprotobuf.BenchmarkTChannelGogoprotobufFetch(tchannelGogopbAddr, n, conns, conc, newGenerator(), r, s, d)
					}, func() []keyValue {
						return []keyValue{
							{"n", fmt.Sprintf("%d", n)},
							{"idsLen", fmt.Sprintf("%d", idsLen)},
							{"connectionsPerHost", fmt.Sprintf("%d", connectionsPerHost)},
							{"concurrency", fmt.Sprintf("%d", concurrency)},
						}
					}, func(name string, t time.Duration, errs []error) {
						fd.WriteString(fmt.Sprintf(
							"%s,%d,%d,%d,%d,%d,%.10f\n",
							name, n, idsLen, connectionsPerHost, concurrency, 0,
							float64(t)/float64(time.Second)))
					})

					for b := batchLen; b <= batchLenMax; b *= batchLenIncFactor {
						runBenchmark(fmt.Sprintf("TChannelGogoprotobuf_FetchBatch_%d", b), func(r chan<- struct{}, s <-chan struct{}, d chan<- []error) {
							benchtchannelgogoprotobuf.BenchmarkTChannelGogoprotobufFetchBatch(tchannelGogopbAddr, n, conns, conc, b, newGenerator(), r, s, d)
						}, func() []keyValue {
							return []keyValue{
								{"n", fmt.Sprintf("%d", n)},
								{"idsLen", fmt.Sprintf("%d", idsLen)},
								{"connectionsPerHost", fmt.Sprintf("%d", connectionsPerHost)},
								{"concurrency", fmt.Sprintf("%d", concurrency)},
								{"batchLen", fmt.Sprintf("%d", b)},
							}
						}, func(name string, t time.Duration, errs []error) {
							fd.WriteString(fmt.Sprintf(
								"%s,%d,%d,%d,%d,%d,%.10f\n",
								name, n, idsLen, connectionsPerHost, concurrency, b,
								float64(t)/float64(time.Second)))
						})
					}
				}
			}
		}
	}
}

func runBenchmark(
	name string,
	benchmark benchmarkFn,
	describe describeBenchmarkFn,
	results resultsFn,
) {
	ready := make(chan struct{})
	start := make(chan struct{})
	done := make(chan []error)

	go benchmark(ready, start, done)

	log.Infof("%s: signal ready", name)
	<-ready

	description := ""
	for _, kv := range describe() {
		description += fmt.Sprintf("\n%s=%s", kv.key, kv.value)
	}
	log.Infof("%s: starting %s", name, description)

	begin := time.Now()
	start <- struct{}{}

	errs := <-done
	end := time.Now()

	t := end.Sub(begin)
	results(name, t, errs)

	log.Infof("%s: done %d errors, took %v\n\n", name, len(errs), t)

	cooldown := 2 * time.Second
	log.Infof("Cooldown: %v\n\n", cooldown)
	time.Sleep(cooldown)
}

func openOutFile(filename string) *os.File {
	handle, err := os.OpenFile(filename, os.O_WRONLY, 0666)
	if os.IsNotExist(err) {
		handle, err = os.Create(filename)
	}
	if err == nil {
		err = handle.Truncate(0)
	}
	if err != nil {
		log.Fatalf("could not open for writing %s: %v", filename, err)
	}
	return handle
}
