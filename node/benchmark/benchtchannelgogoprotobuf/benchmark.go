package benchtchannelgogoprotobuf

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"code.uber.internal/infra/memtsdb/node/benchmark/bench"
	"code.uber.internal/infra/memtsdb/x/logging"

	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/raw"
	"golang.org/x/net/context"
)

var log = logging.SimpleLogger

// Close is a method to call to close a resource or procedure
type Close func()

type service struct {
	segment      []byte
	bytesPerHour int
}

func newDefaultService() *service {
	// [2048]byte fixed data segment
	// 900 bytesPerHour (2.5 bytes per datapoint, 6 data points per minute, 60 minutes)
	data := make([]byte, 2048)
	for i := range data {
		data[i] = byte(i % 255)
	}
	return newService(data, 900)
}

func newService(segment []byte, bytesPerHour int) *service {
	return &service{segment, bytesPerHour}
}

func (s *service) Fetch(ctx context.Context, req *FetchRequest) (*FetchResult, error) {
	durationMs := math.Abs(float64(req.EndUnixMs) - float64(req.StartUnixMs))
	hours := durationMs / 1000 / 60 / 60

	bytesLen := int(math.Ceil(hours * float64(s.bytesPerHour)))
	segmentsLen := int(math.Ceil(float64(bytesLen) / float64(len(s.segment))))
	lastSegmentLen := bytesLen % len(s.segment)

	segments := make([]*FetchResult_Segment, segmentsLen)
	for i := range segments {
		if i == segmentsLen-1 {
			segments[i] = &FetchResult_Segment{s.segment[:lastSegmentLen]}
		} else {
			segments[i] = &FetchResult_Segment{s.segment}
		}
	}

	return &FetchResult{Segments: segments}, nil
}

func (s *service) FetchBatch(ctx context.Context, req *FetchBatchRequest) (*FetchBatchResult, error) {
	durationMs := math.Abs(float64(req.EndUnixMs) - float64(req.StartUnixMs))
	hours := durationMs / 1000 / 60 / 60

	bytesLen := int(math.Ceil(hours * float64(s.bytesPerHour)))
	segmentsLen := int(math.Ceil(float64(bytesLen) / float64(len(s.segment))))
	lastSegmentLen := bytesLen % len(s.segment)

	segments := make([]*FetchResult_Segment, segmentsLen)
	for i := range segments {
		if i == segmentsLen-1 {
			segments[i] = &FetchResult_Segment{s.segment[:lastSegmentLen]}
		} else {
			segments[i] = &FetchResult_Segment{s.segment}
		}
	}

	result := &FetchBatchResult{Results: make([]*FetchResult, len(req.Ids))}
	singleResult := &FetchResult{Segments: segments}
	for i := range result.Results {
		result.Results[i] = singleResult
	}

	return result, nil
}

type fetchHandler struct {
	s       *service
	reqPool sync.Pool
}

func newFetchHandler(s *service) *fetchHandler {
	return &fetchHandler{s, sync.Pool{
		New: func() interface{} {
			return &FetchRequest{}
		},
	}}
}

func (h *fetchHandler) Handle(ctx context.Context, args *raw.Args) (*raw.Res, error) {
	req := h.reqPool.Get().(*FetchRequest)
	if err := req.Unmarshal(args.Arg3); err != nil {
		// NB(r): this should really send back an error to the client
		h.reqPool.Put(req)
		return nil, err
	}

	result, err := h.s.Fetch(ctx, req)
	if err != nil {
		// NB(r): this should really send back an error to the client
		h.reqPool.Put(req)
		return nil, err
	}
	h.reqPool.Put(req)

	response, err := result.Marshal()
	if err != nil {
		// NB(r): this should really send back an error to the client
		return nil, err
	}

	return &raw.Res{Arg3: response}, nil
}

func (h *fetchHandler) OnError(ctx context.Context, err error) {
	log.Errorf("fetchHandler.OnError: %v", err)
}

type fetchBatchHandler struct {
	s       *service
	reqPool sync.Pool
}

func newFetchBatchHandler(s *service) *fetchBatchHandler {
	return &fetchBatchHandler{s, sync.Pool{
		New: func() interface{} {
			return &FetchBatchRequest{}
		},
	}}
}

func (h *fetchBatchHandler) Handle(ctx context.Context, args *raw.Args) (*raw.Res, error) {
	req := h.reqPool.Get().(*FetchBatchRequest)
	if err := req.Unmarshal(args.Arg3); err != nil {
		// NB(r): this should really send back an error to the client
		h.reqPool.Put(req)
		return nil, err
	}

	result, err := h.s.FetchBatch(ctx, req)
	if err != nil {
		// NB(r): this should really send back an error to the client
		h.reqPool.Put(req)
		return nil, err
	}
	h.reqPool.Put(req)

	response, err := result.Marshal()
	if err != nil {
		// NB(r): this should really send back an error to the client
		return nil, err
	}

	return &raw.Res{Arg3: response}, nil
}

func (h *fetchBatchHandler) OnError(ctx context.Context, err error) {
	log.Errorf("fetchBatchHandler.OnError: %v", err)
}

func serveTestTChannelServer(address string, s *service, opts *tchannel.ChannelOptions) (Close, error) {
	channel, err := tchannel.NewChannel("benchtchannel-server", opts)
	if err != nil {
		return nil, err
	}

	channel.Register(raw.Wrap(newFetchHandler(s)), "Fetch")
	channel.Register(raw.Wrap(newFetchBatchHandler(s)), "FetchBatch")

	channel.ListenAndServe(address)

	return func() {
		channel.Close()
	}, nil
}

// StartTestTChannelServer starts a test server
func StartTestTChannelServer(address string, opts *tchannel.ChannelOptions) (Close, error) {
	return serveTestTChannelServer(address, newDefaultService(), opts)
}

type clientLBEntry struct {
	channel    *tchannel.Channel
	subChannel *tchannel.SubChannel
}

type clientRoundRobinLB struct {
	pool    []*clientLBEntry
	poolLen int64
	used    int64
}

func newClientRoundRobinLB(address string, n int) *clientRoundRobinLB {
	pool := make([]*clientLBEntry, n)
	for i := 0; i < n; i++ {
		channel, err := tchannel.NewChannel("benchtchannel-client", nil)
		if err != nil {
			log.Fatalf("TChannel construct channel to %s error: %v", address, err)
		}

		// Add peer and connect
		peer := channel.Peers().Add(address)
		if _, err := peer.GetConnection(context.Background()); err != nil {
			log.Fatalf("could not connect to peer: %v", err)
		}

		pool[i] = &clientLBEntry{channel, channel.GetSubChannel("benchtchannel-server")}
	}
	return &clientRoundRobinLB{pool, int64(len(pool)), 0}
}

func (p *clientRoundRobinLB) get() *clientLBEntry {
	n := atomic.AddInt64(&p.used, 1)
	return p.pool[n%p.poolLen]
}

func (p *clientRoundRobinLB) closeAll() {
	for i := range p.pool {
		p.pool[i].channel.Close()
	}
}

// BenchmarkTChannelGogoprotobufFetch benchmarks fetching one by one
func BenchmarkTChannelGogoprotobufFetch(
	address string,
	n, connectionsPerHost, concurrency int,
	nextRequest bench.RequestGenerator,
	ready chan<- struct{},
	start <-chan struct{},
	done chan<- []error,
) {
	lb := newClientRoundRobinLB(address, connectionsPerHost)
	defer lb.closeAll()

	workers := bench.NewWorkerPool(concurrency)

	ctx := context.Background()

	fetchRequestPool := sync.Pool{
		New: func() interface{} {
			return &FetchRequest{}
		},
	}

	fetchResultPool := sync.Pool{
		New: func() interface{} {
			return &FetchResult{}
		},
	}

	method := "Fetch"

	var errLock sync.Mutex
	var errs []error

	ready <- struct{}{}
	<-start

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		desc := nextRequest()

		for j := range desc.IDs {
			req := fetchRequestPool.Get().(*FetchRequest)
			req.StartUnixMs = desc.StartUnixMs
			req.EndUnixMs = desc.EndUnixMs
			req.Id = desc.IDs[j]

			wg.Add(1)
			workers.Go(func() {
				// Use explicit call to avoid cost of using defer
				done := func() {
					fetchRequestPool.Put(req)
					wg.Done()
				}

				payload, err := req.Marshal()
				if err != nil {
					errLock.Lock()
					errs = append(errs, err)
					errLock.Unlock()
					done()
					return
				}

				rctx, _ := context.WithTimeout(ctx, 24*time.Hour)
				resp, err := raw.CallV2(rctx, lb.get().subChannel, raw.CArgs{
					Method: method,
					Arg3:   payload,
				})
				if err != nil {
					errLock.Lock()
					errs = append(errs, err)
					errLock.Unlock()
					done()
					return
				}
				if resp.AppError {
					errLock.Lock()
					errs = append(errs, fmt.Errorf("tchannel application error"))
					errLock.Unlock()
					done()
					return
				}

				result := fetchResultPool.Get().(*FetchResult)
				if err := result.Unmarshal(resp.Arg3); err != nil {
					fetchResultPool.Put(result)
					errLock.Lock()
					errs = append(errs, err)
					errLock.Unlock()
					done()
					return
				}
				fetchResultPool.Put(result)

				done()
			})
		}
	}

	wg.Wait()

	done <- errs
}

// BenchmarkTChannelGogoprotobufFetchBatch benchmarks fetching one by one with a stream
func BenchmarkTChannelGogoprotobufFetchBatch(
	address string,
	n, connectionsPerHost, concurrency, batchLen int,
	nextRequest bench.RequestGenerator,
	ready chan<- struct{},
	start <-chan struct{},
	done chan<- []error,
) {
	lb := newClientRoundRobinLB(address, connectionsPerHost)
	defer lb.closeAll()

	workers := bench.NewWorkerPool(concurrency)

	ctx := context.Background()

	fetchRequestPool := sync.Pool{
		New: func() interface{} {
			return &FetchBatchRequest{}
		},
	}

	fetchResultPool := sync.Pool{
		New: func() interface{} {
			return &FetchBatchResult{}
		},
	}

	method := "FetchBatch"

	var errLock sync.Mutex
	var errs []error

	ready <- struct{}{}
	<-start

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		desc := nextRequest()

		wg.Add(1)
		workers.Go(func() {
			var batchesWg sync.WaitGroup

			idsLen := len(desc.IDs)

			for j := 0; j < idsLen; j += batchLen {
				desc := desc
				j := j

				batchesWg.Add(1)
				workers.Go(func() {
					req := fetchRequestPool.Get().(*FetchBatchRequest)
					req.StartUnixMs = desc.StartUnixMs
					req.EndUnixMs = desc.EndUnixMs

					k := minInt(j+batchLen, idsLen)
					req.Ids = desc.IDs[j:k]

					// Use explicit call to avoid cost of using defer
					done := func() {
						fetchRequestPool.Put(req)
						batchesWg.Done()
					}

					payload, err := req.Marshal()
					if err != nil {
						errLock.Lock()
						errs = append(errs, err)
						errLock.Unlock()
						done()
						return
					}

					rctx, _ := context.WithTimeout(ctx, 24*time.Hour)
					resp, err := raw.CallV2(rctx, lb.get().subChannel, raw.CArgs{
						Method: method,
						Arg3:   payload,
					})
					if err != nil {
						errLock.Lock()
						errs = append(errs, err)
						errLock.Unlock()
						done()
						return
					}
					if resp.AppError {
						errLock.Lock()
						errs = append(errs, fmt.Errorf("tchannel application error"))
						errLock.Unlock()
						done()
						return
					}

					result := fetchResultPool.Get().(*FetchBatchResult)
					if err := result.Unmarshal(resp.Arg3); err != nil {
						fetchResultPool.Put(result)
						errLock.Lock()
						errs = append(errs, err)
						errLock.Unlock()
						done()
						return
					}
					fetchResultPool.Put(result)

					done()
				})
			}

			batchesWg.Wait()
			wg.Done()
		})
	}

	wg.Wait()

	done <- errs
}

func minInt(lhs, rhs int) int {
	if lhs < rhs {
		return lhs
	}
	return rhs
}
