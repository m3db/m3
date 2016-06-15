package benchgrpc

import (
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"

	"code.uber.internal/infra/memtsdb/node/benchmark/bench"
	"code.uber.internal/infra/memtsdb/x/logging"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"
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

func (s *service) FetchStream(stream MemTSDB_FetchStreamServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		result, err := s.Fetch(context.TODO(), req)
		if err != nil {
			return err
		}
		if err := stream.Send(result); err != nil {
			return err
		}
	}
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

func (s *service) FetchBatchStream(stream MemTSDB_FetchBatchStreamServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		result, err := s.FetchBatch(context.TODO(), req)
		if err != nil {
			return err
		}
		if err := stream.Send(result); err != nil {
			return err
		}
	}
}

func serveTestGRPCServer(listener net.Listener, impl MemTSDBServer, opts ...grpc.ServerOption) (Close, error) {
	s := grpc.NewServer(opts...)
	RegisterMemTSDBServer(s, impl)

	go s.Serve(listener)

	return s.Stop, nil
}

// StartTestGRPCServer starts a test server
func StartTestGRPCServer(listener net.Listener, opts ...grpc.ServerOption) (Close, error) {
	return serveTestGRPCServer(listener, newDefaultService(), opts...)
}

type clientLBEntry struct {
	conn   *grpc.ClientConn
	client MemTSDBClient
}

type clientRoundRobinLB struct {
	pool    []*clientLBEntry
	poolLen int64
	used    int64
}

func newClientRoundRobinLB(address string, n int) *clientRoundRobinLB {
	pool := make([]*clientLBEntry, n)
	for i := 0; i < n; i++ {
		conn, err := grpc.Dial(address, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("GRPC dial %s error: %v", address, err)
		}

		client := NewMemTSDBClient(conn)

		pool[i] = &clientLBEntry{conn, client}
	}
	return &clientRoundRobinLB{pool, int64(len(pool)), 0}
}

func (p *clientRoundRobinLB) get() *clientLBEntry {
	n := atomic.AddInt64(&p.used, 1)
	return p.pool[n%p.poolLen]
}

func (p *clientRoundRobinLB) closeAll() {
	for i := range p.pool {
		p.pool[i].conn.Close()
	}
}

// BenchmarkGRPCFetch benchmarks fetching one by one
func BenchmarkGRPCFetch(
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
				_, err := lb.get().client.Fetch(ctx, req)
				if err != nil {
					errLock.Lock()
					errs = append(errs, err)
					errLock.Unlock()
				}
				fetchRequestPool.Put(req)

				wg.Done()
			})
		}
	}

	wg.Wait()

	done <- errs
}

// BenchmarkGRPCFetchStream benchmarks fetching one by one with a stream
func BenchmarkGRPCFetchStream(
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

	var errLock sync.Mutex
	var errs []error

	ready <- struct{}{}
	<-start

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		desc := nextRequest()

		wg.Add(1)
		workers.Go(func() {
			stream, err := lb.get().client.FetchStream(ctx)
			if err != nil {
				errLock.Lock()
				errs = append(errs, err)
				errLock.Unlock()
			}

			var readWg sync.WaitGroup
			readWg.Add(1)
			workers.Go(func() {
				// Read results
				idsLen := len(desc.IDs)
				reads := 0
				for reads = 0; reads < idsLen; reads++ {
					_, err := stream.Recv()
					if err == io.EOF {
						break
					}
					if err != nil {
						errLock.Lock()
						errs = append(errs, err)
						errLock.Unlock()
						break
					}
				}
				if err := stream.CloseSend(); err != nil {
					errLock.Lock()
					errs = append(errs, err)
					errLock.Unlock()
				}
				readWg.Done()
			})

			for j := range desc.IDs {
				// Request results
				req := fetchRequestPool.Get().(*FetchRequest)
				req.StartUnixMs = desc.StartUnixMs
				req.EndUnixMs = desc.EndUnixMs
				req.Id = desc.IDs[j]

				err := stream.Send(req)
				if err != nil {
					errLock.Lock()
					errs = append(errs, err)
					errLock.Unlock()
					break
				}

				fetchRequestPool.Put(req)
			}

			readWg.Wait()
			wg.Done()
		})
	}

	wg.Wait()

	done <- errs
}

// BenchmarkGRPCFetchBatch benchmarks fetching in batch
func BenchmarkGRPCFetchBatch(
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

					_, err := lb.get().client.FetchBatch(ctx, req)
					if err != nil {
						errLock.Lock()
						errs = append(errs, err)
						errLock.Unlock()
					}
					fetchRequestPool.Put(req)
					batchesWg.Done()
				})
			}

			batchesWg.Wait()
			wg.Done()
		})
	}

	wg.Wait()

	done <- errs
}

// BenchmarkGRPCFetchBatchStream benchmarks fetching in batch with a stream
func BenchmarkGRPCFetchBatchStream(
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

	var errLock sync.Mutex
	var errs []error

	ready <- struct{}{}
	<-start

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		desc := nextRequest()

		wg.Add(1)
		workers.Go(func() {
			stream, err := lb.get().client.FetchBatchStream(ctx)
			if err != nil {
				errLock.Lock()
				errs = append(errs, err)
				errLock.Unlock()
			}

			idsLen := len(desc.IDs)

			var readWg sync.WaitGroup
			readWg.Add(1)
			workers.Go(func() {
				// Read results
				reads := 0
				for reads < idsLen {
					result, err := stream.Recv()
					if err == io.EOF {
						break
					}
					if err != nil {
						errLock.Lock()
						errs = append(errs, err)
						errLock.Unlock()
						break
					}
					reads += len(result.Results)
				}
				if err := stream.CloseSend(); err != nil {
					errLock.Lock()
					errs = append(errs, err)
					errLock.Unlock()
				}
				readWg.Done()
			})

			for j := 0; j < idsLen; j += batchLen {
				desc := desc
				j := j

				workers.Go(func() {
					req := fetchRequestPool.Get().(*FetchBatchRequest)
					req.StartUnixMs = desc.StartUnixMs
					req.EndUnixMs = desc.EndUnixMs

					k := minInt(j+batchLen, idsLen)
					req.Ids = desc.IDs[j:k]

					err := stream.Send(req)
					if err != nil {
						errLock.Lock()
						errs = append(errs, err)
						errLock.Unlock()
					}

					fetchRequestPool.Put(req)
				})
			}

			readWg.Wait()
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
