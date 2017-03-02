// Copyright (c) 2016 Uber Technologies, Inc.
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

package fs

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/pool"
)

var (
	errBlockRetrieverNotOpen             = errors.New("block retriever is not open")
	errBlockRetrieverAlreadyOpenOrClosed = errors.New("block retriever already open or is closed")
)

const (
	defaultRetrieveRequestQueueCapacity = 4096
)

type blockRetrieverStatus int

type newSeekerMgrFn func(
	bytesPool pool.CheckedBytesPool,
	opts Options,
) FileSetSeekerManager

const (
	blockRetrieverNotOpen blockRetrieverStatus = iota
	blockRetrieverOpen
	blockRetrieverClosed
)

type blockRetriever struct {
	sync.RWMutex

	opts   BlockRetrieverOptions
	fsOpts Options

	newSeekerMgrFn newSeekerMgrFn

	reqPool   retrieveRequestPool
	bytesPool pool.CheckedBytesPool
	namespace ts.ID

	status         blockRetrieverStatus
	reqsByShardIdx []*shardRetrieveRequests
	seekerMgrs     []FileSetSeekerManager
	notifyFetch    chan struct{}
}

type notifyRetrieval struct {
	target  block.OnRetrieveBlock
	id      ts.ID
	start   time.Time
	segment ts.Segment
}

// NewBlockRetriever returns a new block retriever for TSDB file sets.
func NewBlockRetriever(
	opts BlockRetrieverOptions,
	fsOpts Options,
) BlockRetriever {
	segmentReaderPool := opts.SegmentReaderPool()
	reqPoolOpts := opts.RequestPoolOptions()
	reqPool := newRetrieveRequestPool(segmentReaderPool, reqPoolOpts)
	reqPool.Init()
	return &blockRetriever{
		opts:           opts,
		fsOpts:         fsOpts,
		newSeekerMgrFn: NewSeekerManager,
		reqPool:        reqPool,
		bytesPool:      opts.BytesPool(),
		status:         blockRetrieverNotOpen,
		notifyFetch:    make(chan struct{}, 1),
	}
}

func (r *blockRetriever) Open(namespace ts.ID) error {
	r.Lock()
	defer r.Unlock()

	if r.status != blockRetrieverNotOpen {
		return errBlockRetrieverAlreadyOpenOrClosed
	}

	seekerMgrs := make([]FileSetSeekerManager, 0, r.opts.FetchConcurrency())
	for i := 0; i < r.opts.FetchConcurrency(); i++ {
		seekerMgr := r.newSeekerMgrFn(r.bytesPool, r.fsOpts)
		if err := seekerMgr.Open(namespace); err != nil {
			for _, opened := range seekerMgrs {
				opened.Close()
			}
			return err
		}
		seekerMgrs = append(seekerMgrs, seekerMgr)
	}

	r.namespace = namespace
	r.status = blockRetrieverOpen
	r.seekerMgrs = seekerMgrs

	for _, seekerMgr := range seekerMgrs {
		seekerMgr := seekerMgr
		go r.fetchLoop(seekerMgr)
	}
	return nil
}

func (r *blockRetriever) CacheShardIndices(shards []uint32) error {
	r.RLock()
	defer r.RUnlock()

	if r.status != blockRetrieverOpen {
		return errBlockRetrieverNotOpen
	}

	for _, seekerMgr := range r.seekerMgrs {
		if err := seekerMgr.CacheShardIndices(shards); err != nil {
			return err
		}
	}
	return nil
}

func (r *blockRetriever) fetchLoop(seekerMgr FileSetSeekerManager) {
	var (
		inFlight      []*retrieveRequest
		currBatchReqs []*retrieveRequest
	)
	for {
		// Free references to the inflight requests
		for i := range inFlight {
			inFlight[i] = nil
		}
		inFlight = inFlight[:0]

		// Select in flight requests
		r.RLock()
		// Move requests from shard retriever reqs into in flight slice
		for _, reqs := range r.reqsByShardIdx {
			reqs.Lock()
			if len(reqs.queued) > 0 {
				inFlight = append(inFlight, reqs.queued...)
				reqs.resetQueued()
			}
			reqs.Unlock()
		}

		status := r.status
		n := len(inFlight)
		r.RUnlock()

		// Exit if not open and fulfilled all open requests
		if n == 0 && status != blockRetrieverOpen {
			break
		}

		// If no fetches then no work to do, yield
		if n == 0 {
			<-r.notifyFetch
			continue
		}

		// NB(r): Files are all by shard and block time, the locality of
		// files is therefore foremost by block time as that is when they are
		// all written.
		sort.Sort(retrieveRequestByStartAscShardAsc(inFlight))

		// Iterate through all in flight and send them to the seeker in batches
		// of block time + shard.
		currBatchShard := uint32(0)
		currBatchStart := time.Time{}
		currBatchReqs = currBatchReqs[:0]
		for _, req := range inFlight {
			if !req.start.Equal(currBatchStart) ||
				req.shard != currBatchShard {
				// Fetch any outstanding in the current batch
				if len(currBatchReqs) > 0 {
					r.fetchBatch(seekerMgr, currBatchShard, currBatchStart, currBatchReqs)
					for i := range currBatchReqs {
						currBatchReqs[i] = nil
					}
					currBatchReqs = currBatchReqs[:0]
				}

				// Set the new batch attributes
				currBatchShard = req.shard
				currBatchStart = req.start
			}

			// Enqueue into the current batch
			currBatchReqs = append(currBatchReqs, req)
		}

		// Fetch any finally outstanding in the current batch
		if len(currBatchReqs) > 0 {
			r.fetchBatch(seekerMgr, currBatchShard, currBatchStart, currBatchReqs)
			for i := range currBatchReqs {
				currBatchReqs[i] = nil
			}
			currBatchReqs = currBatchReqs[:0]
		}
	}

	// Close the seekers
	seekerMgr.Close()
}

func (r *blockRetriever) fetchBatch(
	seekerMgr FileSetSeekerManager,
	shard uint32,
	blockStart time.Time,
	reqs []*retrieveRequest,
) {
	// Resolve the seeker from the seeker mgr
	seeker, err := seekerMgr.Seeker(shard, blockStart)
	if err != nil {
		for _, req := range reqs {
			req.onError(err)
		}
		return
	}

	// Sort the requests by offset into the file before seeking
	// to ensure all seeks are in ascending order
	for _, req := range reqs {
		req.seekOffset = seeker.SeekOffset(req.id)
	}
	sort.Sort(retrieveRequestByOffsetAsc(reqs))

	// Seek and execute all requests
	for _, req := range reqs {
		data, err := seeker.Seek(req.id)
		if err != nil && err != errSeekIDNotFound {
			req.onError(err)
			continue
		}

		var seg ts.Segment
		if data != nil {
			seg = ts.NewSegment(data, nil, ts.FinalizeHead)
		}

		if req.onRetrieve != nil {
			// NB(r): Need to also trigger callback with a copy of the data.
			// This is used by the database series to cache the in
			// memory data.
			var segCopy ts.Segment
			if data != nil {
				dataCopy := r.bytesPool.Get(data.Len())
				segCopy = ts.NewSegment(dataCopy, nil, ts.FinalizeHead)
				dataCopy.AppendAll(data.Get())
			}

			// Capture ref to req for async goroutine
			req := req
			go req.onRetrieve.OnRetrieveBlock(req.id, req.start, segCopy)
		}

		req.onRetrieved(seg)
	}
}

func (r *blockRetriever) Stream(
	shard uint32,
	id ts.ID,
	startTime time.Time,
	onRetrieve block.OnRetrieveBlock,
) (xio.SegmentReader, error) {
	reqs, err := r.shardRequests(shard)
	if err != nil {
		return nil, err
	}

	req := r.reqPool.Get()
	req.shard = shard
	req.id = id
	req.start = startTime
	req.onRetrieve = onRetrieve
	req.resultWg.Add(1)

	reqs.Lock()
	reqs.queued = append(reqs.queued, req)
	reqs.Unlock()

	// Notify fetch loop
	select {
	case r.notifyFetch <- struct{}{}:
	default:
		// Loop busy, already ready to consume notification
	}

	return req, nil
}

func (r *blockRetriever) shardRequests(
	shard uint32,
) (*shardRetrieveRequests, error) {
	r.RLock()
	if r.status != blockRetrieverOpen {
		r.RUnlock()
		return nil, errBlockRetrieverNotOpen
	}
	if int(shard) < len(r.reqsByShardIdx) {
		reqs := r.reqsByShardIdx[shard]
		r.RUnlock()
		return reqs, nil
	}
	r.RUnlock()

	r.Lock()
	defer r.Unlock()

	// Check if raced with another call to this method
	if r.status != blockRetrieverOpen {
		return nil, errBlockRetrieverNotOpen
	}
	if int(shard) < len(r.reqsByShardIdx) {
		reqs := r.reqsByShardIdx[shard]
		return reqs, nil
	}

	reqsByShardIdx := make([]*shardRetrieveRequests, shard+1)

	for i := range reqsByShardIdx {
		if i < len(r.reqsByShardIdx) {
			reqsByShardIdx[i] = r.reqsByShardIdx[i]
			continue
		}
		capacity := defaultRetrieveRequestQueueCapacity
		reqsByShardIdx[i] = &shardRetrieveRequests{
			shard:  uint32(i),
			queued: make([]*retrieveRequest, 0, capacity),
		}
	}

	r.reqsByShardIdx = reqsByShardIdx
	reqs := r.reqsByShardIdx[shard]

	return reqs, nil
}

func (r *blockRetriever) Close() error {
	r.Lock()
	defer r.Unlock()

	r.namespace = nil
	r.status = blockRetrieverClosed

	return nil
}

type shardRetrieveRequests struct {
	sync.Mutex
	shard  uint32
	queued []*retrieveRequest
}

func (reqs *shardRetrieveRequests) resetQueued() {
	// Free references to the queued requests
	for i := range reqs.queued {
		reqs.queued[i] = nil
	}
	reqs.queued = reqs.queued[:0]
}

type retrieveRequest struct {
	resultWg sync.WaitGroup
	pool     *reqPool

	shard      uint32
	id         ts.ID
	start      time.Time
	onRetrieve block.OnRetrieveBlock

	seekOffset int
	reader     xio.SegmentReader
	err        error
}

func (req *retrieveRequest) onError(err error) {
	req.err = err
	req.resultWg.Done()
}

func (req *retrieveRequest) onRetrieved(segment ts.Segment) {
	req.Reset(segment)
}

func (req *retrieveRequest) Reset(segment ts.Segment) {
	req.reader.Reset(segment)
	req.resultWg.Done()
}

func (req *retrieveRequest) Read(b []byte) (int, error) {
	req.resultWg.Wait()
	if req.err != nil {
		return 0, req.err
	}
	return req.reader.Read(b)
}

func (req *retrieveRequest) Segment() (ts.Segment, error) {
	req.resultWg.Wait()
	if req.err != nil {
		return ts.Segment{}, req.err
	}
	return req.reader.Segment()
}

func (req *retrieveRequest) Finalize() {
	req.reader.Finalize()
	req.pool.Put(req)
}

func (req *retrieveRequest) resetForReuse() {
	req.resultWg = sync.WaitGroup{}
	req.shard = 0
	req.id = nil
	req.start = time.Time{}
	req.onRetrieve = nil
	req.seekOffset = -1
	req.reader = nil
	req.err = nil
}

type retrieveRequestByStartAscShardAsc []*retrieveRequest

func (r retrieveRequestByStartAscShardAsc) Len() int      { return len(r) }
func (r retrieveRequestByStartAscShardAsc) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r retrieveRequestByStartAscShardAsc) Less(i, j int) bool {
	if !r[i].start.Equal(r[j].start) {
		return r[i].start.Before(r[j].start)
	}
	return r[i].shard < r[j].shard
}

type retrieveRequestByOffsetAsc []*retrieveRequest

func (r retrieveRequestByOffsetAsc) Len() int      { return len(r) }
func (r retrieveRequestByOffsetAsc) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r retrieveRequestByOffsetAsc) Less(i, j int) bool {
	return r[i].seekOffset < r[j].seekOffset
}

type retrieveRequestPool interface {
	Init()
	Get() *retrieveRequest
	Put(req *retrieveRequest)
}

type reqPool struct {
	segmentReaderPool xio.SegmentReaderPool
	pool              pool.ObjectPool
}

func newRetrieveRequestPool(
	segmentReaderPool xio.SegmentReaderPool,
	opts pool.ObjectPoolOptions,
) retrieveRequestPool {
	return &reqPool{
		segmentReaderPool: segmentReaderPool,
		pool:              pool.NewObjectPool(opts),
	}
}

func (p *reqPool) Init() {
	p.pool.Init(func() interface{} {
		return &retrieveRequest{pool: p}
	})
}

func (p *reqPool) Get() *retrieveRequest {
	req := p.pool.Get().(*retrieveRequest)
	req.resetForReuse()
	req.reader = p.segmentReaderPool.Get()
	return req
}

func (p *reqPool) Put(req *retrieveRequest) {
	// Also call reset for reuse to nil any references before
	// putting back in pool to avoid holding strong refs to any
	// shortly lived objects while still in the pool
	req.resetForReuse()
	p.pool.Put(req)
}
