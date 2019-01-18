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

// The block retriever is used to stream blocks of data from disk. It controls
// the fetch concurrency on a per-Namespace basis I.E if the server is using
// spinning-disks the concurrency can be set to 1 to serialize all disk fetches
// for a given namespace, and the concurrency be set higher in the case of SSDs.
// This fetch concurrency is primarily implemented via the number of concurrent
// fetchLoops that the retriever creates.
//
// The block retriever also handles batching of requests for data, as well as
// re-arranging the order of requests to increase data locality when seeking
// through and across files.

package fs

import (
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/pool"
)

var (
	errBlockRetrieverNotOpen             = errors.New("block retriever is not open")
	errBlockRetrieverAlreadyOpenOrClosed = errors.New("block retriever already open or is closed")
	errBlockRetrieverAlreadyClosed       = errors.New("block retriever already closed")
	errNoSeekerMgr                       = errors.New("there is no open seeker manager")
)

const (
	defaultRetrieveRequestQueueCapacity = 4096
)

type blockRetrieverStatus int

type newSeekerMgrFn func(
	bytesPool pool.CheckedBytesPool,
	opts Options,
	fetchConcurrency int,
) DataFileSetSeekerManager

const (
	blockRetrieverNotOpen blockRetrieverStatus = iota
	blockRetrieverOpen
	blockRetrieverClosed
)

type blockRetriever struct {
	sync.RWMutex

	opts   BlockRetrieverOptions
	fsOpts Options
	logger log.Logger

	newSeekerMgrFn newSeekerMgrFn

	reqPool    retrieveRequestPool
	bytesPool  pool.CheckedBytesPool
	idPool     ident.Pool
	nsMetadata namespace.Metadata

	blockSize time.Duration

	status                     blockRetrieverStatus
	reqsByShardIdx             []*shardRetrieveRequests
	seekerMgr                  DataFileSetSeekerManager
	notifyFetch                chan struct{}
	fetchLoopsShouldShutdownCh chan struct{}
	fetchLoopsHaveShutdownCh   chan struct{}
}

// NewBlockRetriever returns a new block retriever for TSDB file sets.
func NewBlockRetriever(
	opts BlockRetrieverOptions,
	fsOpts Options,
) DataBlockRetriever {
	segmentReaderPool := opts.SegmentReaderPool()
	reqPoolOpts := opts.RequestPoolOptions()
	reqPool := newRetrieveRequestPool(segmentReaderPool, reqPoolOpts)
	reqPool.Init()
	return &blockRetriever{
		opts:           opts,
		fsOpts:         fsOpts,
		logger:         fsOpts.InstrumentOptions().Logger(),
		newSeekerMgrFn: NewSeekerManager,
		reqPool:        reqPool,
		bytesPool:      opts.BytesPool(),
		idPool:         opts.IdentifierPool(),
		status:         blockRetrieverNotOpen,
		notifyFetch:    make(chan struct{}, 1),
		// We just close this channel when the fetchLoops should shutdown, so no
		// buffering is required
		fetchLoopsShouldShutdownCh: make(chan struct{}),
		fetchLoopsHaveShutdownCh:   make(chan struct{}, opts.FetchConcurrency()),
	}
}

func (r *blockRetriever) Open(ns namespace.Metadata) error {
	r.Lock()
	defer r.Unlock()

	if r.status != blockRetrieverNotOpen {
		return errBlockRetrieverAlreadyOpenOrClosed
	}

	seekerMgr := r.newSeekerMgrFn(r.bytesPool, r.fsOpts, r.opts.FetchConcurrency())
	if err := seekerMgr.Open(ns); err != nil {
		return err
	}

	r.nsMetadata = ns
	r.status = blockRetrieverOpen
	r.seekerMgr = seekerMgr

	// Cache blockSize result
	r.blockSize = ns.Options().RetentionOptions().BlockSize()

	for i := 0; i < r.opts.FetchConcurrency(); i++ {
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
	return r.seekerMgr.CacheShardIndices(shards)
}

func (r *blockRetriever) fetchLoop(seekerMgr DataFileSetSeekerManager) {
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
			select {
			case <-r.notifyFetch:
				continue
			case <-r.fetchLoopsShouldShutdownCh:
				break
			}
		}

		// Files are all by shard and block time, the locality of
		// files is therefore foremost by block time as that is when they are
		// all written. Note that this sort does NOT mean that we're going to stripe
		// through different files at once as you might expect at first, but simply
		// that since all the fileset files are written at the end of a block period
		// those files are more likely to be physically located close to each other
		// on disk. In other words, instead of accessing files like this:
		// 		shard1T1 --> shard1T2 --> shard1T3 --> shard2T1 --> shard2T2 --> shard2T3
		// its probably faster to access them like this:
		// 		shard1T1 --> shard2T1 --> shard1T2 --> shard2T2 --> shard1T3 --> shard2T3
		// so we re-arrange the order of the requests to achieve that
		sort.Sort(retrieveRequestByStartAscShardAsc(inFlight))

		// Iterate through all in flight requests and send them to the seeker in
		// batches of block time + shard.
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

	r.fetchLoopsHaveShutdownCh <- struct{}{}
}

func (r *blockRetriever) fetchBatch(
	seekerMgr DataFileSetSeekerManager,
	shard uint32,
	blockStart time.Time,
	reqs []*retrieveRequest,
) {
	// Resolve the seeker from the seeker mgr
	seeker, err := seekerMgr.Borrow(shard, blockStart)
	if err != nil {
		for _, req := range reqs {
			req.onError(err)
		}
		return
	}

	// Sort the requests by offset into the file before seeking
	// to ensure all seeks are in ascending order
	for _, req := range reqs {
		entry, err := seeker.SeekIndexEntry(req.id)
		if err != nil && err != errSeekIDNotFound {
			req.onError(err)
			continue
		}

		if err == errSeekIDNotFound {
			req.notFound = true
		}
		req.indexEntry = entry
	}
	sort.Sort(retrieveRequestByOffsetAsc(reqs))

	tagDecoderPool := r.fsOpts.TagDecoderPool()

	// Seek and execute all requests
	for _, req := range reqs {
		var data checked.Bytes
		var err error

		// Only try to seek the ID if it exists, otherwise we'll get a checksum
		// mismatch error because default offset value for indexEntry is zero.
		if !req.notFound {
			data, err = seeker.SeekByIndexEntry(req.indexEntry)
			if err != nil && err != errSeekIDNotFound {
				req.onError(err)
				continue
			}
		}

		var (
			seg, onRetrieveSeg ts.Segment
		)
		if data != nil {
			seg = ts.NewSegment(data, nil, ts.FinalizeHead)
		}

		// We don't need to call onRetrieve.OnRetrieveBlock if the ID was not found
		callOnRetrieve := req.onRetrieve != nil && !req.notFound
		if callOnRetrieve {
			// NB(r): Need to also trigger callback with a copy of the data.
			// This is used by the database to cache the in memory data for
			// consequent fetches.
			if data != nil {
				dataCopy := r.bytesPool.Get(data.Len())
				onRetrieveSeg = ts.NewSegment(dataCopy, nil, ts.FinalizeHead)
				dataCopy.AppendAll(data.Bytes())
			}
			if tags := req.indexEntry.EncodedTags; len(tags) != 0 {
				tagsCopy := r.bytesPool.Get(len(tags))
				tagsCopy.IncRef()
				tagsCopy.AppendAll(req.indexEntry.EncodedTags)
				tagsCopy.DecRef()
				decoder := tagDecoderPool.Get()
				decoder.Reset(tagsCopy)
				req.tags = decoder
			}
		}

		// Complete request
		req.onRetrieved(seg)

		if !callOnRetrieve {
			// No need to call the onRetrieve callback
			req.onCallerOrRetrieverDone()
			continue
		}

		go func(r *retrieveRequest) {
			// Call the onRetrieve callback and finalize
			r.onRetrieve.OnRetrieveBlock(r.id, r.tags, r.start, onRetrieveSeg)
			r.onCallerOrRetrieverDone()
		}(req)
	}

	err = seekerMgr.Return(shard, blockStart, seeker)
	if err != nil {
		r.logger.WithFields(
			log.NewField("shard", shard),
			log.NewField("blockStart", blockStart.Unix()),
			log.NewField("err", err.Error()),
		).Error("err returning seeker for shard")
	}
}

func (r *blockRetriever) Stream(
	ctx context.Context,
	shard uint32,
	id ident.ID,
	startTime time.Time,
	onRetrieve block.OnRetrieveBlock,
) (xio.BlockReader, error) {
	req := r.reqPool.Get()
	req.shard = shard
	// NB(r): Clone the ID as we're not positive it will stay valid throughout
	// the lifecycle of the async request.
	req.id = r.idPool.Clone(id)
	req.start = startTime
	req.blockSize = r.blockSize

	req.onRetrieve = onRetrieve
	req.resultWg.Add(1)

	// Ensure to finalize at the end of request
	ctx.RegisterFinalizer(req)

	// Capture variable and RLock() because this slice can be modified in the
	// Open() method
	r.RLock()
	// This should never happen unless caller tries to use Stream() before Open()
	if r.seekerMgr == nil {
		r.RUnlock()
		return xio.EmptyBlockReader, errNoSeekerMgr
	}
	r.RUnlock()

	bloomFilter, err := r.seekerMgr.ConcurrentIDBloomFilter(shard, startTime)
	if err != nil {
		return xio.EmptyBlockReader, err
	}

	// If the ID is not in the seeker's bloom filter, then it's definitely not on
	// disk and we can return immediately
	if !bloomFilter.Test(id.Bytes()) {
		// No need to call req.onRetrieve.OnRetrieveBlock if there is no data
		req.onRetrieved(ts.Segment{})
		return req.toBlock(), nil
	}
	reqs, err := r.shardRequests(shard)
	if err != nil {
		return xio.EmptyBlockReader, err
	}

	reqs.Lock()
	reqs.queued = append(reqs.queued, req)
	reqs.Unlock()

	// Notify fetch loop
	select {
	case r.notifyFetch <- struct{}{}:
	default:
		// Loop busy, already ready to consume notification
	}

	return req.toBlock(), nil
}

func (req *retrieveRequest) toBlock() xio.BlockReader {
	return xio.BlockReader{
		SegmentReader: req,
		Start:         req.start,
		BlockSize:     req.blockSize,
	}
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
	if r.status == blockRetrieverClosed {
		r.Unlock()
		return errBlockRetrieverAlreadyClosed
	}
	r.nsMetadata = nil
	r.status = blockRetrieverClosed

	r.blockSize = 0
	r.Unlock()

	close(r.fetchLoopsShouldShutdownCh)
	for i := 0; i < r.opts.FetchConcurrency(); i++ {
		<-r.fetchLoopsHaveShutdownCh
	}

	return r.seekerMgr.Close()
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

// Don't forget to update the resetForReuse method when adding a new field
type retrieveRequest struct {
	resultWg sync.WaitGroup

	pool *reqPool

	id         ident.ID
	tags       ident.TagIterator
	start      time.Time
	blockSize  time.Duration
	onRetrieve block.OnRetrieveBlock

	indexEntry IndexEntry
	reader     xio.SegmentReader

	err error

	// Finalize requires two calls to finalize (once both the user of the
	// request and the retriever fetch loop is done, and only then, can
	// we free this request) so we track this with an atomic here.
	finalizes uint32
	shard     uint32

	notFound bool
}

func (req *retrieveRequest) onError(err error) {
	req.err = err
	req.resultWg.Done()
}

func (req *retrieveRequest) onRetrieved(segment ts.Segment) {
	req.Reset(segment)
}

func (req *retrieveRequest) onCallerOrRetrieverDone() {
	if atomic.AddUint32(&req.finalizes, 1) != 2 {
		return
	}
	req.id.Finalize()
	req.id = nil
	if req.tags != nil {
		req.tags.Close()
		req.tags = ident.EmptyTagIterator
	}
	req.reader.Finalize()
	req.reader = nil
	req.pool.Put(req)
}

func (req *retrieveRequest) Reset(segment ts.Segment) {
	req.reader.Reset(segment)
	req.resultWg.Done()
}

func (req *retrieveRequest) ResetWindowed(segment ts.Segment, start time.Time, blockSize time.Duration) {
	req.Reset(segment)
	req.start = start
	req.blockSize = blockSize
}

func (req *retrieveRequest) SegmentReader() (xio.SegmentReader, error) {
	return req.reader, nil
}

func (req *retrieveRequest) Clone() (xio.SegmentReader, error) {
	req.resultWg.Wait() // wait until result is ready
	if req.err != nil {
		return nil, req.err
	}
	return req.reader.Clone()
}

func (req *retrieveRequest) DeepClone(
	pool pool.CheckedBytesPool,
) (xio.SegmentReader, error) {
	req.resultWg.Wait() // wait until result is ready
	if req.err != nil {
		return nil, req.err
	}
	return req.reader.DeepClone(pool)
}

func (req *retrieveRequest) Start() time.Time {
	return req.start
}

func (req *retrieveRequest) BlockSize() time.Duration {
	return req.blockSize
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
	// May not actually finalize the request, depending on if
	// retriever is done too
	req.onCallerOrRetrieverDone()
}

func (req *retrieveRequest) resetForReuse() {
	req.resultWg = sync.WaitGroup{}
	req.finalizes = 0
	req.shard = 0
	req.id = nil
	req.tags = ident.EmptyTagIterator
	req.start = time.Time{}
	req.blockSize = 0
	req.onRetrieve = nil
	req.indexEntry = IndexEntry{}
	req.reader = nil
	req.err = nil
	req.notFound = false
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
	return r[i].indexEntry.Offset < r[j].indexEntry.Offset
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
