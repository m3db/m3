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

package node

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/network/server/tchannelthrift"
	"github.com/m3db/m3db/network/server/tchannelthrift/convert"
	tterrors "github.com/m3db/m3db/network/server/tchannelthrift/errors"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3db/ts"
	xio "github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/time"

	"github.com/uber-go/tally"
	"github.com/uber/tchannel-go/thrift"
)

const (
	checkedBytesPoolSize = 65536
)

var (
	// errServerIsOverloaded raised when trying to process a request when the server is overloaded
	errServerIsOverloaded = errors.New("server is overloaded")
)

type serviceMetrics struct {
	fetch               instrument.MethodMetrics
	write               instrument.MethodMetrics
	fetchBlocks         instrument.MethodMetrics
	fetchBlocksMetadata instrument.MethodMetrics
	repair              instrument.MethodMetrics
	truncate            instrument.MethodMetrics
	fetchBatchRaw       instrument.BatchMethodMetrics
	writeBatchRaw       instrument.BatchMethodMetrics
	overloadRejected    tally.Counter
}

func newServiceMetrics(scope tally.Scope, samplingRate float64) serviceMetrics {
	return serviceMetrics{
		fetch:               instrument.NewMethodMetrics(scope, "fetch", samplingRate),
		write:               instrument.NewMethodMetrics(scope, "write", samplingRate),
		fetchBlocks:         instrument.NewMethodMetrics(scope, "fetchBlocks", samplingRate),
		fetchBlocksMetadata: instrument.NewMethodMetrics(scope, "fetchBlocksMetadata", samplingRate),
		repair:              instrument.NewMethodMetrics(scope, "repair", samplingRate),
		truncate:            instrument.NewMethodMetrics(scope, "truncate", samplingRate),
		fetchBatchRaw:       instrument.NewBatchMethodMetrics(scope, "fetchBatchRaw", samplingRate),
		writeBatchRaw:       instrument.NewBatchMethodMetrics(scope, "writeBatchRaw", samplingRate),
		overloadRejected:    scope.Counter("overload-rejected"),
	}
}

// TODO(r): server side pooling for all return types from service methods
type service struct {
	sync.RWMutex

	db                      storage.Database
	opts                    tchannelthrift.Options
	nowFn                   clock.NowFn
	metrics                 serviceMetrics
	idPool                  ts.IdentifierPool
	checkedBytesPool        pool.ObjectPool
	blockMetadataPool       tchannelthrift.BlockMetadataPool
	blockMetadataSlicePool  tchannelthrift.BlockMetadataSlicePool
	blocksMetadataPool      tchannelthrift.BlocksMetadataPool
	blocksMetadataSlicePool tchannelthrift.BlocksMetadataSlicePool
	health                  *rpc.NodeHealthResult_
}

// NewService creates a new node TChannel Thrift service
func NewService(db storage.Database, opts tchannelthrift.Options) rpc.TChanNode {
	if opts == nil {
		opts = tchannelthrift.NewOptions()
	}

	iopts := db.Options().InstrumentOptions()

	scope := iopts.MetricsScope().SubScope("service").Tagged(
		map[string]string{"serviceName": "node"},
	)

	s := &service{
		db:                      db,
		opts:                    opts,
		nowFn:                   db.Options().ClockOptions().NowFn(),
		metrics:                 newServiceMetrics(scope, iopts.MetricsSamplingRate()),
		idPool:                  db.Options().IdentifierPool(),
		blockMetadataPool:       opts.BlockMetadataPool(),
		blockMetadataSlicePool:  opts.BlockMetadataSlicePool(),
		blocksMetadataPool:      opts.BlocksMetadataPool(),
		blocksMetadataSlicePool: opts.BlocksMetadataSlicePool(),
		health: &rpc.NodeHealthResult_{
			Ok:           true,
			Status:       "up",
			Bootstrapped: false,
		},
	}
	checkedBytesPoolOpts := checked.NewBytesOptions().
		SetFinalizer(checked.BytesFinalizerFn(func(b checked.Bytes) {
			b.IncRef()
			b.Reset(nil)
			b.DecRef()
			s.checkedBytesPool.Put(b)
		}))
	s.checkedBytesPool = pool.NewObjectPool(pool.NewObjectPoolOptions().SetSize(checkedBytesPoolSize))
	s.checkedBytesPool.Init(func() interface{} {
		return checked.NewBytes(nil, checkedBytesPoolOpts)
	})

	return s
}

func (s *service) Health(ctx thrift.Context) (*rpc.NodeHealthResult_, error) {
	s.RLock()
	health := s.health
	s.RUnlock()

	// Update bootstrapped field if not up to date
	bootstrapped := s.db.IsBootstrapped()

	if health.Bootstrapped != bootstrapped {
		newHealth := &rpc.NodeHealthResult_{}
		*newHealth = *health
		newHealth.Bootstrapped = bootstrapped

		s.Lock()
		s.health = newHealth
		s.Unlock()

		// Update response
		health = newHealth
	}

	return health, nil
}

func (s *service) Fetch(tctx thrift.Context, req *rpc.FetchRequest) (*rpc.FetchResult_, error) {
	callStart := s.nowFn()
	ctx := tchannelthrift.Context(tctx)

	start, rangeStartErr := convert.ToTime(req.RangeStart, req.RangeType)
	end, rangeEndErr := convert.ToTime(req.RangeEnd, req.RangeType)

	if rangeStartErr != nil || rangeEndErr != nil {
		s.metrics.fetch.ReportError(s.nowFn().Sub(callStart))
		return nil, tterrors.NewBadRequestError(xerrors.FirstError(rangeStartErr, rangeEndErr))
	}

	encoded, err := s.db.ReadEncoded(ctx,
		s.idPool.GetStringID(ctx, req.NameSpace),
		s.idPool.GetStringID(ctx, req.ID),
		start, end)
	if err != nil {
		s.metrics.fetch.ReportError(s.nowFn().Sub(callStart))
		rpcErr := convert.ToRPCError(err)
		return nil, rpcErr
	}

	result := rpc.NewFetchResult_()

	// Make datapoints an initialized empty array for JSON serialization as empty array than null
	result.Datapoints = make([]*rpc.Datapoint, 0)

	multiIt := s.db.Options().MultiReaderIteratorPool().Get()
	multiIt.ResetSliceOfSlices(xio.NewReaderSliceOfSlicesFromSegmentReadersIterator(encoded))
	it := encoding.NewSeriesIterator(req.ID, start, end, []encoding.Iterator{multiIt}, nil)
	defer it.Close()

	for it.Next() {
		dp, _, annotation := it.Current()

		timestamp, timestampErr := convert.ToValue(dp.Timestamp, req.ResultTimeType)
		if timestampErr != nil {
			s.metrics.fetch.ReportError(s.nowFn().Sub(callStart))
			return nil, tterrors.NewBadRequestError(timestampErr)
		}

		datapoint := rpc.NewDatapoint()
		datapoint.Timestamp = timestamp
		datapoint.Value = dp.Value
		datapoint.Annotation = annotation

		result.Datapoints = append(result.Datapoints, datapoint)
	}

	if err := it.Err(); err != nil {
		s.metrics.fetch.ReportError(s.nowFn().Sub(callStart))
		return nil, tterrors.NewInternalError(err)
	}

	s.metrics.fetch.ReportSuccess(s.nowFn().Sub(callStart))

	return result, nil
}

func (s *service) FetchBatchRaw(tctx thrift.Context, req *rpc.FetchBatchRawRequest) (*rpc.FetchBatchRawResult_, error) {
	callStart := s.nowFn()
	ctx := tchannelthrift.Context(tctx)

	start, rangeStartErr := convert.ToTime(req.RangeStart, req.RangeType)
	end, rangeEndErr := convert.ToTime(req.RangeEnd, req.RangeType)

	if rangeStartErr != nil || rangeEndErr != nil {
		s.metrics.fetchBatchRaw.ReportNonRetryableErrors(len(req.Ids))
		s.metrics.fetchBatchRaw.ReportLatency(s.nowFn().Sub(callStart))
		return nil, tterrors.NewBadRequestError(xerrors.FirstError(rangeStartErr, rangeEndErr))
	}

	nsID := s.newID(ctx, req.NameSpace)

	result := rpc.NewFetchBatchRawResult_()

	var (
		success            int
		retryableErrors    int
		nonRetryableErrors int
	)

	for i := range req.Ids {
		rawResult := rpc.NewFetchRawResult_()
		result.Elements = append(result.Elements, rawResult)

		tsID := s.newID(ctx, req.Ids[i])
		encoded, err := s.db.ReadEncoded(ctx, nsID, tsID, start, end)
		if err != nil {
			rawResult.Err = convert.ToRPCError(err)
			if tterrors.IsBadRequestError(rawResult.Err) {
				nonRetryableErrors++
			} else {
				retryableErrors++
			}
			continue
		}

		var streamErr error
		segments := make([]*rpc.Segments, 0, len(encoded))
		for _, readers := range encoded {
			var seg *rpc.Segments
			seg, streamErr = convert.ToSegments(readers)
			if streamErr != nil {
				rawResult.Err = convert.ToRPCError(err)
				if tterrors.IsBadRequestError(rawResult.Err) {
					nonRetryableErrors++
				} else {
					retryableErrors++
				}
				break
			}
			if seg == nil {
				continue
			}
			segments = append(segments, seg)
		}
		if streamErr != nil {
			continue
		}

		success++

		rawResult.Segments = segments
	}

	s.metrics.fetchBatchRaw.ReportSuccess(success)
	s.metrics.fetchBatchRaw.ReportRetryableErrors(retryableErrors)
	s.metrics.fetchBatchRaw.ReportNonRetryableErrors(nonRetryableErrors)
	s.metrics.fetchBatchRaw.ReportLatency(s.nowFn().Sub(callStart))

	return result, nil
}

func (s *service) FetchBlocksRaw(tctx thrift.Context, req *rpc.FetchBlocksRawRequest) (*rpc.FetchBlocksRawResult_, error) {
	if s.db.IsOverloaded() {
		s.metrics.overloadRejected.Inc(1)
		return nil, tterrors.NewInternalError(errServerIsOverloaded)
	}

	callStart := s.nowFn()
	ctx := tchannelthrift.Context(tctx)

	nsID := s.newID(ctx, req.NameSpace)

	res := rpc.NewFetchBlocksRawResult_()
	res.Elements = make([]*rpc.Blocks, len(req.Elements))

	// Preallocate starts to maximum size since at least one element will likely
	// be fetching most blocks for peer bootstrapping
	ropts := s.db.Options().RetentionOptions()
	blockStarts := make([]time.Time, 0, ropts.RetentionPeriod()/ropts.BlockSize())

	for i, request := range req.Elements {
		blockStarts = blockStarts[:0]

		for _, start := range request.Starts {
			blockStarts = append(blockStarts, xtime.FromNanoseconds(start))
		}

		tsID := s.newID(ctx, request.ID)
		fetched, err := s.db.FetchBlocks(
			ctx, nsID, uint32(req.Shard), tsID, blockStarts)
		if err != nil {
			s.metrics.fetchBlocks.ReportError(s.nowFn().Sub(callStart))
			return nil, convert.ToRPCError(err)
		}

		blocks := rpc.NewBlocks()
		blocks.ID = request.ID
		blocks.Blocks = make([]*rpc.Block, 0, len(fetched))

		for _, fetchedBlock := range fetched {
			block := rpc.NewBlock()
			block.Start = xtime.ToNanoseconds(fetchedBlock.Start())
			if ck := fetchedBlock.Checksum(); ck != nil {
				cki := int64(*ck)
				block.Checksum = &cki
			}
			if err := fetchedBlock.Err(); err != nil {
				block.Err = convert.ToRPCError(err)
			} else {
				block.Segments, err = convert.ToSegments(fetchedBlock.Readers())
				if err != nil {
					block.Err = convert.ToRPCError(err)
				}
				if block.Segments == nil {
					// No data for block, skip this block
					continue
				}
			}

			blocks.Blocks = append(blocks.Blocks, block)
		}

		res.Elements[i] = blocks
	}

	s.metrics.fetchBlocks.ReportSuccess(s.nowFn().Sub(callStart))

	return res, nil
}

func (s *service) FetchBlocksMetadataRaw(tctx thrift.Context, req *rpc.FetchBlocksMetadataRawRequest) (*rpc.FetchBlocksMetadataRawResult_, error) {
	if s.db.IsOverloaded() {
		s.metrics.overloadRejected.Inc(1)
		return nil, tterrors.NewInternalError(errServerIsOverloaded)
	}

	callStart := s.nowFn()
	ctx := tchannelthrift.Context(tctx)

	start := time.Unix(0, req.RangeStart)
	end := time.Unix(0, req.RangeEnd)

	if req.Limit <= 0 {
		s.metrics.fetchBlocksMetadata.ReportSuccess(s.nowFn().Sub(callStart))
		return nil, nil
	}

	var pageToken int64
	if req.PageToken != nil {
		pageToken = *req.PageToken
	}

	var includeSizes bool
	if req.IncludeSizes != nil {
		includeSizes = *req.IncludeSizes
	}

	var includeChecksums bool
	if req.IncludeChecksums != nil {
		includeChecksums = *req.IncludeChecksums
	}

	nsID := s.newID(ctx, req.NameSpace)

	fetched, nextPageToken, err := s.db.FetchBlocksMetadata(ctx,
		nsID, uint32(req.Shard), start, end,
		req.Limit, pageToken, includeSizes, includeChecksums)
	if err != nil {
		s.metrics.fetchBlocksMetadata.ReportError(s.nowFn().Sub(callStart))
		return nil, convert.ToRPCError(err)
	}
	ctx.RegisterFinalizer(context.FinalizerFn(fetched.Close))

	fetchedResults := fetched.Results()
	result := rpc.NewFetchBlocksMetadataRawResult_()
	result.NextPageToken = nextPageToken
	result.Elements = s.blocksMetadataSlicePool.Get()

	// NB(xichen): register a closer with context so objects are returned to pool
	// when we are done using them
	ctx.RegisterFinalizer(s.newCloseableMetadataResult(result))

	for _, fetchedMetadata := range fetchedResults {
		blocksMetadata := s.blocksMetadataPool.Get()
		blocksMetadata.ID = fetchedMetadata.ID.Data().Get()
		fetchedMetadataBlocks := fetchedMetadata.Blocks.Results()
		blocksMetadata.Blocks = s.blockMetadataSlicePool.Get()

		for _, fetchedMetadataBlock := range fetchedMetadataBlocks {
			blockMetadata := s.blockMetadataPool.Get()
			blockMetadata.Start = xtime.ToNanoseconds(fetchedMetadataBlock.Start)
			blockMetadata.Size = fetchedMetadataBlock.Size

			if checksum := fetchedMetadataBlock.Checksum; checksum != nil {
				value := int64(*checksum)
				blockMetadata.Checksum = &value
			}

			if err := fetchedMetadataBlock.Err; err != nil {
				blockMetadata.Err = convert.ToRPCError(err)
			}

			blocksMetadata.Blocks = append(blocksMetadata.Blocks, blockMetadata)
		}

		result.Elements = append(result.Elements, blocksMetadata)
	}

	s.metrics.fetchBlocksMetadata.ReportSuccess(s.nowFn().Sub(callStart))

	return result, nil
}

func (s *service) Write(tctx thrift.Context, req *rpc.WriteRequest) error {
	callStart := s.nowFn()
	ctx := tchannelthrift.Context(tctx)

	if req.Datapoint == nil {
		s.metrics.write.ReportError(s.nowFn().Sub(callStart))
		return tterrors.NewBadRequestError(fmt.Errorf("requires datapoint"))
	}

	dp := req.Datapoint
	unit, unitErr := convert.ToUnit(dp.TimestampType)

	if unitErr != nil {
		s.metrics.write.ReportError(s.nowFn().Sub(callStart))
		return tterrors.NewBadRequestError(unitErr)
	}

	d, err := unit.Value()
	if err != nil {
		s.metrics.write.ReportError(s.nowFn().Sub(callStart))
		return tterrors.NewBadRequestError(err)
	}

	if err = s.db.Write(
		ctx, s.idPool.GetStringID(ctx, req.NameSpace), s.idPool.GetStringID(ctx, req.ID),
		xtime.FromNormalizedTime(dp.Timestamp, d), dp.Value, unit, dp.Annotation,
	); err != nil {
		s.metrics.write.ReportError(s.nowFn().Sub(callStart))
		return convert.ToRPCError(err)
	}

	s.metrics.write.ReportSuccess(s.nowFn().Sub(callStart))

	return nil
}

func (s *service) WriteBatchRaw(tctx thrift.Context, req *rpc.WriteBatchRawRequest) error {
	callStart := s.nowFn()
	ctx := tchannelthrift.Context(tctx)

	nsID := s.newID(ctx, req.NameSpace)

	var (
		errs               []*rpc.WriteBatchRawError
		success            int
		retryableErrors    int
		nonRetryableErrors int
	)
	for i, elem := range req.Elements {
		unit, unitErr := convert.ToUnit(elem.Datapoint.TimestampType)
		if unitErr != nil {
			nonRetryableErrors++
			errs = append(errs, tterrors.NewBadRequestWriteBatchRawError(i, unitErr))
			continue
		}

		d, err := unit.Value()
		if err != nil {
			nonRetryableErrors++
			errs = append(errs, tterrors.NewBadRequestWriteBatchRawError(i, err))
			continue
		}

		if err = s.db.Write(
			ctx, nsID, s.newID(ctx, elem.ID),
			xtime.FromNormalizedTime(elem.Datapoint.Timestamp, d),
			elem.Datapoint.Value, unit, elem.Datapoint.Annotation,
		); err != nil && xerrors.IsInvalidParams(err) {
			nonRetryableErrors++
			errs = append(errs, tterrors.NewBadRequestWriteBatchRawError(i, err))
		} else if err != nil {
			retryableErrors++
			errs = append(errs, tterrors.NewWriteBatchRawError(i, err))
		} else {
			success++
		}
	}

	s.metrics.writeBatchRaw.ReportSuccess(success)
	s.metrics.writeBatchRaw.ReportRetryableErrors(retryableErrors)
	s.metrics.writeBatchRaw.ReportNonRetryableErrors(nonRetryableErrors)
	s.metrics.writeBatchRaw.ReportLatency(s.nowFn().Sub(callStart))

	if len(errs) > 0 {
		batchErrs := rpc.NewWriteBatchRawErrors()
		batchErrs.Errors = errs
		return batchErrs
	}

	return nil
}

func (s *service) Repair(tctx thrift.Context) error {
	callStart := s.nowFn()

	if err := s.db.Repair(); err != nil {
		s.metrics.repair.ReportError(s.nowFn().Sub(callStart))
		return convert.ToRPCError(err)
	}

	s.metrics.repair.ReportSuccess(s.nowFn().Sub(callStart))

	return nil
}

func (s *service) Truncate(tctx thrift.Context, req *rpc.TruncateRequest) (r *rpc.TruncateResult_, err error) {
	callStart := s.nowFn()
	ctx := tchannelthrift.Context(tctx)
	truncated, err := s.db.Truncate(s.newID(ctx, req.NameSpace))

	if err != nil {
		s.metrics.truncate.ReportError(s.nowFn().Sub(callStart))
		return nil, convert.ToRPCError(err)
	}

	res := rpc.NewTruncateResult_()
	res.NumSeries = truncated

	s.metrics.truncate.ReportSuccess(s.nowFn().Sub(callStart))

	return res, nil
}

func (s *service) GetPersistRateLimit(
	ctx thrift.Context,
) (*rpc.NodePersistRateLimitResult_, error) {
	opts := s.db.Options().PersistManager().RateLimitOptions()
	limitEnabled := opts.LimitEnabled()
	limitMbps := opts.LimitMbps()
	limitCheckEvery := int64(opts.LimitCheckEvery())
	result := &rpc.NodePersistRateLimitResult_{
		LimitEnabled:    limitEnabled,
		LimitMbps:       limitMbps,
		LimitCheckEvery: limitCheckEvery,
	}
	return result, nil
}

func (s *service) SetPersistRateLimit(
	ctx thrift.Context,
	req *rpc.NodeSetPersistRateLimitRequest,
) (*rpc.NodePersistRateLimitResult_, error) {
	persistManager := s.db.Options().PersistManager()

	opts := persistManager.RateLimitOptions()
	if req.LimitEnabled != nil {
		opts = opts.SetLimitEnabled(*req.LimitEnabled)
	}
	if req.LimitMbps != nil {
		opts = opts.SetLimitMbps(*req.LimitMbps)
	}
	if req.LimitCheckEvery != nil {
		opts = opts.SetLimitCheckEvery(int(*req.LimitCheckEvery))
	}

	persistManager.SetRateLimitOptions(opts)

	return s.GetPersistRateLimit(ctx)
}

func (s *service) isOverloaded() bool {
	// NB(xichen): for now we only use the database load to determine
	// whether the server is overloaded. In the future we may also take
	// into account other metrics such as CPU load, disk I/O rate, etc.
	return s.db.IsOverloaded()
}

func (s *service) newID(ctx context.Context, id []byte) ts.ID {
	checkedBytes := s.checkedBytesPool.Get().(checked.Bytes)
	checkedBytes.IncRef()
	checkedBytes.Reset(id)
	checkedBytes.DecRef()

	return s.idPool.GetBinaryID(ctx, checkedBytes)
}

func (s *service) newCloseableMetadataResult(
	res *rpc.FetchBlocksMetadataRawResult_,
) closeableMetadataResult {
	return closeableMetadataResult{s: s, result: res}
}

type closeableMetadataResult struct {
	s      *service
	result *rpc.FetchBlocksMetadataRawResult_
}

func (c closeableMetadataResult) Finalize() {
	for _, blocksMetadata := range c.result.Elements {
		for _, blockMetadata := range blocksMetadata.Blocks {
			c.s.blockMetadataPool.Put(blockMetadata)
		}
		c.s.blockMetadataSlicePool.Put(blocksMetadata.Blocks)
		c.s.blocksMetadataPool.Put(blocksMetadata)
	}
	c.s.blocksMetadataSlicePool.Put(c.result.Elements)
}
