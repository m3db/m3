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
	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/network/server/tchannelthrift"
	"github.com/m3db/m3db/network/server/tchannelthrift/convert"
	tterrors "github.com/m3db/m3db/network/server/tchannelthrift/errors"
	"github.com/m3db/m3db/serialize"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/x/xio"
	"github.com/m3db/m3db/x/xpool"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/context"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/resource"
	xtime "github.com/m3db/m3x/time"

	"github.com/uber-go/tally"
	"github.com/uber/tchannel-go/thrift"
)

const (
	checkedBytesPoolSize        = 65536
	segmentArrayPoolSize        = 65536
	initSegmentArrayPoolLength  = 4
	maxSegmentArrayPooledLength = 32
)

var (
	// errServerIsOverloaded raised when trying to process a request when the server is overloaded
	errServerIsOverloaded = errors.New("server is overloaded")

	// errIllegalTagValues raised when the tags specified are in-correct
	errIllegalTagValues = errors.New("illegal tag values specified")

	// errRequiresDatapoint raised when a datapoint is not provided
	errRequiresDatapoint = fmt.Errorf("requires datapoint")
)

type serviceMetrics struct {
	fetch               instrument.MethodMetrics
	fetchTagged         instrument.MethodMetrics
	write               instrument.MethodMetrics
	writeTagged         instrument.MethodMetrics
	fetchBlocks         instrument.MethodMetrics
	fetchBlocksMetadata instrument.MethodMetrics
	repair              instrument.MethodMetrics
	truncate            instrument.MethodMetrics
	fetchBatchRaw       instrument.BatchMethodMetrics
	writeBatchRaw       instrument.BatchMethodMetrics
	writeTaggedBatchRaw instrument.BatchMethodMetrics
	overloadRejected    tally.Counter
}

func newServiceMetrics(scope tally.Scope, samplingRate float64) serviceMetrics {
	return serviceMetrics{
		fetch:               instrument.NewMethodMetrics(scope, "fetch", samplingRate),
		fetchTagged:         instrument.NewMethodMetrics(scope, "fetchTagged", samplingRate),
		write:               instrument.NewMethodMetrics(scope, "write", samplingRate),
		writeTagged:         instrument.NewMethodMetrics(scope, "writeTagged", samplingRate),
		fetchBlocks:         instrument.NewMethodMetrics(scope, "fetchBlocks", samplingRate),
		fetchBlocksMetadata: instrument.NewMethodMetrics(scope, "fetchBlocksMetadata", samplingRate),
		repair:              instrument.NewMethodMetrics(scope, "repair", samplingRate),
		truncate:            instrument.NewMethodMetrics(scope, "truncate", samplingRate),
		fetchBatchRaw:       instrument.NewBatchMethodMetrics(scope, "fetchBatchRaw", samplingRate),
		writeBatchRaw:       instrument.NewBatchMethodMetrics(scope, "writeBatchRaw", samplingRate),
		writeTaggedBatchRaw: instrument.NewBatchMethodMetrics(scope, "writeTaggedBatchRaw", samplingRate),
		overloadRejected:    scope.Counter("overload-rejected"),
	}
}

// TODO(r): server side pooling for all return types from service methods
type service struct {
	sync.RWMutex

	db      storage.Database
	logger  log.Logger
	opts    tchannelthrift.Options
	nowFn   clock.NowFn
	pools   pools
	metrics serviceMetrics
	health  *rpc.NodeHealthResult_
}

type pools struct {
	id                   ident.Pool
	tagEncoder           serialize.TagEncoderPool
	tagDecoder           serialize.TagDecoderPool
	checkedBytesWrapper  xpool.CheckedBytesWrapperPool
	segmentsArray        segmentsArrayPool
	blockMetadata        tchannelthrift.BlockMetadataPool
	blockMetadataV2      tchannelthrift.BlockMetadataV2Pool
	blockMetadataSlice   tchannelthrift.BlockMetadataSlicePool
	blockMetadataV2Slice tchannelthrift.BlockMetadataV2SlicePool
	blocksMetadata       tchannelthrift.BlocksMetadataPool
	blocksMetadataSlice  tchannelthrift.BlocksMetadataSlicePool
}

// ensure `pools` matches a required conversion interface
var _ convert.FetchTaggedConversionPools = pools{}

func (p pools) ID() ident.Pool                                     { return p.id }
func (p pools) CheckedBytesWrapper() xpool.CheckedBytesWrapperPool { return p.checkedBytesWrapper }

// NewService creates a new node TChannel Thrift service
func NewService(db storage.Database, opts tchannelthrift.Options) rpc.TChanNode {
	if opts == nil {
		opts = tchannelthrift.NewOptions()
	}

	iopts := opts.InstrumentOptions()

	scope := iopts.
		MetricsScope().
		SubScope("service").
		Tagged(map[string]string{"serviceName": "node"})

	wrapperPoolOpts := pool.NewObjectPoolOptions().
		SetSize(checkedBytesPoolSize).
		SetInstrumentOptions(iopts.SetMetricsScope(
			scope.SubScope("node-checked-bytes-wrapper-pool")))
	wrapperPool := xpool.NewCheckedBytesWrapperPool(wrapperPoolOpts)
	wrapperPool.Init()

	segmentPool := newSegmentsArrayPool(segmentsArrayPoolOpts{
		Capacity:    initSegmentArrayPoolLength,
		MaxCapacity: maxSegmentArrayPooledLength,
		Options: pool.NewObjectPoolOptions().
			SetSize(segmentArrayPoolSize).
			SetInstrumentOptions(iopts.SetMetricsScope(
				scope.SubScope("segment-array-pool"))),
	})
	segmentPool.Init()

	s := &service{
		db:      db,
		logger:  iopts.Logger(),
		opts:    opts,
		nowFn:   db.Options().ClockOptions().NowFn(),
		metrics: newServiceMetrics(scope, iopts.MetricsSamplingRate()),
		pools: pools{
			checkedBytesWrapper:  wrapperPool,
			tagEncoder:           opts.TagEncoderPool(),
			tagDecoder:           opts.TagDecoderPool(),
			id:                   db.Options().IdentifierPool(),
			segmentsArray:        segmentPool,
			blockMetadata:        opts.BlockMetadataPool(),
			blockMetadataV2:      opts.BlockMetadataV2Pool(),
			blockMetadataSlice:   opts.BlockMetadataSlicePool(),
			blockMetadataV2Slice: opts.BlockMetadataV2SlicePool(),
			blocksMetadata:       opts.BlocksMetadataPool(),
			blocksMetadataSlice:  opts.BlocksMetadataSlicePool(),
		},
		health: &rpc.NodeHealthResult_{
			Ok:           true,
			Status:       "up",
			Bootstrapped: false,
		},
	}

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

	tsID := s.pools.id.GetStringID(ctx, req.ID)
	nsID := s.pools.id.GetStringID(ctx, req.NameSpace)
	encoded, err := s.db.ReadEncoded(ctx, nsID, tsID, start, end)
	if err != nil {
		s.metrics.fetch.ReportError(s.nowFn().Sub(callStart))
		rpcErr := convert.ToRPCError(err)
		return nil, rpcErr
	}

	result := rpc.NewFetchResult_()

	// Make datapoints an initialized empty array for JSON serialization as empty array than null
	result.Datapoints = make([]*rpc.Datapoint, 0)

	multiIt := s.db.Options().MultiReaderIteratorPool().Get()
	multiIt.ResetSliceOfSlices(xio.NewReaderSliceOfSlicesFromBlockReadersIterator(encoded))
	defer multiIt.Close()

	for multiIt.Next() {
		dp, _, annotation := multiIt.Current()

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

	if err := multiIt.Err(); err != nil {
		s.metrics.fetch.ReportError(s.nowFn().Sub(callStart))
		return nil, tterrors.NewInternalError(err)
	}

	s.metrics.fetch.ReportSuccess(s.nowFn().Sub(callStart))

	return result, nil
}

func (s *service) FetchTagged(tctx thrift.Context, req *rpc.FetchTaggedRequest) (*rpc.FetchTaggedResult_, error) {
	callStart := s.nowFn()
	ctx := tchannelthrift.Context(tctx)
	ns, query, opts, fetchData, err := convert.FromRPCFetchTaggedRequest(req, s.pools)
	if err != nil {
		s.metrics.fetchTagged.ReportError(s.nowFn().Sub(callStart))
		return nil, tterrors.NewBadRequestError(err)
	}

	queryResult, err := s.db.QueryIDs(ctx, ns, query, opts)
	if err != nil {
		s.metrics.fetchTagged.ReportError(s.nowFn().Sub(callStart))
		return nil, tterrors.NewInternalError(err)
	}

	response := &rpc.FetchTaggedResult_{
		Exhaustive: queryResult.Exhaustive,
	}
	results := queryResult.Results
	nsID := results.Namespace()
	tagsIter := ident.NewTagsIterator(ident.Tags{})
	for _, entry := range results.Map().Iter() {
		tsID := entry.Key()
		tags := entry.Value()
		enc := s.pools.tagEncoder.Get()
		ctx.RegisterFinalizer(enc)
		tagsIter.Reset(tags)
		encodedTags, err := s.encodeTags(enc, tagsIter)
		if err != nil { // This is an invariant, should never happen
			s.metrics.fetchTagged.ReportError(s.nowFn().Sub(callStart))
			return nil, tterrors.NewInternalError(err)
		}

		elem := &rpc.FetchTaggedIDResult_{
			NameSpace:   nsID.Bytes(),
			ID:          tsID.Bytes(),
			EncodedTags: encodedTags.Bytes(),
		}
		response.Elements = append(response.Elements, elem)
		if !fetchData {
			continue
		}
		segments, rpcErr := s.readEncoded(ctx, nsID, tsID, opts.StartInclusive, opts.EndExclusive)
		if rpcErr != nil {
			elem.Err = rpcErr
			continue
		}
		elem.Segments = segments
	}

	s.metrics.fetchTagged.ReportSuccess(s.nowFn().Sub(callStart))
	return response, nil
}

func (s *service) encodeTags(
	enc serialize.TagEncoder,
	tags ident.TagIterator,
) (checked.Bytes, error) {
	if err := enc.Encode(tags); err != nil {
		// should never happen
		err = xerrors.NewRenamedError(err, fmt.Errorf("unable to encode tags"))
		l := instrument.EmitInvariantViolationAndGetLogger(s.opts.InstrumentOptions())
		l.Error(err.Error())
		return nil, err
	}
	encodedTags, ok := enc.Data()
	if !ok {
		// should never happen
		err := fmt.Errorf("unable to encode tags: unable to unwrap bytes")
		l := instrument.EmitInvariantViolationAndGetLogger(s.opts.InstrumentOptions())
		l.Error(err.Error())
		return nil, err
	}
	return encodedTags, nil
}

func (s *service) FetchBatchRaw(tctx thrift.Context, req *rpc.FetchBatchRawRequest) (*rpc.FetchBatchRawResult_, error) {
	callStart := s.nowFn()
	ctx := tchannelthrift.Context(tctx)

	start, rangeStartErr := convert.ToTime(req.RangeStart, req.RangeTimeType)
	end, rangeEndErr := convert.ToTime(req.RangeEnd, req.RangeTimeType)

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
		segments, rpcErr := s.readEncoded(ctx, nsID, tsID, start, end)
		if rpcErr != nil {
			rawResult.Err = rpcErr
			if tterrors.IsBadRequestError(rawResult.Err) {
				nonRetryableErrors++
			} else {
				retryableErrors++
			}
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
	if s.isOverloaded() {
		s.metrics.overloadRejected.Inc(1)
		return nil, tterrors.NewInternalError(errServerIsOverloaded)
	}

	callStart := s.nowFn()
	ctx := tchannelthrift.Context(tctx)

	nsID := s.newID(ctx, req.NameSpace)
	// check if the namespace if known
	nsMetadata, ok := s.db.Namespace(nsID)
	if !ok {
		return nil, tterrors.NewBadRequestError(fmt.Errorf("unable to find specified namespace: %v", nsID.String()))
	}

	res := rpc.NewFetchBlocksRawResult_()
	res.Elements = make([]*rpc.Blocks, len(req.Elements))

	// Preallocate starts to maximum size since at least one element will likely
	// be fetching most blocks for peer bootstrapping
	ropts := nsMetadata.Options().RetentionOptions()
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
			block.Start = fetchedBlock.Start.UnixNano()
			if err := fetchedBlock.Err; err != nil {
				block.Err = convert.ToRPCError(err)
			} else {
				var converted convert.ToSegmentsResult
				converted, err = convert.ToSegments(fetchedBlock.Blocks)
				if err != nil {
					block.Err = convert.ToRPCError(err)
				}
				if converted.Segments == nil {
					// No data for block, skip this block
					continue
				}
				block.Segments = converted.Segments
				block.Checksum = converted.Checksum
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

	var opts block.FetchBlocksMetadataOptions
	if req.IncludeSizes != nil {
		opts.IncludeSizes = *req.IncludeSizes
	}
	if req.IncludeChecksums != nil {
		opts.IncludeChecksums = *req.IncludeChecksums
	}
	if req.IncludeLastRead != nil {
		opts.IncludeLastRead = *req.IncludeLastRead
	}

	nsID := s.newID(ctx, req.NameSpace)

	fetched, nextPageToken, err := s.db.FetchBlocksMetadata(ctx, nsID,
		uint32(req.Shard), start, end, req.Limit, pageToken, opts)
	if err != nil {
		s.metrics.fetchBlocksMetadata.ReportError(s.nowFn().Sub(callStart))
		return nil, convert.ToRPCError(err)
	}
	ctx.RegisterCloser(fetched)

	fetchedResults := fetched.Results()
	result := rpc.NewFetchBlocksMetadataRawResult_()
	result.NextPageToken = nextPageToken
	result.Elements = s.pools.blocksMetadataSlice.Get()

	// NB(xichen): register a closer with context so objects are returned to pool
	// when we are done using them
	ctx.RegisterFinalizer(s.newCloseableMetadataResult(result))

	for _, fetchedMetadata := range fetchedResults {
		blocksMetadata := s.pools.blocksMetadata.Get()
		blocksMetadata.ID = fetchedMetadata.ID.Data().Bytes()
		fetchedMetadataBlocks := fetchedMetadata.Blocks.Results()
		blocksMetadata.Blocks = s.pools.blockMetadataSlice.Get()

		for _, fetchedMetadataBlock := range fetchedMetadataBlocks {
			blockMetadata := s.pools.blockMetadata.Get()

			blockMetadata.Start = fetchedMetadataBlock.Start.UnixNano()

			if opts.IncludeSizes {
				size := fetchedMetadataBlock.Size
				blockMetadata.Size = &size
			} else {
				blockMetadata.Size = nil
			}

			checksum := fetchedMetadataBlock.Checksum
			if opts.IncludeChecksums && checksum != nil {
				value := int64(*checksum)
				blockMetadata.Checksum = &value
			} else {
				blockMetadata.Checksum = nil
			}

			if opts.IncludeLastRead {
				lastRead := fetchedMetadataBlock.LastRead.UnixNano()
				blockMetadata.LastRead = &lastRead
				blockMetadata.LastReadTimeType = rpc.TimeType_UNIX_NANOSECONDS
			} else {
				blockMetadata.LastRead = nil
				blockMetadata.LastReadTimeType = rpc.TimeType(0)
			}

			if err := fetchedMetadataBlock.Err; err != nil {
				blockMetadata.Err = convert.ToRPCError(err)
			} else {
				blockMetadata.Err = nil
			}

			blocksMetadata.Blocks = append(blocksMetadata.Blocks, blockMetadata)
		}

		result.Elements = append(result.Elements, blocksMetadata)
	}

	s.metrics.fetchBlocksMetadata.ReportSuccess(s.nowFn().Sub(callStart))

	return result, nil
}

func (s *service) FetchBlocksMetadataRawV2(tctx thrift.Context, req *rpc.FetchBlocksMetadataRawV2Request) (*rpc.FetchBlocksMetadataRawV2Result_, error) {
	if s.db.IsOverloaded() {
		s.metrics.overloadRejected.Inc(1)
		return nil, tterrors.NewInternalError(errServerIsOverloaded)
	}

	var err error
	callStart := s.nowFn()
	defer func() {
		// No need to report metric anywhere else as we capture all cases here
		s.metrics.fetchBlocksMetadata.ReportSuccessOrError(err, s.nowFn().Sub(callStart))
	}()

	ctx := tchannelthrift.Context(tctx)
	if req.Limit <= 0 {
		return nil, nil
	}

	var opts block.FetchBlocksMetadataOptions
	if req.IncludeSizes != nil {
		opts.IncludeSizes = *req.IncludeSizes
	}
	if req.IncludeChecksums != nil {
		opts.IncludeChecksums = *req.IncludeChecksums
	}
	if req.IncludeLastRead != nil {
		opts.IncludeLastRead = *req.IncludeLastRead
	}

	var (
		nsID  = s.newID(ctx, req.NameSpace)
		start = time.Unix(0, req.RangeStart)
		end   = time.Unix(0, req.RangeEnd)
	)
	fetchedMetadata, nextPageToken, err := s.db.FetchBlocksMetadataV2(
		ctx, nsID, uint32(req.Shard), start, end, req.Limit, req.PageToken, opts)
	if err != nil {
		return nil, convert.ToRPCError(err)
	}

	ctx.RegisterCloser(fetchedMetadata)

	result, err := s.getFetchBlocksMetadataRawV2Result(ctx, nextPageToken, opts, fetchedMetadata)
	if err != nil {
		return nil, convert.ToRPCError(err)
	}

	ctx.RegisterFinalizer(s.newCloseableMetadataV2Result(result))
	return result, nil
}

func (s *service) getFetchBlocksMetadataRawV2Result(
	ctx context.Context,
	nextPageToken storage.PageToken,
	opts block.FetchBlocksMetadataOptions,
	results block.FetchBlocksMetadataResults,
) (*rpc.FetchBlocksMetadataRawV2Result_, error) {
	elements, err := s.getBlocksMetadataV2FromResult(ctx, opts, results)
	if err != nil {
		return nil, err
	}

	result := rpc.NewFetchBlocksMetadataRawV2Result_()
	result.NextPageToken = nextPageToken
	result.Elements = elements
	return result, nil
}

func (s *service) getBlocksMetadataV2FromResult(
	ctx context.Context,
	opts block.FetchBlocksMetadataOptions,
	results block.FetchBlocksMetadataResults,
) ([]*rpc.BlockMetadataV2, error) {
	blocks := s.pools.blockMetadataV2Slice.Get()
	for _, fetchedMetadata := range results.Results() {
		fetchedMetadataBlocks := fetchedMetadata.Blocks.Results()

		var (
			id          = fetchedMetadata.ID.Bytes()
			tags        = fetchedMetadata.Tags
			encodedTags []byte
		)
		if tags != nil && tags.Remaining() > 0 {
			enc := s.pools.tagEncoder.Get()
			ctx.RegisterFinalizer(enc)
			encoded, err := s.encodeTags(enc, tags)
			if err != nil {
				return nil, err
			}
			encodedTags = encoded.Bytes()
		}

		for _, fetchedMetadataBlock := range fetchedMetadataBlocks {
			blockMetadata := s.pools.blockMetadataV2.Get()
			blockMetadata.ID = id
			blockMetadata.EncodedTags = encodedTags
			blockMetadata.Start = fetchedMetadataBlock.Start.UnixNano()

			if opts.IncludeSizes {
				size := fetchedMetadataBlock.Size
				blockMetadata.Size = &size
			} else {
				blockMetadata.Size = nil
			}

			checksum := fetchedMetadataBlock.Checksum
			if opts.IncludeChecksums && checksum != nil {
				value := int64(*checksum)
				blockMetadata.Checksum = &value
			} else {
				blockMetadata.Checksum = nil
			}

			if opts.IncludeLastRead {
				lastRead := fetchedMetadataBlock.LastRead.UnixNano()
				blockMetadata.LastRead = &lastRead
				blockMetadata.LastReadTimeType = rpc.TimeType_UNIX_NANOSECONDS
			} else {
				blockMetadata.LastRead = nil
				blockMetadata.LastReadTimeType = rpc.TimeType(0)
			}

			if err := fetchedMetadataBlock.Err; err != nil {
				blockMetadata.Err = convert.ToRPCError(err)
			} else {
				blockMetadata.Err = nil
			}

			blocks = append(blocks, blockMetadata)
		}
	}

	return blocks, nil
}

func (s *service) Write(tctx thrift.Context, req *rpc.WriteRequest) error {
	callStart := s.nowFn()
	ctx := tchannelthrift.Context(tctx)

	if req.Datapoint == nil {
		s.metrics.write.ReportError(s.nowFn().Sub(callStart))
		return tterrors.NewBadRequestError(errRequiresDatapoint)
	}

	dp := req.Datapoint
	unit, unitErr := convert.ToUnit(dp.TimestampTimeType)

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
		ctx, s.pools.id.GetStringID(ctx, req.NameSpace), s.pools.id.GetStringID(ctx, req.ID),
		xtime.FromNormalizedTime(dp.Timestamp, d), dp.Value, unit, dp.Annotation,
	); err != nil {
		s.metrics.write.ReportError(s.nowFn().Sub(callStart))
		return convert.ToRPCError(err)
	}

	s.metrics.write.ReportSuccess(s.nowFn().Sub(callStart))

	return nil
}

func (s *service) WriteTagged(tctx thrift.Context, req *rpc.WriteTaggedRequest) error {
	callStart := s.nowFn()
	ctx := tchannelthrift.Context(tctx)

	if req.Datapoint == nil {
		s.metrics.writeTagged.ReportError(s.nowFn().Sub(callStart))
		return tterrors.NewBadRequestError(errRequiresDatapoint)
	}

	if req.Tags == nil {
		s.metrics.writeTagged.ReportError(s.nowFn().Sub(callStart))
		return tterrors.NewBadRequestError(errIllegalTagValues)
	}

	dp := req.Datapoint
	unit, unitErr := convert.ToUnit(dp.TimestampTimeType)

	if unitErr != nil {
		s.metrics.writeTagged.ReportError(s.nowFn().Sub(callStart))
		return tterrors.NewBadRequestError(unitErr)
	}

	d, err := unit.Value()
	if err != nil {
		s.metrics.writeTagged.ReportError(s.nowFn().Sub(callStart))
		return tterrors.NewBadRequestError(err)
	}

	iter, err := convert.ToTagsIter(req)
	if err != nil {
		s.metrics.writeTagged.ReportError(s.nowFn().Sub(callStart))
		return tterrors.NewBadRequestError(err)
	}

	if err = s.db.WriteTagged(ctx,
		s.pools.id.GetStringID(ctx, req.NameSpace),
		s.pools.id.GetStringID(ctx, req.ID),
		iter, xtime.FromNormalizedTime(dp.Timestamp, d),
		dp.Value, unit, dp.Annotation); err != nil {
		s.metrics.writeTagged.ReportError(s.nowFn().Sub(callStart))
		return convert.ToRPCError(err)
	}

	s.metrics.writeTagged.ReportSuccess(s.nowFn().Sub(callStart))

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
		unit, unitErr := convert.ToUnit(elem.Datapoint.TimestampTimeType)
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

func (s *service) WriteTaggedBatchRaw(tctx thrift.Context, req *rpc.WriteTaggedBatchRawRequest) error {
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
		unit, unitErr := convert.ToUnit(elem.Datapoint.TimestampTimeType)
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

		dec, err := s.newTagsDecoder(ctx, elem.EncodedTags)
		if err != nil {
			nonRetryableErrors++
			errs = append(errs, tterrors.NewBadRequestWriteBatchRawError(i, err))
			continue
		}

		if err = s.db.WriteTagged(
			ctx, nsID, s.newID(ctx, elem.ID), dec,
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

	s.metrics.writeTaggedBatchRaw.ReportSuccess(success)
	s.metrics.writeTaggedBatchRaw.ReportRetryableErrors(retryableErrors)
	s.metrics.writeTaggedBatchRaw.ReportNonRetryableErrors(nonRetryableErrors)
	s.metrics.writeTaggedBatchRaw.ReportLatency(s.nowFn().Sub(callStart))

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
	runtimeOptsMgr := s.db.Options().RuntimeOptionsManager()
	opts := runtimeOptsMgr.Get().PersistRateLimitOptions()
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
	runtimeOptsMgr := s.db.Options().RuntimeOptionsManager()
	runopts := runtimeOptsMgr.Get()
	opts := runopts.PersistRateLimitOptions()
	if req.LimitEnabled != nil {
		opts = opts.SetLimitEnabled(*req.LimitEnabled)
	}
	if req.LimitMbps != nil {
		opts = opts.SetLimitMbps(*req.LimitMbps)
	}
	if req.LimitCheckEvery != nil {
		opts = opts.SetLimitCheckEvery(int(*req.LimitCheckEvery))
	}

	runtimeOptsMgr.Update(runopts.SetPersistRateLimitOptions(opts))

	return s.GetPersistRateLimit(ctx)
}

func (s *service) GetWriteNewSeriesAsync(
	ctx thrift.Context,
) (*rpc.NodeWriteNewSeriesAsyncResult_, error) {
	runtimeOptsMgr := s.db.Options().RuntimeOptionsManager()
	value := runtimeOptsMgr.Get().WriteNewSeriesAsync()
	return &rpc.NodeWriteNewSeriesAsyncResult_{
		WriteNewSeriesAsync: value,
	}, nil
}

func (s *service) SetWriteNewSeriesAsync(
	ctx thrift.Context,
	req *rpc.NodeSetWriteNewSeriesAsyncRequest,
) (*rpc.NodeWriteNewSeriesAsyncResult_, error) {
	runtimeOptsMgr := s.db.Options().RuntimeOptionsManager()
	set := runtimeOptsMgr.Get().SetWriteNewSeriesAsync(req.WriteNewSeriesAsync)
	if err := runtimeOptsMgr.Update(set); err != nil {
		return nil, tterrors.NewBadRequestError(err)
	}
	return s.GetWriteNewSeriesAsync(ctx)
}

func (s *service) GetWriteNewSeriesBackoffDuration(
	ctx thrift.Context,
) (
	*rpc.NodeWriteNewSeriesBackoffDurationResult_,
	error,
) {
	runtimeOptsMgr := s.db.Options().RuntimeOptionsManager()
	value := runtimeOptsMgr.Get().WriteNewSeriesBackoffDuration()
	return &rpc.NodeWriteNewSeriesBackoffDurationResult_{
		WriteNewSeriesBackoffDuration: int64(value / time.Millisecond),
		DurationType:                  rpc.TimeType_UNIX_MILLISECONDS,
	}, nil
}

func (s *service) SetWriteNewSeriesBackoffDuration(
	ctx thrift.Context,
	req *rpc.NodeSetWriteNewSeriesBackoffDurationRequest,
) (
	*rpc.NodeWriteNewSeriesBackoffDurationResult_,
	error,
) {
	unit, err := convert.ToDuration(req.DurationType)
	if err != nil {
		return nil, tterrors.NewBadRequestError(xerrors.NewInvalidParamsError(err))
	}
	runtimeOptsMgr := s.db.Options().RuntimeOptionsManager()
	value := time.Duration(req.WriteNewSeriesBackoffDuration) * unit
	set := runtimeOptsMgr.Get().SetWriteNewSeriesBackoffDuration(value)
	if err := runtimeOptsMgr.Update(set); err != nil {
		return nil, tterrors.NewBadRequestError(err)
	}
	return s.GetWriteNewSeriesBackoffDuration(ctx)
}

func (s *service) GetWriteNewSeriesLimitPerShardPerSecond(
	ctx thrift.Context,
) (
	*rpc.NodeWriteNewSeriesLimitPerShardPerSecondResult_,
	error,
) {
	runtimeOptsMgr := s.db.Options().RuntimeOptionsManager()
	value := runtimeOptsMgr.Get().WriteNewSeriesLimitPerShardPerSecond()
	return &rpc.NodeWriteNewSeriesLimitPerShardPerSecondResult_{
		WriteNewSeriesLimitPerShardPerSecond: int64(value),
	}, nil
}

func (s *service) SetWriteNewSeriesLimitPerShardPerSecond(
	ctx thrift.Context,
	req *rpc.NodeSetWriteNewSeriesLimitPerShardPerSecondRequest,
) (
	*rpc.NodeWriteNewSeriesLimitPerShardPerSecondResult_,
	error,
) {
	runtimeOptsMgr := s.db.Options().RuntimeOptionsManager()
	value := int(req.WriteNewSeriesLimitPerShardPerSecond)
	set := runtimeOptsMgr.Get().SetWriteNewSeriesLimitPerShardPerSecond(value)
	if err := runtimeOptsMgr.Update(set); err != nil {
		return nil, tterrors.NewBadRequestError(err)
	}
	return s.GetWriteNewSeriesLimitPerShardPerSecond(ctx)
}

func (s *service) isOverloaded() bool {
	// NB(xichen): for now we only use the database load to determine
	// whether the server is overloaded. In the future we may also take
	// into account other metrics such as CPU load, disk I/O rate, etc.
	return s.db.IsOverloaded()
}

func (s *service) newID(ctx context.Context, id []byte) ident.ID {
	checkedBytes := s.pools.checkedBytesWrapper.Get(id)
	return s.pools.id.GetBinaryID(ctx, checkedBytes)
}

func (s *service) readEncoded(
	ctx context.Context,
	nsID, tsID ident.ID,
	start, end time.Time,
) ([]*rpc.Segments, *rpc.Error) {
	encoded, err := s.db.ReadEncoded(ctx, nsID, tsID, start, end)
	if err != nil {
		return nil, convert.ToRPCError(err)
	}

	segments := s.pools.segmentsArray.Get()
	segments = segmentsArr(segments).grow(len(encoded))
	segments = segments[:0]
	ctx.RegisterFinalizer(resource.FinalizerFn(func() {
		s.pools.segmentsArray.Put(segments)
	}))

	for _, readers := range encoded {
		converted, err := convert.ToSegments(readers)
		if err != nil {
			return nil, convert.ToRPCError(err)
		}
		if converted.Segments == nil {
			continue
		}
		segments = append(segments, converted.Segments)
	}

	return segments, nil
}

func (s *service) newTagsDecoder(ctx context.Context, encodedTags []byte) (serialize.TagDecoder, error) {
	checkedBytes := s.pools.checkedBytesWrapper.Get(encodedTags)
	dec := s.pools.tagDecoder.Get()
	ctx.RegisterCloser(dec)
	dec.Reset(checkedBytes)
	if err := dec.Err(); err != nil {
		return nil, err
	}
	return dec, nil
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
			c.s.pools.blockMetadata.Put(blockMetadata)
		}
		c.s.pools.blockMetadataSlice.Put(blocksMetadata.Blocks)
		c.s.pools.blocksMetadata.Put(blocksMetadata)
	}
	c.s.pools.blocksMetadataSlice.Put(c.result.Elements)
}

func (s *service) newCloseableMetadataV2Result(
	res *rpc.FetchBlocksMetadataRawV2Result_,
) closeableMetadataV2Result {
	return closeableMetadataV2Result{s: s, result: res}
}

type closeableMetadataV2Result struct {
	s      *service
	result *rpc.FetchBlocksMetadataRawV2Result_
}

func (c closeableMetadataV2Result) Finalize() {
	for _, blockMetadata := range c.result.Elements {
		c.s.pools.blockMetadataV2.Put(blockMetadata)
	}
	c.s.pools.blockMetadataV2Slice.Put(c.result.Elements)
}
