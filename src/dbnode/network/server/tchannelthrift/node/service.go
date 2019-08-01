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

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/dbnode/network/server/tchannelthrift"
	"github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/convert"
	tterrors "github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/errors"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/tracepoint"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/dbnode/x/xpool"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/context"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xopentracing "github.com/m3db/m3/src/x/opentracing"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/resource"
	"github.com/m3db/m3/src/x/serialize"
	xtime "github.com/m3db/m3/src/x/time"

	apachethrift "github.com/apache/thrift/lib/go/thrift"
	"github.com/m3db/m3/src/dbnode/namespace"
	opentracinglog "github.com/opentracing/opentracing-go/log"
	"github.com/uber-go/tally"
	"github.com/uber/tchannel-go/thrift"
	"go.uber.org/zap"
)

var (
	// NB(r): pool sizes are vars to help reduce stress on tests.
	checkedBytesPoolSize        = 65536
	segmentArrayPoolSize        = 65536
	writeBatchPooledReqPoolSize = 1024
)

const (
	initSegmentArrayPoolLength  = 4
	maxSegmentArrayPooledLength = 32
	// Any pooled error slices that grow beyond this capcity will be thrown away.
	writeBatchPooledReqPoolMaxErrorsSliceSize = 4096
)

var (
	// errServerIsOverloaded raised when trying to process a request when the server is overloaded
	errServerIsOverloaded = errors.New("server is overloaded")

	// errIllegalTagValues raised when the tags specified are in-correct
	errIllegalTagValues = errors.New("illegal tag values specified")

	// errRequiresDatapoint raised when a datapoint is not provided
	errRequiresDatapoint = errors.New("requires datapoint")

	// errNodeIsNotBootstrapped
	errNodeIsNotBootstrapped = errors.New("node is not bootstrapped")

	// errDatabaseIsNotInitializedYet is raised when an RPC attempt is made before the database
	// has been set.
	errDatabaseIsNotInitializedYet = errors.New("database is not yet initialized")

	// errDatabaseHasAlreadyBeenSet is raised when SetDatabase() is called more than one time.
	errDatabaseHasAlreadyBeenSet = errors.New("database has already been set")

	// errNotImplemented raised when attempting to execute an un-implemented method
	errNotImplemented = errors.New("method is not implemented")

	// errHealthNotSet is raised when server health data structure is not set.
	errHealthNotSet = errors.New("server health not set")
)

type serviceMetrics struct {
	fetch               instrument.MethodMetrics
	fetchTagged         instrument.MethodMetrics
	aggregate           instrument.MethodMetrics
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
		aggregate:           instrument.NewMethodMetrics(scope, "aggregate", samplingRate),
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
	state serviceState

	logger *zap.Logger

	opts    tchannelthrift.Options
	nowFn   clock.NowFn
	pools   pools
	metrics serviceMetrics
}

type serviceState struct {
	sync.RWMutex
	db     storage.Database
	health *rpc.NodeHealthResult_

	numOutstandingWriteRPCs int
	maxOutstandingWriteRPCs int

	numOutstandingReadRPCs int
	maxOutstandingReadRPCs int
}

func (s *serviceState) DB() (storage.Database, bool) {
	s.RLock()
	v := s.db
	s.RUnlock()
	return v, v != nil
}

func (s *serviceState) Health() (*rpc.NodeHealthResult_, bool) {
	s.RLock()
	v := s.health
	s.RUnlock()
	return v, v != nil
}

func (s *serviceState) DBForWriteRPCWithLimit() (
	db storage.Database, dbInitialized bool, rpcDoesNotExceedLimit bool) {
	s.Lock()
	defer s.Unlock()

	if s.db == nil {
		return nil, false, false
	}
	if s.numOutstandingWriteRPCs >= s.maxOutstandingWriteRPCs {
		return nil, true, false
	}

	v := s.db
	s.numOutstandingWriteRPCs++
	return v, true, true
}

func (s *serviceState) DecNumOutstandingWriteRPCs() {
	s.Lock()
	s.numOutstandingWriteRPCs--
	s.Unlock()
}

func (s *serviceState) DBForReadRPCWithLimit() (
	db storage.Database, dbInitialized bool, requestDoesNotExceedLimit bool) {
	s.Lock()
	defer s.Unlock()

	if s.db == nil {
		return nil, false, false
	}
	if s.numOutstandingReadRPCs >= s.maxOutstandingReadRPCs {
		return nil, true, false
	}

	v := s.db
	s.numOutstandingReadRPCs++
	return v, true, true
}

func (s *serviceState) DecNumOutstandingReadRPCs() {
	s.Lock()
	s.numOutstandingReadRPCs--
	s.Unlock()
}

type pools struct {
	id                      ident.Pool
	tagEncoder              serialize.TagEncoderPool
	tagDecoder              serialize.TagDecoderPool
	checkedBytesWrapper     xpool.CheckedBytesWrapperPool
	segmentsArray           segmentsArrayPool
	writeBatchPooledReqPool *writeBatchPooledReqPool
	blockMetadataV2         tchannelthrift.BlockMetadataV2Pool
	blockMetadataV2Slice    tchannelthrift.BlockMetadataV2SlicePool
}

// ensure `pools` matches a required conversion interface
var _ convert.FetchTaggedConversionPools = pools{}

func (p pools) ID() ident.Pool                                     { return p.id }
func (p pools) CheckedBytesWrapper() xpool.CheckedBytesWrapperPool { return p.checkedBytesWrapper }

// Service is the interface for the node RPC service.
type Service interface {
	rpc.TChanNode

	// Only safe to be called one time once the service has started.
	SetDatabase(db storage.Database) error
}

// NewService creates a new node TChannel Thrift service
func NewService(db storage.Database, opts tchannelthrift.Options) Service {
	if opts == nil {
		opts = tchannelthrift.NewOptions()
	}

	iopts := opts.InstrumentOptions()

	scope := iopts.
		MetricsScope().
		SubScope("service").
		Tagged(map[string]string{"service-name": "node"})

	// Use the new scope in options
	iopts = iopts.SetMetricsScope(scope)
	opts = opts.SetInstrumentOptions(iopts)

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

	writeBatchPoolSize := writeBatchPooledReqPoolSize
	if maxWriteReqs := opts.MaxOutstandingWriteRequests(); maxWriteReqs > 0 {
		// If a limit on the number of maximum outstanding write
		// requests has been set then we know the exact number of
		// of writeBatchPooledReq objects we need to never have to
		// allocate one on demand.
		writeBatchPoolSize = maxWriteReqs
	}
	writeBatchPooledReqPool := newWriteBatchPooledReqPool(writeBatchPoolSize, iopts)
	writeBatchPooledReqPool.Init(opts.TagDecoderPool())

	return &service{
		state: serviceState{
			db: db,
			health: &rpc.NodeHealthResult_{
				Ok:           true,
				Status:       "up",
				Bootstrapped: false,
			},
			maxOutstandingWriteRPCs: opts.MaxOutstandingWriteRequests(),
			maxOutstandingReadRPCs:  opts.MaxOutstandingReadRequests(),
		},
		logger:  iopts.Logger(),
		opts:    opts,
		nowFn:   opts.ClockOptions().NowFn(),
		metrics: newServiceMetrics(scope, iopts.MetricsSamplingRate()),
		pools: pools{
			id:                      opts.IdentifierPool(),
			checkedBytesWrapper:     wrapperPool,
			tagEncoder:              opts.TagEncoderPool(),
			tagDecoder:              opts.TagDecoderPool(),
			segmentsArray:           segmentPool,
			writeBatchPooledReqPool: writeBatchPooledReqPool,
			blockMetadataV2:         opts.BlockMetadataV2Pool(),
			blockMetadataV2Slice:    opts.BlockMetadataV2SlicePool(),
		},
	}
}

func (s *service) Health(ctx thrift.Context) (*rpc.NodeHealthResult_, error) {
	health, ok := s.state.Health()
	if !ok {
		// Health should always be set
		return nil, convert.ToRPCError(errHealthNotSet)
	}

	db, ok := s.state.DB()
	if !ok {
		// DB not yet set, just return existing health status
		return health, nil
	}

	// Update bootstrapped field if not up to date. Note that we use
	// IsBootstrappedAndDurable instead of IsBootstrapped to make sure
	// that in the scenario where a topology change has occurred, none of
	// our automated tooling will assume a node is healthy until it has
	// marked all its shards as available and is able to bootstrap all the
	// shards it owns from its own local disk.
	bootstrapped := db.IsBootstrappedAndDurable()
	if health.Bootstrapped != bootstrapped {
		newHealth := &rpc.NodeHealthResult_{}
		*newHealth = *health
		newHealth.Bootstrapped = bootstrapped

		s.state.Lock()
		s.state.health = newHealth
		s.state.Unlock()

		// Update response
		health = newHealth
	}

	return health, nil
}

// Bootstrapped is designed to be used with cluster management tools like k8s
// that expect an endpoint that will return success if the node is
// healthy/bootstrapped and an error if not. We added this endpoint because
// while the Health endpoint provides the same information, this endpoint does
// not require parsing the response to determine if the node is bootstrapped or
// not.
func (s *service) Bootstrapped(ctx thrift.Context) (*rpc.NodeBootstrappedResult_, error) {
	db, ok := s.state.DB()
	if !ok {
		return nil, convert.ToRPCError(errDatabaseIsNotInitializedYet)
	}

	// Note that we use IsBootstrappedAndDurable instead of IsBootstrapped to
	// make sure that in the scenario where a topology change has occurred, none
	// of our automated tooling will assume a node is healthy until it has
	// marked all its shards as available and is able to bootstrap all the
	// shards it owns from its own local disk.
	if bootstrapped := db.IsBootstrappedAndDurable(); !bootstrapped {
		return nil, convert.ToRPCError(errNodeIsNotBootstrapped)
	}

	return &rpc.NodeBootstrappedResult_{}, nil
}

// BootstrappedInPlacementOrNoPlacement is designed to be used with cluster
// management tools like k8s that expected an endpoint that will return
// success if the node either:
// 1) Has no cluster placement set yet.
// 2) Is bootstrapped and durable, meaning it is bootstrapped and is able
//    to bootstrap the shards it owns from it's own local disk.
// This is useful in addition to the Bootstrapped RPC method as it helps
// progress node addition/removal/modifications when no placement is set
// at all and therefore the node has not been able to bootstrap yet.
func (s *service) BootstrappedInPlacementOrNoPlacement(ctx thrift.Context) (*rpc.NodeBootstrappedInPlacementOrNoPlacementResult_, error) {
	hasPlacement, err := s.opts.TopologyInitializer().TopologyIsSet()
	if err != nil {
		return nil, convert.ToRPCError(err)
	}

	if !hasPlacement {
		// No placement at all.
		return &rpc.NodeBootstrappedInPlacementOrNoPlacementResult_{}, nil
	}

	db, ok := s.state.DB()
	if !ok {
		return nil, convert.ToRPCError(errDatabaseIsNotInitializedYet)
	}

	if bootstrapped := db.IsBootstrappedAndDurable(); !bootstrapped {
		return nil, convert.ToRPCError(errNodeIsNotBootstrapped)
	}

	return &rpc.NodeBootstrappedInPlacementOrNoPlacementResult_{}, nil
}

func (s *service) Query(tctx thrift.Context, req *rpc.QueryRequest) (*rpc.QueryResult_, error) {
	db, err := s.startReadRPCWithDB()
	if err != nil {
		return nil, err
	}
	defer s.readRPCCompleted()

	ctx, sp := tchannelthrift.Context(tctx).StartTraceSpan(tracepoint.Query)
	sp.LogFields(
		opentracinglog.String("query", req.Query.String()),
		opentracinglog.String("namespace", req.NameSpace),
		xopentracing.Time("start", time.Unix(0, req.RangeStart)),
		xopentracing.Time("end", time.Unix(0, req.RangeStart)),
	)

	result, err := s.query(ctx, db, req)
	if err != nil {
		sp.LogFields(opentracinglog.Error(err))
	}
	sp.Finish()

	return result, err
}

func (s *service) query(ctx context.Context, db storage.Database, req *rpc.QueryRequest) (*rpc.QueryResult_, error) {
	start, rangeStartErr := convert.ToTime(req.RangeStart, req.RangeType)
	end, rangeEndErr := convert.ToTime(req.RangeEnd, req.RangeType)
	if rangeStartErr != nil || rangeEndErr != nil {
		return nil, tterrors.NewBadRequestError(xerrors.FirstError(rangeStartErr, rangeEndErr))
	}

	q, err := convert.FromRPCQuery(req.Query)
	if err != nil {
		return nil, convert.ToRPCError(err)
	}

	nsID := s.pools.id.GetStringID(ctx, req.NameSpace)
	opts := index.QueryOptions{
		StartInclusive: start,
		EndExclusive:   end,
	}
	if l := req.Limit; l != nil {
		opts.Limit = int(*l)
	}
	queryResult, err := db.QueryIDs(ctx, nsID, index.Query{Query: q}, opts)
	if err != nil {
		return nil, convert.ToRPCError(err)
	}

	result := &rpc.QueryResult_{
		Results:    make([]*rpc.QueryResultElement, 0, queryResult.Results.Map().Len()),
		Exhaustive: queryResult.Exhaustive,
	}
	fetchData := true
	if req.NoData != nil && *req.NoData {
		fetchData = false
	}
	for _, entry := range queryResult.Results.Map().Iter() {
		elem := &rpc.QueryResultElement{
			ID:   entry.Key().String(),
			Tags: make([]*rpc.Tag, 0, len(entry.Value().Values())),
		}
		result.Results = append(result.Results, elem)
		for _, tag := range entry.Value().Values() {
			elem.Tags = append(elem.Tags, &rpc.Tag{
				Name:  tag.Name.String(),
				Value: tag.Value.String(),
			})
		}
		if !fetchData {
			continue
		}
		tsID := entry.Key()
		datapoints, err := s.readDatapoints(ctx, db, nsID, tsID, start, end,
			req.ResultTimeType)
		if err != nil {
			return nil, convert.ToRPCError(err)
		}
		elem.Datapoints = datapoints
	}

	return result, nil
}

func (s *service) Fetch(tctx thrift.Context, req *rpc.FetchRequest) (*rpc.FetchResult_, error) {
	db, err := s.startReadRPCWithDB()
	if err != nil {
		return nil, err
	}
	defer s.readRPCCompleted()

	var (
		callStart = s.nowFn()
		ctx       = tchannelthrift.Context(tctx)

		start, rangeStartErr = convert.ToTime(req.RangeStart, req.RangeType)
		end, rangeEndErr     = convert.ToTime(req.RangeEnd, req.RangeType)
	)
	if rangeStartErr != nil || rangeEndErr != nil {
		s.metrics.fetch.ReportError(s.nowFn().Sub(callStart))
		return nil, tterrors.NewBadRequestError(xerrors.FirstError(rangeStartErr, rangeEndErr))
	}

	tsID := s.pools.id.GetStringID(ctx, req.ID)
	nsID := s.pools.id.GetStringID(ctx, req.NameSpace)

	// Make datapoints an initialized empty array for JSON serialization as empty array than null
	datapoints, err := s.readDatapoints(ctx, db, nsID, tsID, start, end,
		req.ResultTimeType)
	if err != nil {
		s.metrics.fetch.ReportError(s.nowFn().Sub(callStart))
		return nil, convert.ToRPCError(err)
	}

	s.metrics.fetch.ReportSuccess(s.nowFn().Sub(callStart))
	return &rpc.FetchResult_{Datapoints: datapoints}, nil
}

func (s *service) readDatapoints(
	ctx context.Context,
	db storage.Database,
	nsID, tsID ident.ID,
	start, end time.Time,
	timeType rpc.TimeType,
) ([]*rpc.Datapoint, error) {
	encoded, err := db.ReadEncoded(ctx, nsID, tsID, start, end)
	if err != nil {
		return nil, err
	}

	// Resolve all futures and filter out any empty segments.
	filteredBlockReaderSliceOfSlices, err := xio.FilterEmptyBlockReadersInPlaceSliceOfSlices(encoded)
	if err != nil {
		return nil, err
	}

	// Make datapoints an initialized empty array for JSON serialization as empty array than null
	datapoints := make([]*rpc.Datapoint, 0)

	multiIt := db.Options().MultiReaderIteratorPool().Get()
	nsCtx := namespace.NewContextFor(nsID, db.Options().SchemaRegistry())
	multiIt.ResetSliceOfSlices(
		xio.NewReaderSliceOfSlicesFromBlockReadersIterator(
			filteredBlockReaderSliceOfSlices), nsCtx.Schema)
	defer multiIt.Close()

	for multiIt.Next() {
		dp, _, annotation := multiIt.Current()

		timestamp, timestampErr := convert.ToValue(dp.Timestamp, timeType)
		if timestampErr != nil {
			return nil, xerrors.NewInvalidParamsError(timestampErr)
		}

		datapoint := rpc.NewDatapoint()
		datapoint.Timestamp = timestamp
		datapoint.Value = dp.Value
		datapoint.Annotation = annotation

		datapoints = append(datapoints, datapoint)
	}

	if err := multiIt.Err(); err != nil {
		return nil, err
	}

	return datapoints, nil
}

func (s *service) FetchTagged(tctx thrift.Context, req *rpc.FetchTaggedRequest) (*rpc.FetchTaggedResult_, error) {
	db, err := s.startReadRPCWithDB()
	if err != nil {
		return nil, err
	}
	defer s.readRPCCompleted()

	ctx, sp := tchannelthrift.Context(tctx).StartTraceSpan(tracepoint.FetchTagged)
	sp.LogFields(
		opentracinglog.String("query", string(req.Query)),
		opentracinglog.String("namespace", string(req.NameSpace)),
		xopentracing.Time("start", time.Unix(0, req.RangeStart)),
		xopentracing.Time("end", time.Unix(0, req.RangeEnd)),
	)

	result, err := s.fetchTagged(ctx, db, req)
	if err != nil {
		sp.LogFields(opentracinglog.Error(err))
	}
	sp.Finish()

	return result, err
}

func (s *service) fetchTagged(ctx context.Context, db storage.Database, req *rpc.FetchTaggedRequest) (*rpc.FetchTaggedResult_, error) {
	callStart := s.nowFn()

	ns, query, opts, fetchData, err := convert.FromRPCFetchTaggedRequest(req, s.pools)
	if err != nil {
		s.metrics.fetchTagged.ReportError(s.nowFn().Sub(callStart))
		return nil, tterrors.NewBadRequestError(err)
	}

	queryResult, err := db.QueryIDs(ctx, ns, query, opts)
	if err != nil {
		s.metrics.fetchTagged.ReportError(s.nowFn().Sub(callStart))
		return nil, convert.ToRPCError(err)
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
		segments, rpcErr := s.readEncoded(ctx, db, nsID, tsID, opts.StartInclusive, opts.EndExclusive)
		if rpcErr != nil {
			elem.Err = rpcErr
			continue
		}
		elem.Segments = segments
	}

	s.metrics.fetchTagged.ReportSuccess(s.nowFn().Sub(callStart))
	return response, nil
}

func (s *service) Aggregate(tctx thrift.Context, req *rpc.AggregateQueryRequest) (*rpc.AggregateQueryResult_, error) {
	db, err := s.startReadRPCWithDB()
	if err != nil {
		return nil, err
	}
	defer s.readRPCCompleted()

	callStart := s.nowFn()
	ctx := tchannelthrift.Context(tctx)

	ns, query, opts, err := convert.FromRPCAggregateQueryRequest(req)
	if err != nil {
		s.metrics.aggregate.ReportError(s.nowFn().Sub(callStart))
		return nil, tterrors.NewBadRequestError(err)
	}

	queryResult, err := db.AggregateQuery(ctx, ns, query, opts)
	if err != nil {
		s.metrics.aggregate.ReportError(s.nowFn().Sub(callStart))
		return nil, convert.ToRPCError(err)
	}

	response := &rpc.AggregateQueryResult_{
		Exhaustive: queryResult.Exhaustive,
	}
	results := queryResult.Results
	for _, entry := range results.Map().Iter() {
		responseElem := &rpc.AggregateQueryResultTagNameElement{
			TagName: entry.Key().String(),
		}
		tagValues := entry.Value()
		tagValuesMap := tagValues.Map()
		responseElem.TagValues = make([]*rpc.AggregateQueryResultTagValueElement, 0, tagValuesMap.Len())
		for _, entry := range tagValuesMap.Iter() {
			responseElem.TagValues = append(responseElem.TagValues, &rpc.AggregateQueryResultTagValueElement{
				TagValue: entry.Key().String(),
			})
		}
		response.Results = append(response.Results, responseElem)
	}
	s.metrics.aggregate.ReportSuccess(s.nowFn().Sub(callStart))
	return response, nil
}

func (s *service) AggregateRaw(tctx thrift.Context, req *rpc.AggregateQueryRawRequest) (*rpc.AggregateQueryRawResult_, error) {
	db, err := s.startReadRPCWithDB()
	if err != nil {
		return nil, err
	}
	defer s.readRPCCompleted()

	callStart := s.nowFn()
	ctx := tchannelthrift.Context(tctx)

	ns, query, opts, err := convert.FromRPCAggregateQueryRawRequest(req, s.pools)
	if err != nil {
		s.metrics.aggregate.ReportError(s.nowFn().Sub(callStart))
		return nil, tterrors.NewBadRequestError(err)
	}

	queryResult, err := db.AggregateQuery(ctx, ns, query, opts)
	if err != nil {
		s.metrics.aggregate.ReportError(s.nowFn().Sub(callStart))
		return nil, convert.ToRPCError(err)
	}

	response := &rpc.AggregateQueryRawResult_{
		Exhaustive: queryResult.Exhaustive,
	}
	results := queryResult.Results
	for _, entry := range results.Map().Iter() {
		responseElem := &rpc.AggregateQueryRawResultTagNameElement{
			TagName: entry.Key().Bytes(),
		}
		tagValues := entry.Value()
		tagValuesMap := tagValues.Map()
		responseElem.TagValues = make([]*rpc.AggregateQueryRawResultTagValueElement, 0, tagValuesMap.Len())
		for _, entry := range tagValuesMap.Iter() {
			responseElem.TagValues = append(responseElem.TagValues, &rpc.AggregateQueryRawResultTagValueElement{
				TagValue: entry.Key().Bytes(),
			})
		}
		response.Results = append(response.Results, responseElem)
	}
	s.metrics.aggregate.ReportSuccess(s.nowFn().Sub(callStart))
	return response, nil
}

func (s *service) encodeTags(
	enc serialize.TagEncoder,
	tags ident.TagIterator,
) (checked.Bytes, error) {
	if err := enc.Encode(tags); err != nil {
		// should never happen
		err = xerrors.NewRenamedError(err, fmt.Errorf("unable to encode tags"))
		instrument.EmitAndLogInvariantViolation(s.opts.InstrumentOptions(), func(l *zap.Logger) {
			l.Error(err.Error())
		})
		return nil, err
	}
	encodedTags, ok := enc.Data()
	if !ok {
		// should never happen
		err := fmt.Errorf("unable to encode tags: unable to unwrap bytes")
		instrument.EmitAndLogInvariantViolation(s.opts.InstrumentOptions(), func(l *zap.Logger) {
			l.Error(err.Error())
		})
		return nil, err
	}
	return encodedTags, nil
}

func (s *service) FetchBatchRaw(tctx thrift.Context, req *rpc.FetchBatchRawRequest) (*rpc.FetchBatchRawResult_, error) {
	db, err := s.startReadRPCWithDB()
	if err != nil {
		return nil, err
	}
	defer s.readRPCCompleted()

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
		segments, rpcErr := s.readEncoded(ctx, db, nsID, tsID, start, end)
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
	db, err := s.startReadRPCWithDB()
	if err != nil {
		return nil, err
	}
	defer s.readRPCCompleted()

	var (
		callStart = s.nowFn()
		ctx       = tchannelthrift.Context(tctx)
		nsID      = s.newID(ctx, req.NameSpace)
		// check if the namespace if known
		nsMetadata, ok = db.Namespace(nsID)
	)
	if !ok {
		return nil, tterrors.NewBadRequestError(fmt.Errorf("unable to find specified namespace: %v", nsID.String()))
	}

	res := rpc.NewFetchBlocksRawResult_()
	res.Elements = make([]*rpc.Blocks, len(req.Elements))

	// Preallocate starts to maximum size since at least one element will likely
	// be fetching most blocks for peer bootstrapping
	ropts := nsMetadata.Options().RetentionOptions()
	blockStarts := make([]time.Time, 0,
		(ropts.RetentionPeriod()+ropts.FutureRetentionPeriod())/ropts.BlockSize())

	for i, request := range req.Elements {
		blockStarts = blockStarts[:0]

		for _, start := range request.Starts {
			blockStarts = append(blockStarts, xtime.FromNanoseconds(start))
		}

		tsID := s.newID(ctx, request.ID)
		fetched, err := db.FetchBlocks(
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

func (s *service) FetchBlocksMetadataRawV2(tctx thrift.Context, req *rpc.FetchBlocksMetadataRawV2Request) (*rpc.FetchBlocksMetadataRawV2Result_, error) {
	db, err := s.startReadRPCWithDB()
	if err != nil {
		return nil, err
	}
	defer s.readRPCCompleted()

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
	fetchedMetadata, nextPageToken, err := db.FetchBlocksMetadataV2(
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
	db, err := s.startWriteRPCWithDB()
	if err != nil {
		return err
	}
	defer s.writeRPCCompleted()

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

	if err = db.Write(
		ctx,
		s.pools.id.GetStringID(ctx, req.NameSpace),
		s.pools.id.GetStringID(ctx, req.ID),
		xtime.FromNormalizedTime(dp.Timestamp, d),
		dp.Value,
		unit,
		dp.Annotation,
	); err != nil {
		s.metrics.write.ReportError(s.nowFn().Sub(callStart))
		return convert.ToRPCError(err)
	}

	s.metrics.write.ReportSuccess(s.nowFn().Sub(callStart))

	return nil
}

func (s *service) WriteTagged(tctx thrift.Context, req *rpc.WriteTaggedRequest) error {
	db, err := s.startWriteRPCWithDB()
	if err != nil {
		return err
	}
	defer s.writeRPCCompleted()

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

	if err = db.WriteTagged(ctx,
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
	db, err := s.startWriteRPCWithDB()
	if err != nil {
		return err
	}
	defer s.writeRPCCompleted()

	callStart := s.nowFn()
	ctx := tchannelthrift.Context(tctx)

	// NB(r): Use the pooled request tracking to return thrift alloc'd bytes
	// to the thrift bytes pool and to return ident.ID wrappers to a pool for
	// reuse. We also reduce contention on pools by getting one per batch request
	// rather than one per ID.
	pooledReq := s.pools.writeBatchPooledReqPool.Get()
	pooledReq.writeReq = req
	ctx.RegisterFinalizer(pooledReq)

	var (
		nsID               = s.newPooledID(ctx, req.NameSpace, pooledReq)
		retryableErrors    int
		nonRetryableErrors int
	)

	batchWriter, err := db.BatchWriter(nsID, len(req.Elements))
	if err != nil {
		return convert.ToRPCError(err)
	}
	// The lifecycle of the annotations is more involved than the rest of the data
	// so we set the annotation pool put method as the finalization function and
	// let the database take care of returning them to the pool.
	batchWriter.SetFinalizeAnnotationFn(finalizeAnnotationFn)

	for i, elem := range req.Elements {
		unit, unitErr := convert.ToUnit(elem.Datapoint.TimestampTimeType)
		if unitErr != nil {
			nonRetryableErrors++
			pooledReq.addError(tterrors.NewBadRequestWriteBatchRawError(i, unitErr))
			continue
		}

		d, err := unit.Value()
		if err != nil {
			nonRetryableErrors++
			pooledReq.addError(tterrors.NewBadRequestWriteBatchRawError(i, err))
			continue
		}

		seriesID := s.newPooledID(ctx, elem.ID, pooledReq)
		batchWriter.Add(
			i,
			seriesID,
			xtime.FromNormalizedTime(elem.Datapoint.Timestamp, d),
			elem.Datapoint.Value,
			unit,
			elem.Datapoint.Annotation,
		)
	}

	err = db.WriteBatch(ctx, nsID, batchWriter.(ts.WriteBatch),
		pooledReq)
	if err != nil {
		return convert.ToRPCError(err)
	}

	nonRetryableErrors += pooledReq.numNonRetryableErrors()
	retryableErrors += pooledReq.numRetryableErrors()
	totalErrors := nonRetryableErrors + retryableErrors

	s.metrics.writeBatchRaw.ReportSuccess(len(req.Elements) - totalErrors)
	s.metrics.writeBatchRaw.ReportRetryableErrors(retryableErrors)
	s.metrics.writeBatchRaw.ReportNonRetryableErrors(nonRetryableErrors)
	s.metrics.writeBatchRaw.ReportLatency(s.nowFn().Sub(callStart))

	errs := pooledReq.writeBatchRawErrors()
	if len(errs) > 0 {
		batchErrs := rpc.NewWriteBatchRawErrors()
		batchErrs.Errors = errs
		return batchErrs
	}

	return nil
}

func (s *service) WriteTaggedBatchRaw(tctx thrift.Context, req *rpc.WriteTaggedBatchRawRequest) error {
	db, err := s.startWriteRPCWithDB()
	if err != nil {
		return err
	}
	defer s.writeRPCCompleted()

	callStart := s.nowFn()
	ctx := tchannelthrift.Context(tctx)

	// NB(r): Use the pooled request tracking to return thrift alloc'd bytes
	// to the thrift bytes pool and to return ident.ID wrappers to a pool for
	// reuse. We also reduce contention on pools by getting one per batch request
	// rather than one per ID.
	pooledReq := s.pools.writeBatchPooledReqPool.Get()
	pooledReq.writeTaggedReq = req
	ctx.RegisterFinalizer(pooledReq)

	var (
		nsID               = s.newPooledID(ctx, req.NameSpace, pooledReq)
		retryableErrors    int
		nonRetryableErrors int
	)

	batchWriter, err := db.BatchWriter(nsID, len(req.Elements))
	if err != nil {
		return convert.ToRPCError(err)
	}
	// The lifecycle of the annotations is more involved than the rest of the data
	// so we set the annotation pool put method as the finalization function and
	// let the database take care of returning them to the pool.
	batchWriter.SetFinalizeAnnotationFn(finalizeAnnotationFn)

	for i, elem := range req.Elements {
		unit, unitErr := convert.ToUnit(elem.Datapoint.TimestampTimeType)
		if unitErr != nil {
			nonRetryableErrors++
			pooledReq.addError(tterrors.NewBadRequestWriteBatchRawError(i, unitErr))
			continue
		}

		d, err := unit.Value()
		if err != nil {
			nonRetryableErrors++
			pooledReq.addError(tterrors.NewBadRequestWriteBatchRawError(i, err))
			continue
		}

		dec, err := s.newPooledTagsDecoder(ctx, elem.EncodedTags, pooledReq)
		if err != nil {
			nonRetryableErrors++
			pooledReq.addError(tterrors.NewBadRequestWriteBatchRawError(i, err))
			continue
		}

		seriesID := s.newPooledID(ctx, elem.ID, pooledReq)
		batchWriter.AddTagged(
			i,
			seriesID,
			dec,
			xtime.FromNormalizedTime(elem.Datapoint.Timestamp, d),
			elem.Datapoint.Value,
			unit,
			elem.Datapoint.Annotation)
	}

	err = db.WriteTaggedBatch(ctx, nsID, batchWriter, pooledReq)
	if err != nil {
		return convert.ToRPCError(err)
	}

	nonRetryableErrors += pooledReq.numNonRetryableErrors()
	retryableErrors += pooledReq.numRetryableErrors()
	totalErrors := nonRetryableErrors + retryableErrors

	s.metrics.writeTaggedBatchRaw.ReportSuccess(len(req.Elements) - totalErrors)
	s.metrics.writeTaggedBatchRaw.ReportRetryableErrors(retryableErrors)
	s.metrics.writeTaggedBatchRaw.ReportNonRetryableErrors(nonRetryableErrors)
	s.metrics.writeTaggedBatchRaw.ReportLatency(s.nowFn().Sub(callStart))

	errs := pooledReq.writeBatchRawErrors()
	if len(errs) > 0 {
		batchErrs := rpc.NewWriteBatchRawErrors()
		batchErrs.Errors = errs
		return batchErrs
	}

	return nil
}

func (s *service) Repair(tctx thrift.Context) error {
	db, err := s.startRPCWithDB()
	if err != nil {
		return err
	}

	callStart := s.nowFn()

	if err := db.Repair(); err != nil {
		s.metrics.repair.ReportError(s.nowFn().Sub(callStart))
		return convert.ToRPCError(err)
	}

	s.metrics.repair.ReportSuccess(s.nowFn().Sub(callStart))

	return nil
}

func (s *service) Truncate(tctx thrift.Context, req *rpc.TruncateRequest) (r *rpc.TruncateResult_, err error) {
	db, err := s.startRPCWithDB()
	if err != nil {
		return nil, err
	}

	callStart := s.nowFn()
	ctx := tchannelthrift.Context(tctx)
	truncated, err := db.Truncate(s.newID(ctx, req.NameSpace))
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
	db, err := s.startRPCWithDB()
	if err != nil {
		return nil, err
	}

	runtimeOptsMgr := db.Options().RuntimeOptionsManager()
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
	db, err := s.startRPCWithDB()
	if err != nil {
		return nil, err
	}

	runtimeOptsMgr := db.Options().RuntimeOptionsManager()
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
	db, err := s.startRPCWithDB()
	if err != nil {
		return nil, err
	}

	runtimeOptsMgr := db.Options().RuntimeOptionsManager()
	value := runtimeOptsMgr.Get().WriteNewSeriesAsync()
	return &rpc.NodeWriteNewSeriesAsyncResult_{
		WriteNewSeriesAsync: value,
	}, nil
}

func (s *service) SetWriteNewSeriesAsync(
	ctx thrift.Context,
	req *rpc.NodeSetWriteNewSeriesAsyncRequest,
) (*rpc.NodeWriteNewSeriesAsyncResult_, error) {
	db, err := s.startRPCWithDB()
	if err != nil {
		return nil, err
	}

	runtimeOptsMgr := db.Options().RuntimeOptionsManager()
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
	db, err := s.startRPCWithDB()
	if err != nil {
		return nil, err
	}

	runtimeOptsMgr := db.Options().RuntimeOptionsManager()
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
	db, err := s.startRPCWithDB()
	if err != nil {
		return nil, err
	}

	unit, err := convert.ToDuration(req.DurationType)
	if err != nil {
		return nil, tterrors.NewBadRequestError(xerrors.NewInvalidParamsError(err))
	}
	runtimeOptsMgr := db.Options().RuntimeOptionsManager()
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
	db, err := s.startRPCWithDB()
	if err != nil {
		return nil, err
	}

	runtimeOptsMgr := db.Options().RuntimeOptionsManager()
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
	db, err := s.startRPCWithDB()
	if err != nil {
		return nil, err
	}

	runtimeOptsMgr := db.Options().RuntimeOptionsManager()
	value := int(req.WriteNewSeriesLimitPerShardPerSecond)
	set := runtimeOptsMgr.Get().SetWriteNewSeriesLimitPerShardPerSecond(value)
	if err := runtimeOptsMgr.Update(set); err != nil {
		return nil, tterrors.NewBadRequestError(err)
	}
	return s.GetWriteNewSeriesLimitPerShardPerSecond(ctx)
}

func (s *service) SetDatabase(db storage.Database) error {
	s.state.Lock()
	defer s.state.Unlock()

	if s.state.db != nil {
		return errDatabaseHasAlreadyBeenSet
	}

	s.state.db = db
	return nil
}

func (s *service) startWriteRPCWithDB() (storage.Database, error) {
	if s.state.maxOutstandingWriteRPCs == 0 {
		// No limitations on number of outstanding requests.
		return s.startRPCWithDB()
	}

	db, dbIsInitialized, requestDoesNotExceedLimit := s.state.DBForWriteRPCWithLimit()
	if !dbIsInitialized {
		return nil, convert.ToRPCError(errDatabaseIsNotInitializedYet)
	}
	if !requestDoesNotExceedLimit {
		s.metrics.overloadRejected.Inc(1)
		return nil, convert.ToRPCError(errServerIsOverloaded)
	}
	if db.IsOverloaded() {
		s.metrics.overloadRejected.Inc(1)
		return nil, convert.ToRPCError(errServerIsOverloaded)
	}

	return db, nil
}

func (s *service) writeRPCCompleted() {
	if s.state.maxOutstandingWriteRPCs == 0 {
		// Nothing to do since we're not tracking the number outstanding RPCs.
		return
	}

	s.state.DecNumOutstandingWriteRPCs()
}

func (s *service) startReadRPCWithDB() (storage.Database, error) {
	if s.state.maxOutstandingReadRPCs == 0 {
		// No limitations on number of outstanding requests.
		return s.startRPCWithDB()
	}

	db, dbIsInitialized, requestDoesNotExceedLimit := s.state.DBForReadRPCWithLimit()
	if !dbIsInitialized {
		return nil, convert.ToRPCError(errDatabaseIsNotInitializedYet)
	}
	if !requestDoesNotExceedLimit {
		s.metrics.overloadRejected.Inc(1)
		return nil, convert.ToRPCError(errServerIsOverloaded)
	}
	if db.IsOverloaded() {
		s.metrics.overloadRejected.Inc(1)
		return nil, convert.ToRPCError(errServerIsOverloaded)
	}

	return db, nil
}

func (s *service) readRPCCompleted() {
	if s.state.maxOutstandingReadRPCs == 0 {
		// Nothing to do since we're not tracking the number outstanding RPCs.
		return
	}

	s.state.DecNumOutstandingReadRPCs()
}

func (s *service) startRPCWithDB() (storage.Database, error) {
	db, ok := s.state.DB()
	if !ok {
		return nil, convert.ToRPCError(errDatabaseIsNotInitializedYet)
	}

	if db.IsOverloaded() {
		s.metrics.overloadRejected.Inc(1)
		return nil, convert.ToRPCError(errServerIsOverloaded)
	}

	return db, nil
}

func (s *service) newID(ctx context.Context, id []byte) ident.ID {
	checkedBytes := s.pools.checkedBytesWrapper.Get(id)
	return s.pools.id.GetBinaryID(ctx, checkedBytes)
}

func (s *service) newPooledID(
	ctx context.Context,
	id []byte,
	p *writeBatchPooledReq,
) ident.ID {
	if result, ok := p.nextPooledID(id); ok {
		return result
	}
	return s.newID(ctx, id)
}

func (s *service) readEncoded(
	ctx context.Context,
	db storage.Database,
	nsID, tsID ident.ID,
	start, end time.Time,
) ([]*rpc.Segments, *rpc.Error) {
	encoded, err := db.ReadEncoded(ctx, nsID, tsID, start, end)
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

func (s *service) newPooledTagsDecoder(
	ctx context.Context,
	encodedTags []byte,
	p *writeBatchPooledReq,
) (serialize.TagDecoder, error) {
	if decoder, ok := p.nextPooledTagDecoder(encodedTags); ok {
		if err := decoder.Err(); err != nil {
			return nil, err
		}
		return decoder, nil
	}
	return s.newTagsDecoder(ctx, encodedTags)
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

type writeBatchPooledReq struct {
	pooledIDs      []writeBatchPooledReqID
	pooledIDsUsed  int
	writeReq       *rpc.WriteBatchRawRequest
	writeTaggedReq *rpc.WriteTaggedBatchRawRequest

	// We want to avoid allocating an intermediary slice of []error so we
	// just include all the error handling in this struct for performance
	// reasons since its pooled on a per-request basis anyways. This allows
	// us to use this object as a storage.IndexedErrorHandler and avoid allocating
	// []error in the storage package, as well as pool the []*rpc.WriteBatchRawError,
	// although the individual *rpc.WriteBatchRawError still need to be allocated
	// each time.
	nonRetryableErrors int
	retryableErrors    int
	errs               []*rpc.WriteBatchRawError

	pool *writeBatchPooledReqPool
}

func (r *writeBatchPooledReq) nextPooledID(idBytes []byte) (ident.ID, bool) {
	if r.pooledIDsUsed >= len(r.pooledIDs) {
		return nil, false
	}

	bytes := r.pooledIDs[r.pooledIDsUsed].bytes
	bytes.IncRef()
	bytes.Reset(idBytes)

	id := r.pooledIDs[r.pooledIDsUsed].id
	r.pooledIDsUsed++

	return id, true
}

func (r *writeBatchPooledReq) nextPooledTagDecoder(encodedTags []byte) (serialize.TagDecoder, bool) {
	if r.pooledIDsUsed >= len(r.pooledIDs) {
		return nil, false
	}

	bytes := r.pooledIDs[r.pooledIDsUsed].bytes
	bytes.IncRef()
	bytes.Reset(encodedTags)

	decoder := r.pooledIDs[r.pooledIDsUsed].tagDecoder
	decoder.Reset(bytes)

	r.pooledIDsUsed++

	return decoder, true
}

func (r *writeBatchPooledReq) Finalize() {
	// Reset the pooledIDsUsed and decrement the ref counts
	for i := 0; i < r.pooledIDsUsed; i++ {
		r.pooledIDs[i].bytes.DecRef()
	}
	r.pooledIDsUsed = 0

	// Return any pooled thrift byte slices to the thrift pool.
	if r.writeReq != nil {
		for _, elem := range r.writeReq.Elements {
			apachethrift.BytesPoolPut(elem.ID)
			// Ownership of the annotations has been transferred to the BatchWriter
			// so they will get returned the pool automatically by the commitlog once
			// it finishes writing them to disk via the finalization function that
			// gets set on the WriteBatch.
		}
		r.writeReq = nil
	}
	if r.writeTaggedReq != nil {
		for _, elem := range r.writeTaggedReq.Elements {
			apachethrift.BytesPoolPut(elem.ID)
			apachethrift.BytesPoolPut(elem.EncodedTags)
			// See comment above about not finalizing annotations here.
		}
		r.writeTaggedReq = nil
	}

	r.nonRetryableErrors = 0
	r.retryableErrors = 0
	if cap(r.errs) <= writeBatchPooledReqPoolMaxErrorsSliceSize {
		r.errs = r.errs[:0]
	} else {
		// Slice grew too large, throw it away and let a new one be
		// allocated on the next append call.
		r.errs = nil
	}

	// Return to pool
	r.pool.Put(r)
}

func (r *writeBatchPooledReq) HandleError(index int, err error) {
	if err == nil {
		return
	}

	if xerrors.IsInvalidParams(err) {
		r.nonRetryableErrors++
		r.errs = append(
			r.errs,
			tterrors.NewBadRequestWriteBatchRawError(index, err))
		return
	}

	r.retryableErrors++
	r.errs = append(
		r.errs,
		tterrors.NewWriteBatchRawError(index, err))
}

func (r *writeBatchPooledReq) addError(err *rpc.WriteBatchRawError) {
	r.errs = append(r.errs, err)
}

func (r *writeBatchPooledReq) writeBatchRawErrors() []*rpc.WriteBatchRawError {
	return r.errs
}

func (r *writeBatchPooledReq) numRetryableErrors() int {
	return r.retryableErrors
}

func (r *writeBatchPooledReq) numNonRetryableErrors() int {
	return r.nonRetryableErrors
}

type writeBatchPooledReqID struct {
	bytes      checked.Bytes
	id         ident.ID
	tagDecoder serialize.TagDecoder
}

type writeBatchPooledReqPool struct {
	pool pool.ObjectPool
}

func newWriteBatchPooledReqPool(
	size int,
	iopts instrument.Options,
) *writeBatchPooledReqPool {
	pool := pool.NewObjectPool(pool.NewObjectPoolOptions().
		SetSize(size).
		SetInstrumentOptions(iopts.SetMetricsScope(
			iopts.MetricsScope().SubScope("write-batch-pooled-req-pool"))))
	return &writeBatchPooledReqPool{pool: pool}
}

func (p *writeBatchPooledReqPool) Init(
	tagDecoderPool serialize.TagDecoderPool,
) {
	p.pool.Init(func() interface{} {
		// NB(r): Make pooled IDs 2x the default write batch size to account for
		// write tagged which also has encoded tags, plus an extra one for the
		// namespace
		pooledIDs := make([]writeBatchPooledReqID, 1+(2*client.DefaultWriteBatchSize))
		for i := range pooledIDs {
			pooledIDs[i].bytes = checked.NewBytes(nil, nil)
			pooledIDs[i].id = ident.BinaryID(pooledIDs[i].bytes)
			// BinaryID(..) incs the ref, we however don't want to pretend
			// the bytes is owned at this point since its not being used, so we
			// immediately dec a ref here to avoid calling get on this ID
			// being a valid call
			pooledIDs[i].bytes.DecRef()
			// Also ready a tag decoder
			pooledIDs[i].tagDecoder = tagDecoderPool.Get()
		}
		return &writeBatchPooledReq{
			pooledIDs: pooledIDs,
			pool:      p,
		}
	})
}

func (p *writeBatchPooledReqPool) Get() *writeBatchPooledReq {
	return p.pool.Get().(*writeBatchPooledReq)
}

func (p *writeBatchPooledReqPool) Put(v *writeBatchPooledReq) {
	p.pool.Put(v)
}

// finalizeAnnotationFn implements ts.FinalizeAnnotationFn because
// apachethrift.BytesPoolPut(b) returns a bool but ts.FinalizeAnnotationFn
// does not.
func finalizeAnnotationFn(b []byte) {
	apachethrift.BytesPoolPut(b)
}
