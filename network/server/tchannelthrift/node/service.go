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
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/instrument"
	"github.com/m3db/m3db/network/server/tchannelthrift"
	"github.com/m3db/m3db/network/server/tchannelthrift/convert"
	tterrors "github.com/m3db/m3db/network/server/tchannelthrift/errors"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3db/ts"
	xio "github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/time"

	"github.com/uber-go/tally"
	"github.com/uber/tchannel-go/thrift"
)

type serviceMetrics struct {
	fetch               instrument.MethodMetrics
	fetchRawBatch       instrument.MethodMetrics
	write               instrument.MethodMetrics
	writeBatch          instrument.MethodMetrics
	fetchBlocks         instrument.MethodMetrics
	fetchBlocksMetadata instrument.MethodMetrics
	repair              instrument.MethodMetrics
	truncate            instrument.MethodMetrics
}

func newServiceMetrics(scope tally.Scope, samplingRate float64) serviceMetrics {
	return serviceMetrics{
		fetch:               instrument.NewMethodMetrics(scope, "fetch", samplingRate),
		fetchRawBatch:       instrument.NewMethodMetrics(scope, "fetchRawBatch", samplingRate),
		write:               instrument.NewMethodMetrics(scope, "write", samplingRate),
		writeBatch:          instrument.NewMethodMetrics(scope, "writeBatch", samplingRate),
		fetchBlocks:         instrument.NewMethodMetrics(scope, "fetchBlocks", samplingRate),
		fetchBlocksMetadata: instrument.NewMethodMetrics(scope, "fetchBlocksMetadata", samplingRate),
		repair:              instrument.NewMethodMetrics(scope, "repair", samplingRate),
		truncate:            instrument.NewMethodMetrics(scope, "truncate", samplingRate),
	}
}

// TODO(r): server side pooling for all return types from service methods
type service struct {
	sync.RWMutex

	db      storage.Database
	nowFn   clock.NowFn
	metrics serviceMetrics
	health  *rpc.NodeHealthResult_
}

// NewService creates a new node TChannel Thrift service
func NewService(db storage.Database) rpc.TChanNode {
	opts := db.Options()
	iops := opts.InstrumentOptions()
	scope := iops.MetricsScope().SubScope("service").Tagged(map[string]string{"serviceName": "node"})

	return &service{
		db:      db,
		nowFn:   opts.ClockOptions().NowFn(),
		metrics: newServiceMetrics(scope, iops.MetricsSamplingRate()),
		health:  &rpc.NodeHealthResult_{Ok: true, Status: "up", Bootstrapped: false},
	}
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

	encoded, err := s.db.ReadEncoded(
		ctx, ts.StringID(req.NameSpace), ts.StringID(req.ID), start, end)
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
		ts, tsErr := convert.ToValue(dp.Timestamp, req.ResultTimeType)
		if tsErr != nil {
			s.metrics.fetch.ReportError(s.nowFn().Sub(callStart))
			return nil, tterrors.NewBadRequestError(tsErr)
		}

		datapoint := rpc.NewDatapoint()
		datapoint.Timestamp = ts
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
		s.metrics.fetchRawBatch.ReportError(s.nowFn().Sub(callStart))
		return nil, tterrors.NewBadRequestError(xerrors.FirstError(rangeStartErr, rangeEndErr))
	}
	ns := ts.BinaryID(req.NameSpace)

	result := rpc.NewFetchBatchRawResult_()
	for i := range req.Ids {
		rawResult := rpc.NewFetchRawResult_()
		result.Elements = append(result.Elements, rawResult)

		encoded, err := s.db.ReadEncoded(ctx, ns, ts.BinaryID(req.Ids[i]), start, end)
		if err != nil {
			rawResult.Err = convert.ToRPCError(err)
			continue
		}

		segments := make([]*rpc.Segments, 0, len(encoded))
		for _, readers := range encoded {
			if s := convert.ToSegments(readers); s != nil {
				segments = append(segments, s)
			}
		}
		rawResult.Segments = segments
	}

	s.metrics.fetchRawBatch.ReportSuccess(s.nowFn().Sub(callStart))
	return result, nil
}

func (s *service) FetchBlocksRaw(tctx thrift.Context, req *rpc.FetchBlocksRawRequest) (*rpc.FetchBlocksRawResult_, error) {
	callStart := s.nowFn()
	ctx := tchannelthrift.Context(tctx)

	var blockStarts []time.Time

	ns, shardID := ts.BinaryID(req.NameSpace), uint32(req.Shard)
	res := rpc.NewFetchBlocksRawResult_()
	res.Elements = make([]*rpc.Blocks, len(req.Elements))
	for i, request := range req.Elements {
		id := ts.BinaryID(request.ID)
		blockStarts = blockStarts[:0]
		for _, start := range request.Starts {
			blockStarts = append(blockStarts, xtime.FromNanoseconds(start))
		}
		fetched, err := s.db.FetchBlocks(ctx, ns, shardID, id, blockStarts)
		if err != nil {
			s.metrics.fetchBlocks.ReportError(s.nowFn().Sub(callStart))
			return nil, convert.ToRPCError(err)
		}
		blocks := rpc.NewBlocks()
		blocks.ID = id.Data()
		blocks.Blocks = make([]*rpc.Block, len(fetched))
		for j, fetchedBlock := range fetched {
			block := rpc.NewBlock()
			block.Start = xtime.ToNanoseconds(fetchedBlock.Start())
			if err := fetchedBlock.Err(); err != nil {
				block.Err = convert.ToRPCError(err)
			} else {
				block.Segments = convert.ToSegments(fetchedBlock.Readers())
			}
			blocks.Blocks[j] = block
		}
		res.Elements[i] = blocks
	}

	s.metrics.fetchBlocks.ReportSuccess(s.nowFn().Sub(callStart))
	return res, nil
}

func (s *service) FetchBlocksMetadataRaw(tctx thrift.Context, req *rpc.FetchBlocksMetadataRawRequest) (*rpc.FetchBlocksMetadataRawResult_, error) {
	callStart := s.nowFn()
	ctx := tchannelthrift.Context(tctx)

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

	ns, shardID := ts.BinaryID(req.NameSpace), uint32(req.Shard)
	fetched, nextPageToken, err := s.db.FetchBlocksMetadata(ctx,
		ns, shardID, req.Limit, pageToken, includeSizes, includeChecksums)
	if err != nil {
		s.metrics.fetchBlocksMetadata.ReportError(s.nowFn().Sub(callStart))
		return nil, convert.ToRPCError(err)
	}
	result := rpc.NewFetchBlocksMetadataRawResult_()
	result.NextPageToken = nextPageToken
	result.Elements = make([]*rpc.BlocksMetadata, len(fetched))
	for i, fetchedMetadata := range fetched {
		blocksMetadata := rpc.NewBlocksMetadata()
		blocksMetadata.ID = fetchedMetadata.ID().Data()
		fetchedMetadataBlocks := fetchedMetadata.Blocks()
		blocksMetadata.Blocks = make([]*rpc.BlockMetadata, len(fetchedMetadataBlocks))
		for j, fetchedMetadataBlock := range fetchedMetadataBlocks {
			blockMetadata := rpc.NewBlockMetadata()
			blockMetadata.Start = xtime.ToNanoseconds(fetchedMetadataBlock.Start())
			blockMetadata.Size = fetchedMetadataBlock.Size()
			if checksum := fetchedMetadataBlock.Checksum(); checksum != nil {
				value := int64(*checksum)
				blockMetadata.Checksum = &value
			}
			if err := fetchedMetadataBlock.Err(); err != nil {
				blockMetadata.Err = convert.ToRPCError(err)
			}
			blocksMetadata.Blocks[j] = blockMetadata
		}
		result.Elements[i] = blocksMetadata
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
	timestamp := xtime.FromNormalizedTime(dp.Timestamp, d)
	err = s.db.Write(
		ctx, ts.StringID(req.NameSpace), ts.StringID(req.ID), timestamp, dp.Value, unit, dp.Annotation)
	if err != nil {
		s.metrics.write.ReportError(s.nowFn().Sub(callStart))
		return convert.ToRPCError(err)
	}
	s.metrics.write.ReportSuccess(s.nowFn().Sub(callStart))
	return nil
}

func (s *service) WriteBatchRaw(tctx thrift.Context, req *rpc.WriteBatchRawRequest) error {
	callStart := s.nowFn()
	ctx := tchannelthrift.Context(tctx)
	ns := ts.BinaryID(req.NameSpace)

	var errs []*rpc.WriteBatchRawError
	for i, elem := range req.Elements {
		unit, unitErr := convert.ToUnit(elem.Datapoint.TimestampType)
		if unitErr != nil {
			errs = append(errs, tterrors.NewBadRequestWriteBatchRawError(i, unitErr))
			continue
		}
		d, err := unit.Value()
		if err != nil {
			errs = append(errs, tterrors.NewBadRequestWriteBatchRawError(i, err))
			continue
		}
		timestamp := xtime.FromNormalizedTime(elem.Datapoint.Timestamp, d)
		err = s.db.Write(
			ctx, ns, ts.BinaryID(elem.ID), timestamp, elem.Datapoint.Value, unit, elem.Datapoint.Annotation)
		if err != nil {
			if xerrors.IsInvalidParams(err) {
				errs = append(errs, tterrors.NewBadRequestWriteBatchRawError(i, err))
			} else {
				errs = append(errs, tterrors.NewWriteBatchRawError(i, err))
			}
		}
	}

	if len(errs) > 0 {
		s.metrics.writeBatch.ReportError(s.nowFn().Sub(callStart))
		batchErrs := rpc.NewWriteBatchRawErrors()
		batchErrs.Errors = errs
		return batchErrs
	}

	s.metrics.writeBatch.ReportSuccess(s.nowFn().Sub(callStart))
	return nil
}

func (s *service) Repair(tctx thrift.Context) error {
	callStart := s.nowFn()
	err := s.db.Repair()
	if err != nil {
		s.metrics.repair.ReportError(s.nowFn().Sub(callStart))
		return convert.ToRPCError(err)
	}
	s.metrics.repair.ReportSuccess(s.nowFn().Sub(callStart))
	return nil
}

func (s *service) Truncate(tctx thrift.Context, req *rpc.TruncateRequest) (r *rpc.TruncateResult_, err error) {
	callStart := s.nowFn()
	truncated, err := s.db.Truncate(ts.BinaryID(req.NameSpace))
	if err != nil {
		s.metrics.truncate.ReportError(s.nowFn().Sub(callStart))
		return nil, convert.ToRPCError(err)
	}

	s.metrics.truncate.ReportSuccess(s.nowFn().Sub(callStart))
	res := rpc.NewTruncateResult_()
	res.NumSeries = truncated
	return res, nil
}
