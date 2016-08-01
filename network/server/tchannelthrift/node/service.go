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

	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/network/server/tchannelthrift/convert"
	tterrors "github.com/m3db/m3db/network/server/tchannelthrift/errors"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/time"

	"github.com/uber/tchannel-go/thrift"
)

// TODO(r): server side pooling for all return types from service methods
type service struct {
	sync.RWMutex

	db     storage.Database
	health *rpc.HealthResult_
}

// NewService creates a new node TChannel Thrift service
func NewService(db storage.Database) rpc.TChanNode {
	return &service{
		db:     db,
		health: &rpc.HealthResult_{Ok: true, Status: "up"},
	}
}

func (s *service) Health(ctx thrift.Context) (*rpc.HealthResult_, error) {
	s.RLock()
	health := s.health
	s.RUnlock()
	return health, nil
}

func (s *service) Fetch(tctx thrift.Context, req *rpc.FetchRequest) (*rpc.FetchResult_, error) {
	ctx := s.db.Options().GetContextPool().Get()
	defer ctx.Close()

	start, rangeStartErr := convert.ValueToTime(req.RangeStart, req.RangeType)
	end, rangeEndErr := convert.ValueToTime(req.RangeEnd, req.RangeType)
	if rangeStartErr != nil || rangeEndErr != nil {
		return nil, tterrors.NewBadRequestError(xerrors.FirstError(rangeStartErr, rangeEndErr))
	}

	encoded, err := s.db.ReadEncoded(ctx, req.ID, start, end)
	if err != nil {
		if xerrors.IsInvalidParams(err) {
			return nil, tterrors.NewBadRequestError(err)
		}
		return nil, tterrors.NewInternalError(err)
	}

	result := rpc.NewFetchResult_()

	// Make datapoints an initialized empty array for JSON serialization as empty array than null
	result.Datapoints = make([]*rpc.Datapoint, 0)

	multiIt := s.db.Options().GetMultiReaderIteratorPool().Get()
	multiIt.ResetSliceOfSlices(encoding.NewReaderSliceOfSlicesFromSegmentReadersIterator(encoded))
	it := encoding.NewSeriesIterator(req.ID, start, end, []m3db.Iterator{multiIt}, nil)
	defer it.Close()
	for it.Next() {
		dp, _, annotation := it.Current()
		ts, tsErr := convert.TimeToValue(dp.Timestamp, req.ResultTimeType)
		if tsErr != nil {
			return nil, tterrors.NewBadRequestError(tsErr)
		}

		datapoint := rpc.NewDatapoint()
		datapoint.Timestamp = ts
		datapoint.Value = dp.Value
		datapoint.Annotation = annotation
		result.Datapoints = append(result.Datapoints, datapoint)
	}
	if err := it.Err(); err != nil {
		return nil, tterrors.NewInternalError(err)
	}

	return result, nil
}

func (s *service) FetchRawBatch(tctx thrift.Context, req *rpc.FetchRawBatchRequest) (*rpc.FetchRawBatchResult_, error) {
	ctx := s.db.Options().GetContextPool().Get()
	defer ctx.Close()

	start, rangeStartErr := convert.ValueToTime(req.RangeStart, req.RangeType)
	end, rangeEndErr := convert.ValueToTime(req.RangeEnd, req.RangeType)
	if rangeStartErr != nil || rangeEndErr != nil {
		return nil, tterrors.NewBadRequestError(xerrors.FirstError(rangeStartErr, rangeEndErr))
	}

	result := rpc.NewFetchRawBatchResult_()
	for i := range req.Ids {
		rawResult := rpc.NewFetchRawResult_()
		result.Elements = append(result.Elements, rawResult)

		encoded, err := s.db.ReadEncoded(ctx, req.Ids[i], start, end)
		if err != nil {
			if xerrors.IsInvalidParams(err) {
				rawResult.Err = tterrors.NewBadRequestError(err)
				continue
			}
			rawResult.Err = tterrors.NewInternalError(err)
			continue
		}

		segments := make([]*rpc.Segments, 0, len(encoded))
		for _, readers := range encoded {
			s := &rpc.Segments{}
			if len(readers) == 1 {
				seg := readers[0].Segment()
				s.Merged = &rpc.Segment{Head: seg.Head, Tail: seg.Tail}
			} else {
				for _, reader := range readers {
					seg := reader.Segment()
					s.Unmerged = append(s.Unmerged, &rpc.Segment{Head: seg.Head, Tail: seg.Tail})
				}
			}
			segments = append(segments, s)
		}
		rawResult.Segments = segments
	}

	return result, nil
}

func (s *service) Write(tctx thrift.Context, req *rpc.WriteRequest) error {
	ctx := s.db.Options().GetContextPool().Get()
	defer ctx.Close()

	if req.Datapoint == nil {
		return tterrors.NewBadRequestError(fmt.Errorf("requires datapoint"))
	}
	unit, unitErr := convert.TimeTypeToUnit(req.Datapoint.TimestampType)
	if unitErr != nil {
		return tterrors.NewBadRequestError(unitErr)
	}
	d, err := unit.Value()
	if err != nil {
		return tterrors.NewBadRequestError(err)
	}
	ts := xtime.FromNormalizedTime(req.Datapoint.Timestamp, d)
	err = s.db.Write(ctx, req.ID, ts, req.Datapoint.Value, unit, req.Datapoint.Annotation)
	if err != nil {
		if xerrors.IsInvalidParams(err) {
			return tterrors.NewBadRequestError(err)
		}
		return tterrors.NewInternalError(err)
	}
	return nil
}

func (s *service) WriteBatch(tctx thrift.Context, req *rpc.WriteBatchRequest) error {
	ctx := s.db.Options().GetContextPool().Get()
	defer ctx.Close()

	var errs []*rpc.WriteBatchError
	for i, elem := range req.Elements {
		unit, unitErr := convert.TimeTypeToUnit(elem.Datapoint.TimestampType)
		if unitErr != nil {
			errs = append(errs, tterrors.NewBadRequestWriteBatchError(i, unitErr))
			continue
		}
		d, err := unit.Value()
		if err != nil {
			errs = append(errs, tterrors.NewBadRequestWriteBatchError(i, err))
			continue
		}
		ts := xtime.FromNormalizedTime(elem.Datapoint.Timestamp, d)
		err = s.db.Write(ctx, elem.ID, ts, elem.Datapoint.Value, unit, elem.Datapoint.Annotation)
		if err != nil {
			if xerrors.IsInvalidParams(err) {
				errs = append(errs, tterrors.NewBadRequestWriteBatchError(i, err))
			} else {
				errs = append(errs, tterrors.NewWriteBatchError(i, err))
			}
		}
	}

	if len(errs) > 0 {
		batchErrs := rpc.NewWriteBatchErrors()
		batchErrs.Errors = errs
		return batchErrs
	}
	return nil
}
