package tchannelthrift

import (
	"fmt"

	"code.uber.internal/infra/memtsdb/services/m3dbnode/serve/tchannelthrift/thrift/gen-go/rpc"
	"code.uber.internal/infra/memtsdb/storage"
	xerrors "code.uber.internal/infra/memtsdb/x/errors"
	xio "code.uber.internal/infra/memtsdb/x/io"
	xtime "code.uber.internal/infra/memtsdb/x/time"

	"github.com/uber/tchannel-go/thrift"
)

type service struct {
	db storage.Database
}

// NewService creates a new TChannel Thrift compatible service
func NewService(db storage.Database) rpc.TChanNode {
	return &service{db: db}
}

func (s *service) Fetch(tctx thrift.Context, req *rpc.FetchRequest) (*rpc.FetchResult_, error) {
	ctx := s.db.Options().GetContextPool().Get()
	defer ctx.Close()

	start, rangeStartErr := valueToTime(req.RangeStart, req.RangeType)
	end, rangeEndErr := valueToTime(req.RangeEnd, req.RangeType)
	if rangeStartErr != nil || rangeEndErr != nil {
		return nil, newNodeBadRequestError(xerrors.FirstError(rangeStartErr, rangeEndErr))
	}

	encoded, err := s.db.ReadEncoded(ctx, req.ID, start, end)
	if err != nil {
		if xerrors.IsInvalidParams(err) {
			return nil, newNodeBadRequestError(err)
		}
		return nil, newNodeInternalError(err)
	}

	result := rpc.NewFetchResult_()

	// Make datapoints an initialized empty array for JSON serialization as empty array than null
	result.Datapoints = make([]*rpc.Datapoint, 0)

	if encoded == nil || len(encoded.Readers()) == 0 {
		return result, nil
	}

	for _, reader := range encoded.Readers() {
		newDecoderFn := s.db.Options().GetNewDecoderFn()
		it := newDecoderFn().Decode(reader)
		for it.Next() {
			dp, _, annotation := it.Current()
			ts, tsErr := timeToValue(dp.Timestamp, req.ResultTimeType)
			if tsErr != nil {
				return nil, newNodeBadRequestError(tsErr)
			}

			afterOrAtStart := !dp.Timestamp.Before(start)
			beforeOrAtEnd := !dp.Timestamp.After(end)
			if afterOrAtStart && beforeOrAtEnd {
				datapoint := rpc.NewDatapoint()
				datapoint.Timestamp = ts
				datapoint.Value = dp.Value
				datapoint.Annotation = annotation
				result.Datapoints = append(result.Datapoints, datapoint)
			}
		}
		if err := it.Err(); err != nil {
			return nil, newNodeInternalError(err)
		}
	}

	return result, nil
}

func (s *service) FetchRawBatch(tctx thrift.Context, req *rpc.FetchRawBatchRequest) (*rpc.FetchRawBatchResult_, error) {
	ctx := s.db.Options().GetContextPool().Get()
	defer ctx.Close()

	start, rangeStartErr := valueToTime(req.RangeStart, req.RangeType)
	end, rangeEndErr := valueToTime(req.RangeEnd, req.RangeType)
	if rangeStartErr != nil || rangeEndErr != nil {
		return nil, newNodeBadRequestError(xerrors.FirstError(rangeStartErr, rangeEndErr))
	}

	result := rpc.NewFetchRawBatchResult_()
	for i := range req.Ids {
		encoded, err := s.db.ReadEncoded(ctx, req.Ids[i], start, end)
		if err != nil {
			if xerrors.IsInvalidParams(err) {
				return nil, newNodeBadRequestError(err)
			}
			return nil, newNodeInternalError(err)
		}
		rawResult := rpc.NewFetchRawResult_()
		sgrs, err := xio.GetSegmentReaders(encoded)
		if err != nil {
			return nil, newNodeInternalError(err)
		}
		segments := make([]*rpc.Segment, 0, len(sgrs))
		for _, sgr := range sgrs {
			seg := sgr.Segment()
			segments = append(segments, &rpc.Segment{Head: seg.Head, Tail: seg.Tail})
		}
		rawResult.Segments = segments
		result.Elements = append(result.Elements, rawResult)
	}

	return result, nil
}

func (s *service) Write(tctx thrift.Context, req *rpc.WriteRequest) error {
	ctx := s.db.Options().GetContextPool().Get()
	defer ctx.Close()

	if req.Datapoint == nil {
		return newBadRequestWriteError(fmt.Errorf("requires datapoint"))
	}
	unit, unitErr := timeTypeToUnit(req.Datapoint.TimestampType)
	if unitErr != nil {
		return newBadRequestWriteError(unitErr)
	}
	d, err := unit.Value()
	if err != nil {
		return newBadRequestWriteError(err)
	}
	ts := xtime.FromNormalizedTime(req.Datapoint.Timestamp, d)
	err = s.db.Write(ctx, req.ID, ts, req.Datapoint.Value, unit, req.Datapoint.Annotation)
	if err != nil {
		if xerrors.IsInvalidParams(err) {
			return newBadRequestWriteError(err)
		}
		return newWriteError(err)
	}
	return nil
}

func (s *service) WriteBatch(tctx thrift.Context, req *rpc.WriteBatchRequest) error {
	ctx := s.db.Options().GetContextPool().Get()
	defer ctx.Close()

	var errs []*rpc.WriteBatchError
	for i, elem := range req.Elements {
		unit, unitErr := timeTypeToUnit(elem.Datapoint.TimestampType)
		if unitErr != nil {
			errs = append(errs, newBadRequestWriteBatchError(i, unitErr))
			continue
		}
		d, err := unit.Value()
		if err != nil {
			errs = append(errs, newBadRequestWriteBatchError(i, err))
			continue
		}
		ts := xtime.FromNormalizedTime(elem.Datapoint.Timestamp, d)
		err = s.db.Write(ctx, elem.ID, ts, elem.Datapoint.Value, unit, elem.Datapoint.Annotation)
		if err != nil {
			if xerrors.IsInvalidParams(err) {
				errs = append(errs, newBadRequestWriteBatchError(i, err))
			} else {
				errs = append(errs, newWriteBatchError(i, err))
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
