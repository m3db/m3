package tchannelthrift

import (
	"fmt"

	"code.uber.internal/infra/memtsdb/services/mdbnode/serve/tchannelthrift/thrift/gen-go/rpc"
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

func (s *service) Fetch(ctx thrift.Context, req *rpc.FetchRequest) (*rpc.FetchResult_, error) {
	start, rangeStartErr := valueToTime(req.RangeStart, req.RangeType)
	end, rangeEndErr := valueToTime(req.RangeEnd, req.RangeType)
	if rangeStartErr != nil || rangeEndErr != nil {
		return nil, newNodeBadRequestError(xerrors.FirstError(rangeStartErr, rangeEndErr))
	}

	encoded, err := s.db.FetchEncodedSegments(req.ID, start, end)
	if err != nil {
		return nil, newNodeInternalError(err)
	}

	result := rpc.NewFetchResult_()

	// Make datapoints an initialized empty array for JSON serialization as empty array than null
	result.Datapoints = make([]*rpc.Datapoint, 0)

	it := s.db.GetOptions().GetNewDecoderFn()().Decode(encoded)
	for it.Next() {
		dp, annotation := it.Current()
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

	return result, nil
}

func (s *service) FetchRawBatch(ctx thrift.Context, req *rpc.FetchRawBatchRequest) (*rpc.FetchRawBatchResult_, error) {
	start, rangeStartErr := valueToTime(req.RangeStart, req.RangeType)
	end, rangeEndErr := valueToTime(req.RangeEnd, req.RangeType)
	if rangeStartErr != nil || rangeEndErr != nil {
		return nil, newNodeBadRequestError(xerrors.FirstError(rangeStartErr, rangeEndErr))
	}

	result := rpc.NewFetchRawBatchResult_()
	for i := range req.Ids {
		encoded, err := s.db.FetchEncodedSegments(req.Ids[i], start, end)
		if err != nil {
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
			if seg == nil {
				continue
			}
			segments = append(segments, &rpc.Segment{Head: seg.Head, Tail: seg.Tail})
		}
		rawResult.Segments = segments
		result.Elements = append(result.Elements, rawResult)
	}

	return result, nil
}

func (s *service) Write(ctx thrift.Context, req *rpc.WriteRequest) error {
	if req.Datapoint == nil {
		return newNodeBadRequestError(fmt.Errorf("requires datapoint"))
	}
	unit, unitErr := timeTypeToUnit(req.Datapoint.TimestampType)
	if unitErr != nil {
		return newNodeBadRequestError(unitErr)
	}
	ts := xtime.FromNormalizedTime(req.Datapoint.Timestamp, unit)
	err := s.db.Write(req.ID, ts, req.Datapoint.Value, unit, req.Datapoint.Annotation)
	if err != nil {
		return newWriteError(err)
	}
	return nil
}

func (s *service) WriteBatch(ctx thrift.Context, req *rpc.WriteBatchRequest) error {
	var errs []*rpc.WriteBatchError
	for i, elem := range req.Elements {
		unit, unitErr := timeTypeToUnit(elem.Datapoint.TimestampType)
		if unitErr != nil {
			errs = append(errs, newWriteBatchError(i, unitErr))
			continue
		}

		ts := xtime.FromNormalizedTime(elem.Datapoint.Timestamp, unit)
		err := s.db.Write(elem.ID, ts, elem.Datapoint.Value, unit, elem.Datapoint.Annotation)
		if err != nil {
			errs = append(errs, newWriteBatchError(i, err))
		}
	}

	if len(errs) > 0 {
		batchErrs := rpc.NewWriteBatchErrors()
		batchErrs.Errors = errs
		return batchErrs
	}
	return nil
}
