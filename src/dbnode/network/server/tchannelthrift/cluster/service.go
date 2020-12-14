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

package cluster

import (
	"errors"
	"fmt"
	"sync"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/dbnode/network/server/tchannelthrift"
	"github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/convert"
	tterrors "github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/errors"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/x/checked"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/uber/tchannel-go/thrift"
)

type service struct {
	sync.RWMutex

	client client.Client
	active client.Session
	opts   client.Options
	idPool ident.Pool
	health *rpc.HealthResult_
}

// NewService creates a new cluster TChannel Thrift service
func NewService(client client.Client) rpc.TChanCluster {
	s := &service{
		client: client,
		opts:   client.Options(),
		idPool: client.Options().IdentifierPool(),
		health: &rpc.HealthResult_{Ok: true, Status: "up"},
	}
	return s
}

func (s *service) session() (client.Session, error) {
	s.RLock()
	session := s.active
	s.RUnlock()
	if session != nil {
		return session, nil
	}

	s.Lock()
	if s.active != nil {
		session := s.active
		s.Unlock()
		return session, nil
	}
	session, err := s.client.DefaultSession()
	if err != nil {
		s.Unlock()
		return nil, err
	}
	s.active = session
	s.Unlock()

	return session, nil
}

func (s *service) Close() error {
	var err error
	s.Lock()
	if s.active != nil {
		err = s.active.Close()
	}
	s.Unlock()
	return err
}

func (s *service) Health(ctx thrift.Context) (*rpc.HealthResult_, error) {
	s.RLock()
	health := s.health
	s.RUnlock()
	return health, nil
}

func (s *service) Query(tctx thrift.Context, req *rpc.QueryRequest) (*rpc.QueryResult_, error) {
	start, rangeStartErr := convert.ToTime(req.RangeStart, req.RangeType)
	end, rangeEndErr := convert.ToTime(req.RangeEnd, req.RangeType)

	if rangeStartErr != nil || rangeEndErr != nil {
		return nil, tterrors.NewBadRequestError(xerrors.FirstError(rangeStartErr, rangeEndErr))
	}

	q, err := convert.FromRPCQuery(req.Query)
	if err != nil {
		return nil, convert.ToRPCError(err)
	}

	nsID := ident.StringID(req.NameSpace)
	opts := index.QueryOptions{
		StartInclusive: start,
		EndExclusive:   end,
	}

	if l := req.Limit; l != nil {
		opts.SeriesLimit = int(*l)
	}
	if len(req.Source) > 0 {
		opts.Source = req.Source
	}

	session, err := s.session()
	if err != nil {
		return nil, convert.ToRPCError(err)
	}

	if req.NoData != nil && *req.NoData {
		results, metadata, err := session.FetchTaggedIDs(nsID,
			index.Query{Query: q}, opts)
		if err != nil {
			return nil, convert.ToRPCError(err)
		}

		result := &rpc.QueryResult_{
			Exhaustive: metadata.Exhaustive,
		}

		for results.Next() {
			_, tsID, tags := results.Current()
			curr := &rpc.QueryResultElement{
				ID: tsID.String(),
			}
			result.Results = append(result.Results, curr)
			for tags.Next() {
				t := tags.Current()
				curr.Tags = append(curr.Tags, &rpc.Tag{
					Name:  t.Name.String(),
					Value: t.Value.String(),
				})
			}
			if err := tags.Err(); err != nil {
				return nil, convert.ToRPCError(err)
			}
			tags.Close()
		}

		if err := results.Err(); err != nil {
			return nil, convert.ToRPCError(err)
		}
		return result, nil
	}

	results, metadata, err := session.FetchTagged(nsID,
		index.Query{Query: q}, opts)
	if err != nil {
		return nil, convert.ToRPCError(err)
	}

	result := &rpc.QueryResult_{
		Results:    make([]*rpc.QueryResultElement, 0, results.Len()),
		Exhaustive: metadata.Exhaustive,
	}

	for _, series := range results.Iters() {
		curr := &rpc.QueryResultElement{
			ID: series.ID().String(),
		}
		result.Results = append(result.Results, curr)
		tags := series.Tags()
		for tags.Next() {
			t := tags.Current()
			curr.Tags = append(curr.Tags, &rpc.Tag{
				Name:  t.Name.String(),
				Value: t.Value.String(),
			})
		}
		if err := tags.Err(); err != nil {
			return nil, convert.ToRPCError(err)
		}
		tags.Close()

		var datapoints []*rpc.Datapoint
		for series.Next() {
			dp, _, annotation := series.Current()

			timestamp, timestampErr := convert.ToValue(dp.Timestamp, req.ResultTimeType)
			if timestampErr != nil {
				return nil, xerrors.NewInvalidParamsError(timestampErr)
			}

			datapoints = append(datapoints, &rpc.Datapoint{
				Timestamp:  timestamp,
				Value:      dp.Value,
				Annotation: annotation,
			})
		}
		if err := series.Err(); err != nil {
			return nil, convert.ToRPCError(err)
		}
		curr.Datapoints = datapoints
	}

	results.Close()
	return result, nil
}

func (s *service) Fetch(tctx thrift.Context, req *rpc.FetchRequest) (*rpc.FetchResult_, error) {
	session, err := s.session()
	if err != nil {
		return nil, tterrors.NewInternalError(err)
	}

	start, rangeStartErr := convert.ToTime(req.RangeStart, req.RangeType)
	end, rangeEndErr := convert.ToTime(req.RangeEnd, req.RangeType)
	if rangeStartErr != nil || rangeEndErr != nil {
		return nil, tterrors.NewBadRequestError(xerrors.FirstError(rangeStartErr, rangeEndErr))
	}

	ctx := tchannelthrift.Context(tctx)
	nsID := s.idPool.GetStringID(ctx, req.NameSpace)
	tsID := s.idPool.GetStringID(ctx, req.ID)

	it, err := session.Fetch(nsID, tsID, start, end)
	if err != nil {
		return nil, convert.ToRPCError(err)
	}

	defer it.Close()

	result := rpc.NewFetchResult_()
	// Make datapoints an initialized empty array for JSON serialization as empty array than null
	result.Datapoints = make([]*rpc.Datapoint, 0)

	for it.Next() {
		dp, _, annotation := it.Current()
		ts, tsErr := convert.ToValue(dp.Timestamp, req.ResultTimeType)
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

func (s *service) Aggregate(ctx thrift.Context, req *rpc.AggregateQueryRequest) (*rpc.AggregateQueryResult_, error) {
	session, err := s.session()
	if err != nil {
		return nil, tterrors.NewInternalError(err)
	}

	ns, query, opts, err := convert.FromRPCAggregateQueryRequest(req)
	if err != nil {
		return nil, tterrors.NewBadRequestError(err)
	}

	iter, metadata, err := session.Aggregate(ns, query, opts)
	if err != nil {
		return nil, convert.ToRPCError(err)
	}
	defer iter.Finalize()

	response := &rpc.AggregateQueryResult_{
		Exhaustive: metadata.Exhaustive,
	}
	for iter.Next() {
		name, values := iter.Current()
		responseElem := &rpc.AggregateQueryResultTagNameElement{
			TagName: name.String(),
		}
		responseElem.TagValues = make([]*rpc.AggregateQueryResultTagValueElement, 0, values.Remaining())
		for values.Next() {
			value := values.Current()
			responseElem.TagValues = append(responseElem.TagValues, &rpc.AggregateQueryResultTagValueElement{
				TagValue: value.String(),
			})
		}
		if err := values.Err(); err != nil {
			return nil, convert.ToRPCError(err)
		}
		response.Results = append(response.Results, responseElem)
	}
	if err := iter.Err(); err != nil {
		return nil, convert.ToRPCError(err)
	}
	return response, nil
}

func (s *service) Write(tctx thrift.Context, req *rpc.WriteRequest) error {
	session, err := s.session()
	if err != nil {
		return tterrors.NewInternalError(err)
	}
	if req.Datapoint == nil {
		return tterrors.NewBadRequestError(fmt.Errorf("requires datapoint"))
	}
	dp := req.Datapoint
	unit, unitErr := convert.ToUnit(dp.TimestampTimeType)
	if unitErr != nil {
		return tterrors.NewBadRequestError(unitErr)
	}
	d, err := unit.Value()
	if err != nil {
		return tterrors.NewBadRequestError(err)
	}
	ts := xtime.FromNormalizedTime(dp.Timestamp, d)

	ctx := tchannelthrift.Context(tctx)
	nsID := s.idPool.GetStringID(ctx, req.NameSpace)
	tsID := s.idPool.GetStringID(ctx, req.ID)
	err = session.Write(nsID, tsID, ts, dp.Value, unit, dp.Annotation)
	if err != nil {
		return convert.ToRPCError(err)
	}
	return nil
}

func (s *service) WriteTagged(tctx thrift.Context, req *rpc.WriteTaggedRequest) error {
	session, err := s.session()
	if err != nil {
		return tterrors.NewInternalError(err)
	}
	if req.Datapoint == nil {
		return tterrors.NewBadRequestError(fmt.Errorf("requires datapoint"))
	}
	dp := req.Datapoint
	unit, unitErr := convert.ToUnit(dp.TimestampTimeType)
	if unitErr != nil {
		return tterrors.NewBadRequestError(unitErr)
	}
	d, err := unit.Value()
	if err != nil {
		return tterrors.NewBadRequestError(err)
	}
	ts := xtime.FromNormalizedTime(dp.Timestamp, d)

	ctx := tchannelthrift.Context(tctx)
	nsID := s.idPool.GetStringID(ctx, req.NameSpace)
	tsID := s.idPool.GetStringID(ctx, req.ID)
	var tags ident.Tags
	for _, tag := range req.Tags {
		tags.Append(s.idPool.GetStringTag(ctx, tag.Name, tag.Value))
	}
	err = session.WriteTagged(nsID, tsID, ident.NewTagsIterator(tags),
		ts, dp.Value, unit, dp.Annotation)
	if err != nil {
		return convert.ToRPCError(err)
	}
	return nil
}

func (s *service) Truncate(tctx thrift.Context, req *rpc.TruncateRequest) (*rpc.TruncateResult_, error) {
	session, err := s.session()
	if err != nil {
		return nil, tterrors.NewInternalError(err)
	}

	adminSession, ok := session.(client.AdminSession)
	if !ok {
		return nil, tterrors.NewInternalError(errors.New("unable to get an admin session"))
	}

	nsID := ident.BinaryID(checked.NewBytes(req.NameSpace, nil))
	truncated, err := adminSession.Truncate(nsID)
	if err != nil {
		return nil, convert.ToRPCError(err)
	}

	res := rpc.NewTruncateResult_()
	res.NumSeries = truncated
	return res, nil
}
