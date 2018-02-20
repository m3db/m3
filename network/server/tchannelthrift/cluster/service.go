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

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/network/server/tchannelthrift"
	"github.com/m3db/m3db/network/server/tchannelthrift/convert"
	tterrors "github.com/m3db/m3db/network/server/tchannelthrift/errors"
	"github.com/m3db/m3x/checked"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/uber/tchannel-go/thrift"
)

var (
	// errIllegalTagValues raised when the tags specified are in-correct
	errIllegalTagValues = errors.New("illegal tag values specified")

	// errRequiresDatapoint raised when a datapoint is not provided
	errRequiresDatapoint = fmt.Errorf("requires datapoint")
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
		if client.IsBadRequestError(err) {
			return nil, tterrors.NewBadRequestError(err)
		}
		return nil, tterrors.NewInternalError(err)
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

func (s *service) FetchTagged(ctx thrift.Context, req *rpc.FetchTaggedRequest) (*rpc.FetchTaggedResult_, error) {
	if req == nil {
		return nil, tterrors.NewBadRequestError(fmt.Errorf("nil request"))
	}

	opts, err := convert.ToIndexQueryOpts(req)
	if err != nil {
		return nil, tterrors.NewBadRequestError(err)
	}

	query, err := convert.ToIndexQuery(req.Query)
	if err != nil {
		return nil, tterrors.NewBadRequestError(err)
	}

	session, err := s.session()
	if err != nil {
		return nil, tterrors.NewInternalError(err)
	}

	// TODO(prateek): support reading data too (i.e. check req.FetchData)
	queryResults, err := session.FetchTaggedIDs(query, opts)
	if err != nil {
		return nil, tterrors.NewInternalError(err)
	}

	return convert.ToRPCTaggedResult(queryResults)
}

func (s *service) Write(tctx thrift.Context, req *rpc.WriteRequest) error {
	session, err := s.session()
	if err != nil {
		return tterrors.NewInternalError(err)
	}
	if req.Datapoint == nil {
		return tterrors.NewBadRequestError(errRequiresDatapoint)
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
		if client.IsBadRequestError(err) {
			return tterrors.NewBadRequestError(err)
		}
		return tterrors.NewInternalError(err)
	}
	return nil
}

func (s *service) WriteTagged(tctx thrift.Context, req *rpc.WriteTaggedRequest) error {
	if req.Datapoint == nil {
		return tterrors.NewBadRequestError(errRequiresDatapoint)
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

	if req.TagNames == nil ||
		req.TagValues == nil ||
		len(req.TagNames) != len(req.TagValues) {
		return tterrors.NewBadRequestError(errIllegalTagValues)
	}

	session, err := s.session()
	if err != nil {
		return tterrors.NewInternalError(err)
	}

	iter, err := convert.ToTagsIter(req)
	if err != nil {
		return tterrors.NewBadRequestError(err)
	}

	var (
		ts   = xtime.FromNormalizedTime(dp.Timestamp, d)
		ctx  = tchannelthrift.Context(tctx)
		nsID = s.idPool.GetStringID(ctx, req.NameSpace)
		tsID = s.idPool.GetStringID(ctx, req.ID)
	)

	err = session.WriteTagged(nsID, tsID, iter, ts, dp.Value, unit, dp.Annotation)
	if err != nil {
		if client.IsBadRequestError(err) {
			return tterrors.NewBadRequestError(err)
		}
		return tterrors.NewInternalError(err)
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
