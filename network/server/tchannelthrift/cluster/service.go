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
	"fmt"
	"sync"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/network/server/tchannelthrift/convert"
	tterrors "github.com/m3db/m3db/network/server/tchannelthrift/errors"
	"github.com/m3db/m3db/network/server/tchannelthrift/thrift/gen-go/rpc"
	"github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/time"

	"github.com/uber/tchannel-go/thrift"
)

type service struct {
	sync.RWMutex

	client m3db.Client
	active m3db.Session
	health *rpc.HealthResult_
}

// NewService creates a new cluster TChannel Thrift service
func NewService(client m3db.Client) rpc.TChanCluster {
	s := &service{
		client: client,
		health: &rpc.HealthResult_{Ok: true, Status: "up"},
	}
	// Attempt to warm session
	go s.session()
	return s
}

func (s *service) session() (m3db.Session, error) {
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
	session, err := s.client.NewSession()
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

	start, rangeStartErr := convert.ValueToTime(req.RangeStart, req.RangeType)
	end, rangeEndErr := convert.ValueToTime(req.RangeEnd, req.RangeType)
	if rangeStartErr != nil || rangeEndErr != nil {
		return nil, tterrors.NewBadRequestError(xerrors.FirstError(rangeStartErr, rangeEndErr))
	}

	it, err := session.Fetch(req.ID, start, end)
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

func (s *service) Write(tctx thrift.Context, req *rpc.WriteRequest) error {
	session, err := s.session()
	if err != nil {
		return tterrors.NewInternalError(err)
	}
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
	err = session.Write(req.ID, ts, req.Datapoint.Value, unit, req.Datapoint.Annotation)
	if err != nil {
		if client.IsBadRequestError(err) {
			return tterrors.NewBadRequestError(err)
		}
		return tterrors.NewInternalError(err)
	}
	return nil
}
