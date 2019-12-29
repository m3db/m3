// Copyright (c) 2019 Uber Technologies, Inc.
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

package tchannelthrift

import (
	"sync"

	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/x/instrument"

	apachethrift "github.com/apache/thrift/lib/go/thrift"
	"github.com/uber-go/tally"
	"github.com/uber/tchannel-go/thrift"
)

var _ thrift.TChanClient = (*snappyTChanClient)(nil)

type snappyTChanClient struct {
	client thrift.TChanClient
}

// NewSnappyTChanClient creates a new snappy TChanClient.
func NewSnappyTChanClient(client thrift.TChanClient) thrift.TChanClient {
	return &snappyTChanClient{client: client}
}

func (c *snappyTChanClient) Call(
	ctx thrift.Context,
	serviceName, methodName string,
	req, resp apachethrift.TStruct,
) (bool, error) {
	snappyReq := snappyTStructPool.Get().(*snappyTStruct)
	snappyReq.Reset(req, apachethrift.SnappyCompressType)

	snappyResp := snappyTStructPool.Get().(*snappyTStruct)
	snappyResp.Reset(resp, apachethrift.NoneCompressType)

	result, err := c.client.Call(ctx, serviceName, methodName, snappyReq, snappyResp)

	snappyTStructPool.Put(snappyReq)
	snappyTStructPool.Put(snappyResp)

	return result, err
}

var snappyTStructPool = sync.Pool{
	New: func() interface{} {
		return &snappyTStruct{}
	},
}

var _ apachethrift.TStruct = (*snappyTStruct)(nil)

type snappyTStruct struct {
	thriftStruct      apachethrift.TStruct
	writeCompressType apachethrift.CompressType
}

func (s *snappyTStruct) Reset(
	thriftStruct apachethrift.TStruct,
	writeCompressType apachethrift.CompressType,
) {
	s.thriftStruct = thriftStruct
	s.writeCompressType = writeCompressType
}

func (s *snappyTStruct) Write(p apachethrift.TProtocol) error {
	compressibleProtocol, ok := p.(apachethrift.TCompressibleProtocol)
	if ok {
		opts := apachethrift.WriteCompressibleMessageBeginOptions{
			CompressType: s.writeCompressType,
		}
		err := compressibleProtocol.WriteCompressibleMessageBegin(opts)
		if err != nil {
			return err
		}
	}

	err := s.thriftStruct.Write(p)

	var writeEndErr error
	if ok {
		// Always make sure to read call read message end even if encounter
		// an error so that cleanup can occur.
		writeEndErr = compressibleProtocol.WriteCompressibleMessageEnd()
	}

	// Handle write error.
	if err != nil {
		return err
	}

	return writeEndErr
}

func (s *snappyTStruct) Read(p apachethrift.TProtocol) error {
	compressibleProtocol, ok := p.(apachethrift.TCompressibleProtocol)
	if ok {
		err := compressibleProtocol.ReadCompressibleMessageBegin()
		if err != nil {
			return err
		}
	}

	err := s.thriftStruct.Read(p)

	var readEndErr error
	if ok {
		// Always make sure to read call read message end even if encounter
		// an error so that cleanup can occur.
		_, readEndErr = compressibleProtocol.ReadCompressibleMessageEnd()
	}

	// Handle read error.
	if err != nil {
		return err
	}

	return readEndErr
}

type snappyTChanNodeServer struct {
	thrift.TChanServer
	metrics snappyTChanNodeServerMetrics
}

func newSnappyTChanNodeServerMetrics(
	scope tally.Scope,
) snappyTChanNodeServerMetrics {
	scope = scope.SubScope("dbnode-server").
		Tagged(map[string]string{"server": "node"})
	traffic := make(map[snappyTChanNodeServerTrafficMetricsKey]snappyTChanNodeServerTrafficMetrics)
	for _, compressType := range apachethrift.ValidCompressTypes() {
		key := snappyTChanNodeServerTrafficMetricsKey{
			compressType: compressType,
		}
		trafficScope := scope.Tagged(map[string]string{
			"compress": compressType.String(),
		})
		traffic[key] = newSnappyTChanNodeServerTrafficMetrics(trafficScope)
	}
	unknownTraffic := newSnappyTChanNodeServerTrafficMetrics(
		scope.Tagged(map[string]string{
			"compress": "unknown",
		}))
	return snappyTChanNodeServerMetrics{
		traffic:        traffic,
		unknownTraffic: unknownTraffic,
	}
}

type snappyTChanNodeServerMetrics struct {
	traffic        map[snappyTChanNodeServerTrafficMetricsKey]snappyTChanNodeServerTrafficMetrics
	unknownTraffic snappyTChanNodeServerTrafficMetrics
}

type snappyTChanNodeServerTrafficMetricsKey struct {
	compressType apachethrift.CompressType
}

type snappyTChanNodeServerTrafficMetrics struct {
	requestsSuccess tally.Counter
	requestsError   tally.Counter
}

func newSnappyTChanNodeServerTrafficMetrics(
	scope tally.Scope,
) snappyTChanNodeServerTrafficMetrics {
	const requests = "requests"
	return snappyTChanNodeServerTrafficMetrics{
		requestsSuccess: scope.Tagged(map[string]string{
			"result": "success",
		}).Counter(requests),
		requestsError: scope.Tagged(map[string]string{
			"result": "error",
		}).Counter(requests),
	}
}

// NewSnappyTChanNodeServer returns a new snappy TChanNodeServer.
func NewSnappyTChanNodeServer(
	handler rpc.TChanNode,
	instrumentOpts instrument.Options,
) thrift.TChanServer {
	return &snappyTChanNodeServer{
		TChanServer: rpc.NewTChanNodeServer(handler),
		metrics:     newSnappyTChanNodeServerMetrics(instrumentOpts.MetricsScope()),
	}
}

func (s *snappyTChanNodeServer) trafficMetrics(
	compressType apachethrift.CompressType,
) snappyTChanNodeServerTrafficMetrics {
	elem, ok := s.metrics.traffic[snappyTChanNodeServerTrafficMetricsKey{
		compressType: compressType,
	}]
	if ok {
		return elem
	}
	return s.metrics.unknownTraffic
}

func (s *snappyTChanNodeServer) Handle(
	ctx thrift.Context,
	methodName string,
	protocol apachethrift.TProtocol,
) (bool, apachethrift.TStruct, error) {
	compressibleProtocol, ok := protocol.(apachethrift.TCompressibleProtocol)
	if ok {
		readStartErr := compressibleProtocol.ReadCompressibleMessageBegin()
		if readStartErr != nil {
			return false, nil, readStartErr
		}
	}

	// Handle request.
	result, resp, err := s.TChanServer.Handle(ctx, methodName, protocol)

	var (
		reqReadResult = apachethrift.ReadCompressibleMessageEndResult{
			CompressType: apachethrift.NoneCompressType,
		}
		readEndErr error
	)
	if ok {
		// Always make sure to read call read message end even if encounter
		// an error so that cleanup can occur.
		reqReadResult, readEndErr = compressibleProtocol.ReadCompressibleMessageEnd()
	}

	// Record metrics.
	trafficMetrics := s.trafficMetrics(reqReadResult.CompressType)
	if err != nil {
		trafficMetrics.requestsError.Inc(1)
	} else {
		trafficMetrics.requestsSuccess.Inc(1)
	}

	// Return err if request wasn't handled successfully.
	if err != nil {
		return result, resp, err
	}

	if ok && reqReadResult.CompressType != apachethrift.NoneCompressType {
		// If the request was compressed and result is successful, then
		// wrap the response in a snappy struct that will compress the
		// response back to the client.
		resp = &snappyTStruct{
			thriftStruct:      resp,
			writeCompressType: reqReadResult.CompressType,
		}
	}

	return result, resp, readEndErr
}
