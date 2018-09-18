// Copyright (c) 2018 Uber Technologies, Inc.
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

package remote

import (
	"net"

	"github.com/m3db/m3/src/dbnode/encoding"
	rpc "github.com/m3db/m3/src/query/generated/proto/rpcpb"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util/logging"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// TODO: add metrics
type grpcServer struct {
	storage storage.Storage
	pools   encoding.IteratorPools
}

// CreateNewGrpcServer creates server, given context local storage
func CreateNewGrpcServer(
	store storage.Storage,
	pools encoding.IteratorPools,
) *grpc.Server {
	server := grpc.NewServer()
	grpcServer := &grpcServer{
		storage: store,
		pools:   pools,
	}
	rpc.RegisterQueryServer(server, grpcServer)

	return server
}

// StartNewGrpcServer starts server on given address, then notifies channel
func StartNewGrpcServer(
	server *grpc.Server,
	address string,
	waitForStart chan<- struct{},
) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	waitForStart <- struct{}{}
	return server.Serve(lis)
}

// Fetch reads decompressed series from local storage
func (s *grpcServer) Fetch(
	message *rpc.FetchRequest,
	stream rpc.Query_FetchServer,
) error {
	ctx := RetrieveMetadata(stream.Context())
	logger := logging.WithContext(ctx)
	storeQuery, err := DecodeFetchRequest(message)
	if err != nil {
		logger.Error("unable to decode fetch query", zap.Any("error", err))
		return err
	}

	result, err := s.storage.FetchRaw(ctx, storeQuery, nil)
	if err != nil {
		logger.Error("unable to fetch local query", zap.Error(err))
		return err
	}

	response, err := EncodeToCompressedFetchResult(result, s.pools)
	if err != nil {
		logger.Error("unable to compress query", zap.Error(err))
		return err
	}

	err = stream.Send(response)
	if err != nil {
		logger.Error("unable to send fetch result", zap.Error(err))
	}

	return err
}
