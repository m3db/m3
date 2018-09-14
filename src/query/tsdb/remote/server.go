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
	"errors"
	"io"
	"net"

	rpc "github.com/m3db/m3/src/query/generated/proto/rpcpb"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util/logging"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type grpcServer struct {
	storage storage.Storage
}

func newServer(store storage.Storage) *grpcServer {
	return &grpcServer{
		storage: store,
	}
}

// CreateNewGrpcServer creates server, given context local storage
func CreateNewGrpcServer(store storage.Storage) *grpc.Server {
	server := grpc.NewServer()
	grpcServer := newServer(store)
	rpc.RegisterQueryServer(server, grpcServer)

	return server
}

// StartNewGrpcServer starts server on given address, then notifies channel
func StartNewGrpcServer(server *grpc.Server, address string, waitForStart chan<- struct{}) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	waitForStart <- struct{}{}
	return server.Serve(lis)
}

// FetchDecompressed reads decompressed series from local storage
func (s *grpcServer) FetchDecompressed(
	message *rpc.FetchMessage,
	stream rpc.Query_FetchDecompressedServer,
) error {
	storeQuery, id, err := DecodeFetchMessage(message)
	ctx := logging.NewContextWithID(stream.Context(), id)
	logger := logging.WithContext(ctx)

	if err != nil {
		logger.Error("unable to decode fetch query", zap.Any("error", err))
		return err
	}

	// Iterate while there are more results
	for {
		result, err := s.storage.Fetch(ctx, storeQuery, nil)

		if err != nil {
			logger.Error("unable to fetch local query", zap.Any("error", err))
			return err
		}
		err = stream.Send(EncodeFetchResult(result))

		if err != nil {
			logger.Error("unable to send fetch result", zap.Any("error", err))
			return err
		}
		if !result.HasNext {
			break
		}
	}
	return nil
}

// FetchTagged reads from local storage
// TODO: implement
func (s *grpcServer) FetchTagged(message *rpc.FetchMessage, stream rpc.Query_FetchTaggedServer) error {
	return errors.New("not implemented")
}

// Write writes to local storage
func (s *grpcServer) Write(stream rpc.Query_WriteServer) error {
	for {
		message, err := stream.Recv()
		ctx := stream.Context()
		logger := logging.WithContext(ctx)

		if err == io.EOF {
			return nil
		}
		if err != nil {
			logger.Error("unable to use remote write", zap.Any("error", err))
			return err
		}
		query, id := DecodeWriteMessage(message)
		ctx = logging.NewContextWithID(ctx, id)
		logger = logging.WithContext(ctx)

		err = s.storage.Write(ctx, query)
		if err != nil {
			logger.Error("unable to write local query", zap.Any("error", err))
			return err
		}
	}
}
