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
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	rpc "github.com/m3db/m3/src/query/generated/proto/rpcpb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/pools"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/instrument"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const poolTimeout = time.Second * 10

// TODO: add metrics
type grpcServer struct {
	storage          m3.Storage
	queryContextOpts models.QueryContextOptions
	poolWrapper      *pools.PoolWrapper
	once             sync.Once
	pools            encoding.IteratorPools
	poolErr          error
	instrumentOpts   instrument.Options
}

// NewGRPCServer builds a grpc server which must be started later.
func NewGRPCServer(
	store m3.Storage,
	queryContextOpts models.QueryContextOptions,
	poolWrapper *pools.PoolWrapper,
	instrumentOpts instrument.Options,
) *grpc.Server {
	server := grpc.NewServer()
	grpcServer := &grpcServer{
		storage:          store,
		queryContextOpts: queryContextOpts,
		poolWrapper:      poolWrapper,
		instrumentOpts:   instrumentOpts,
	}

	rpc.RegisterQueryServer(server, grpcServer)
	return server
}

func (s *grpcServer) waitForPools() (encoding.IteratorPools, error) {
	s.once.Do(func() {
		s.pools, s.poolErr = s.poolWrapper.WaitForIteratorPools(poolTimeout)
	})

	return s.pools, s.poolErr
}

// Fetch reads decompressed series from M3 storage.
func (s *grpcServer) Fetch(
	message *rpc.FetchRequest,
	stream rpc.Query_FetchServer,
) error {
	ctx := retrieveMetadata(stream.Context(), s.instrumentOpts)
	logger := logging.WithContext(ctx, s.instrumentOpts)
	storeQuery, err := decodeFetchRequest(message)
	if err != nil {
		logger.Error("unable to decode fetch query", zap.Error(err))
		return err
	}

	// TODO(r): Allow propagation of limit from RPC request
	fetchOpts := storage.NewFetchOptions()
	fetchOpts.Limit = s.queryContextOpts.LimitMaxTimeseries

	result, cleanup, err := s.storage.FetchCompressed(ctx, storeQuery, fetchOpts)
	defer cleanup()
	if err != nil {
		logger.Error("unable to fetch local query", zap.Error(err))
		return err
	}

	pools, err := s.waitForPools()
	if err != nil {
		logger.Error("unable to get pools", zap.Error(err))
		return err
	}

	response, err := encodeToCompressedFetchResult(result, pools)
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

func (s *grpcServer) Search(
	message *rpc.SearchRequest,
	stream rpc.Query_SearchServer,
) error {
	var err error

	ctx := retrieveMetadata(stream.Context(), s.instrumentOpts)
	logger := logging.WithContext(ctx, s.instrumentOpts)
	searchQuery, err := decodeSearchRequest(message)
	if err != nil {
		logger.Error("unable to decode search query", zap.Error(err))
		return err
	}

	// TODO(r): Allow propagation of limit from RPC request
	fetchOpts := storage.NewFetchOptions()
	fetchOpts.Limit = s.queryContextOpts.LimitMaxTimeseries

	results, cleanup, err := s.storage.SearchCompressed(ctx, searchQuery,
		fetchOpts)
	defer cleanup()
	if err != nil {
		logger.Error("unable to search tags", zap.Error(err))
		return err
	}

	pools, err := s.waitForPools()
	if err != nil {
		logger.Error("unable to get pools", zap.Error(err))
		return err
	}

	response, err := encodeToCompressedSearchResult(results, pools)
	if err != nil {
		logger.Error("unable to encode search result", zap.Error(err))
		return err
	}

	err = stream.SendMsg(response)
	if err != nil {
		logger.Error("unable to send search result", zap.Error(err))
	}

	return err
}

func (s *grpcServer) CompleteTags(
	message *rpc.CompleteTagsRequest,
	stream rpc.Query_CompleteTagsServer,
) error {
	var err error

	ctx := retrieveMetadata(stream.Context(), s.instrumentOpts)
	logger := logging.WithContext(ctx, s.instrumentOpts)
	completeTagsQuery, err := decodeCompleteTagsRequest(message)
	if err != nil {
		logger.Error("unable to decode complete tags query", zap.Error(err))
		return err
	}

	// TODO(r): Allow propagation of limit from RPC request
	fetchOpts := storage.NewFetchOptions()
	fetchOpts.Limit = s.queryContextOpts.LimitMaxTimeseries

	results, err := s.storage.CompleteTags(ctx, completeTagsQuery, fetchOpts)
	if err != nil {
		logger.Error("unable to complete tags", zap.Error(err))
		return err
	}

	response, err := encodeToCompressedCompleteTagsResult(results)
	if err != nil {
		logger.Error("unable to encode complete tags result", zap.Error(err))
		return err
	}

	err = stream.SendMsg(response)
	if err != nil {
		logger.Error("unable to send complete tags result", zap.Error(err))
	}

	return err
}
