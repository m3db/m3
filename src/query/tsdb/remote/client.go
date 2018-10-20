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
	"context"
	"io"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/errors"
	rpc "github.com/m3db/m3/src/query/generated/proto/rpcpb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/pools"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util/logging"
	xsync "github.com/m3db/m3x/sync"

	"google.golang.org/grpc"
)

// Client is the grpc client
type Client interface {
	storage.Querier
	Close() error
}

type grpcClient struct {
	tagOptions     models.TagOptions
	client         rpc.QueryClient
	connection     *grpc.ClientConn
	poolWrapper    *pools.PoolWrapper
	readWorkerPool xsync.PooledWorkerPool
}

const initResultSize = 10

// NewGRPCClient creates grpc client
func NewGRPCClient(
	addresses []string,
	poolWrapper *pools.PoolWrapper,
	readWorkerPool xsync.PooledWorkerPool,
	tagOptions models.TagOptions,
	additionalDialOpts ...grpc.DialOption,
) (Client, error) {
	if len(addresses) == 0 {
		return nil, errors.ErrNoClientAddresses
	}

	resolver := newStaticResolver(addresses)
	balancer := grpc.RoundRobin(resolver)
	dialOptions := []grpc.DialOption{grpc.WithBalancer(balancer), grpc.WithInsecure()}
	dialOptions = append(dialOptions, additionalDialOpts...)
	cc, err := grpc.Dial("", dialOptions...)
	if err != nil {
		return nil, err
	}

	client := rpc.NewQueryClient(cc)
	return &grpcClient{
		tagOptions:     tagOptions,
		client:         client,
		connection:     cc,
		poolWrapper:    poolWrapper,
		readWorkerPool: readWorkerPool,
	}, nil
}

// Fetch reads from remote client storage
func (c *grpcClient) Fetch(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (*storage.FetchResult, error) {
	iters, err := c.fetchRaw(ctx, query, options)
	if err != nil {
		return nil, err
	}

	return storage.SeriesIteratorsToFetchResult(iters, c.readWorkerPool, true, c.tagOptions)
}

func (c *grpcClient) fetchRaw(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (encoding.SeriesIterators, error) {
	pools, err := c.poolWrapper.WaitForIteratorPools(poolTimeout)
	if err != nil {
		return nil, err
	}

	request, err := encodeFetchRequest(query)
	if err != nil {
		return nil, err
	}

	// Send the id from the client to the remote server so that provides logging
	id := logging.ReadContextID(ctx)
	mdCtx := encodeMetadata(ctx, id)
	fetchClient, err := c.client.Fetch(mdCtx, request)
	if err != nil {
		return nil, err
	}

	defer fetchClient.CloseSend()
	seriesIterators := make([]encoding.SeriesIterator, 0, initResultSize)
	for {
		select {
		// If query is killed during gRPC streaming, close the channel
		case <-options.KillChan:
			return nil, errors.ErrQueryInterrupted
		default:
		}

		result, err := fetchClient.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		iters, err := decodeCompressedFetchResponse(result, pools)
		if err != nil {
			return nil, err
		}

		seriesIterators = append(seriesIterators, iters.Iters()...)
	}

	return encoding.NewSeriesIterators(
		seriesIterators,
		nil,
	), nil
}

// TODO: change to iterator block logic
func (c *grpcClient) FetchBlocks(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (block.Result, error) {
	iters, err := c.fetchRaw(ctx, query, options)
	if err != nil {
		return block.Result{}, err
	}

	fetchResult, err := storage.SeriesIteratorsToFetchResult(
		iters,
		c.readWorkerPool,
		true,
		c.tagOptions,
	)
	if err != nil {
		return block.Result{}, err
	}

	res, err := storage.FetchResultToBlockResult(fetchResult, query)
	if err != nil {
		return block.Result{}, err
	}

	return res, nil
}

func (c *grpcClient) FetchTags(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (*storage.SearchResults, error) {
	pools, err := c.poolWrapper.WaitForIteratorPools(poolTimeout)
	if err != nil {
		return nil, err
	}

	request, err := encodeSearchRequest(query)
	if err != nil {
		return nil, err
	}

	// Send the id from the client to the remote server so that provides logging
	id := logging.ReadContextID(ctx)
	// TODO: add relevant fields to the metadata
	mdCtx := encodeMetadata(ctx, id)
	searchClient, err := c.client.Search(mdCtx, request)
	if err != nil {
		return nil, err
	}

	defer searchClient.CloseSend()
	metrics := make(models.Metrics, 0, initResultSize)
	for {
		select {
		// If query is killed during gRPC streaming, close the channel
		case <-options.KillChan:
			return nil, errors.ErrQueryInterrupted
		default:
		}

		received, err := searchClient.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		m, err := decodeSearchResponse(received, pools, c.tagOptions)
		if err != nil {
			return nil, err
		}

		metrics = append(metrics, m...)
	}

	return &storage.SearchResults{
		Metrics: metrics,
	}, nil
}

// CompleteTags returns autocompleted tags
func (c *grpcClient) CompleteTags(
	ctx context.Context,
	query *storage.CompleteTagsQuery,
	options *storage.FetchOptions,
) (*storage.CompleteTagsResult, error) {
	request, err := encodeCompleteTagsRequest(query)
	if err != nil {
		return nil, err
	}

	// Send the id from the client to the remote server so that provides logging
	id := logging.ReadContextID(ctx)
	// TODO: add relevant fields to the metadata
	mdCtx := encodeMetadata(ctx, id)
	completeTagsClient, err := c.client.CompleteTags(mdCtx, request)
	if err != nil {
		return nil, err
	}

	defer completeTagsClient.CloseSend()
	var accumulatedTags *storage.CompleteTagsResult
	for {
		select {
		// If query is killed during gRPC streaming, close the channel
		case <-options.KillChan:
			return nil, errors.ErrQueryInterrupted
		default:
		}

		received, err := completeTagsClient.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		result, err := decodeCompleteTagsResponse(received)
		if err != nil {
			return nil, err
		}

		if accumulatedTags == nil {
			accumulatedTags = result
		} else {
			err = accumulatedTags.MergeWith(result)
			if err != nil {
				return nil, err
			}
		}
	}

	// Sort tags in the result post-merge.
	accumulatedTags.Finalize()
	return accumulatedTags, nil
}

// Close closes the underlying connection
func (c *grpcClient) Close() error {
	return c.connection.Close()
}
