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
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/cost"
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
	tagOptions       models.TagOptions
	client           rpc.QueryClient
	connection       *grpc.ClientConn
	poolWrapper      *pools.PoolWrapper
	readWorkerPool   xsync.PooledWorkerPool
	once             sync.Once
	pools            encoding.IteratorPools
	poolErr          error
	lookbackDuration time.Duration
}

const initResultSize = 10

// NewGRPCClient creates grpc client
func NewGRPCClient(
	addresses []string,
	poolWrapper *pools.PoolWrapper,
	readWorkerPool xsync.PooledWorkerPool,
	tagOptions models.TagOptions,
	lookbackDuration time.Duration,
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
		tagOptions:       tagOptions,
		client:           client,
		connection:       cc,
		poolWrapper:      poolWrapper,
		readWorkerPool:   readWorkerPool,
		lookbackDuration: lookbackDuration,
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

	enforcer := options.Enforcer
	if enforcer == nil {
		enforcer = cost.NoopChainedEnforcer()
	}

	return storage.SeriesIteratorsToFetchResult(iters, c.readWorkerPool,
		true, enforcer, c.tagOptions)
}

func (c *grpcClient) waitForPools() (encoding.IteratorPools, error) {
	c.once.Do(func() {
		c.pools, c.poolErr = c.poolWrapper.WaitForIteratorPools(poolTimeout)
	})

	return c.pools, c.poolErr
}

func (c *grpcClient) fetchRaw(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (encoding.SeriesIterators, error) {
	pools, err := c.waitForPools()
	if err != nil {
		return nil, err
	}

	request, err := encodeFetchRequest(query)
	if err != nil {
		return nil, err
	}

	// Send the id from the client to the remote server so that provides logging
	// TODO: replace id propagation with opentracing
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
		case <-ctx.Done():
			return nil, ctx.Err()
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

	enforcer := options.Enforcer
	if enforcer == nil {
		enforcer = cost.NoopChainedEnforcer()
	}

	fetchResult, err := storage.SeriesIteratorsToFetchResult(
		iters,
		c.readWorkerPool,
		true,
		enforcer,
		c.tagOptions,
	)
	if err != nil {
		return block.Result{}, err
	}

	res, err := storage.FetchResultToBlockResult(fetchResult, query, c.lookbackDuration, options.Enforcer)
	if err != nil {
		return block.Result{}, err
	}

	return res, nil
}

func (c *grpcClient) SearchSeries(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (*storage.SearchResults, error) {
	pools, err := c.waitForPools()
	if err != nil {
		return nil, err
	}

	request, err := encodeSearchRequest(query)
	if err != nil {
		return nil, err
	}

	// Send the id from the client to the remote server so that provides logging
	// TODO: replace id propagation with opentracing
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
		case <-ctx.Done():
			return nil, ctx.Err()
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
	// TODO: replace id propagation with opentracing
	id := logging.ReadContextID(ctx)
	// TODO: add relevant fields to the metadata
	mdCtx := encodeMetadata(ctx, id)
	completeTagsClient, err := c.client.CompleteTags(mdCtx, request)
	if err != nil {
		return nil, err
	}

	defer completeTagsClient.CloseSend()
	var accumulatedTags storage.CompleteTagsResultBuilder
	for {
		select {
		// If query is killed during gRPC streaming, close the channel
		case <-ctx.Done():
			return nil, ctx.Err()
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
			accumulatedTags = storage.NewCompleteTagsResultBuilder(result.CompleteNameOnly)
		}

		err = accumulatedTags.Add(result)
		if err != nil {
			return nil, err
		}
	}

	// Sort tags in the result post-merge.
	built := accumulatedTags.Build()
	return &built, nil
}

// Close closes the underlying connection
func (c *grpcClient) Close() error {
	return c.connection.Close()
}
