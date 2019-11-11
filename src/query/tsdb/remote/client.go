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
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/query/ts/m3db"
	"github.com/m3db/m3/src/query/ts/m3db/consolidators"
	"github.com/m3db/m3/src/query/util/logging"
	xsync "github.com/m3db/m3/src/x/sync"

	"google.golang.org/grpc"
)

// Client is the remote GRPC client.
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
	opts             m3db.Options
}

const initResultSize = 10

// NewGRPCClient creates a new remote GRPC client.
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
	dialOptions := []grpc.DialOption{
		grpc.WithBalancer(balancer),
		grpc.WithInsecure(),
	}
	dialOptions = append(dialOptions, additionalDialOpts...)
	cc, err := grpc.Dial("", dialOptions...)
	if err != nil {
		return nil, err
	}

	opts := m3db.NewOptions().
		SetTagOptions(tagOptions).
		SetLookbackDuration(lookbackDuration).
		SetConsolidationFunc(consolidators.TakeLast)

	client := rpc.NewQueryClient(cc)
	return &grpcClient{
		tagOptions:       tagOptions,
		client:           client,
		connection:       cc,
		poolWrapper:      poolWrapper,
		readWorkerPool:   readWorkerPool,
		lookbackDuration: lookbackDuration,
		opts:             opts,
	}, nil
}

func (c *grpcClient) Fetch(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (*storage.FetchResult, error) {
	result, err := c.fetchRaw(ctx, query, options)
	if err != nil {
		return nil, err
	}

	enforcer := options.Enforcer
	if enforcer == nil {
		enforcer = cost.NoopChainedEnforcer()
	}

	return storage.SeriesIteratorsToFetchResult(result.SeriesIterators,
		c.readWorkerPool, true, result.Metadata, enforcer, c.tagOptions,
		options)
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
) (m3.SeriesFetchResult, error) {
	fetchResult := m3.SeriesFetchResult{
		Metadata: block.NewResultMetadata(),
	}

	pools, err := c.waitForPools()
	if err != nil {
		return fetchResult, err
	}

	request, err := encodeFetchRequest(query, options)
	if err != nil {
		return fetchResult, err
	}

	// Send the id from the client to the remote server so that provides logging
	// TODO: replace id propagation with opentracing
	id := logging.ReadContextID(ctx)
	mdCtx := encodeMetadata(ctx, id)
	fetchClient, err := c.client.Fetch(mdCtx, request)
	if err != nil {
		return fetchResult, err
	}

	defer fetchClient.CloseSend()
	meta := block.NewResultMetadata()
	seriesIterators := make([]encoding.SeriesIterator, 0, initResultSize)
	for {
		select {
		// If query is killed during gRPC streaming, close the channel
		case <-ctx.Done():
			return fetchResult, ctx.Err()
		default:
		}

		result, err := fetchClient.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			return fetchResult, err
		}

		receivedMeta := decodeResultMetadata(result.GetMeta())
		meta = meta.CombineMetadata(receivedMeta)
		iters, err := decodeCompressedFetchResponse(result, pools)
		if err != nil {
			return fetchResult, err
		}

		seriesIterators = append(seriesIterators, iters.Iters()...)
	}

	fetchResult.Metadata = meta
	fetchResult.SeriesIterators = encoding.NewSeriesIterators(
		seriesIterators,
		nil,
	)

	return fetchResult, nil
}

func (c *grpcClient) FetchBlocks(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (block.Result, error) {
	// Override options with whatever is the current specified lookback duration.
	opts := c.opts.SetLookbackDuration(
		options.LookbackDurationOrDefault(c.opts.LookbackDuration()))

	// If using decoded block, return the legacy path.
	if options.BlockType == models.TypeDecodedBlock {
		fetchResult, err := c.Fetch(ctx, query, options)
		if err != nil {
			return block.Result{
				Metadata: block.NewResultMetadata(),
			}, err
		}

		return storage.FetchResultToBlockResult(fetchResult, query,
			opts.LookbackDuration(), options.Enforcer)
	}

	fetchResult, err := c.fetchRaw(ctx, query, options)
	if err != nil {
		return block.Result{
			Metadata: block.NewResultMetadata(),
		}, err
	}

	return m3.FetchResultToBlockResult(fetchResult, query, options, opts)
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

	request, err := encodeSearchRequest(query, options)
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

	metrics := make(models.Metrics, 0, initResultSize)
	meta := block.NewResultMetadata()
	defer searchClient.CloseSend()
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

		receivedMeta := decodeResultMetadata(received.GetMeta())
		meta = meta.CombineMetadata(receivedMeta)
		m, err := decodeSearchResponse(received, pools, c.tagOptions)
		if err != nil {
			return nil, err
		}

		metrics = append(metrics, m...)
	}

	return &storage.SearchResults{
		Metrics:  metrics,
		Metadata: meta,
	}, nil
}

func (c *grpcClient) CompleteTags(
	ctx context.Context,
	query *storage.CompleteTagsQuery,
	options *storage.FetchOptions,
) (*storage.CompleteTagsResult, error) {
	request, err := encodeCompleteTagsRequest(query, options)
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

	tags := make([]storage.CompletedTag, 0, initResultSize)
	meta := block.NewResultMetadata()
	defer completeTagsClient.CloseSend()
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
		} else if err != nil {
			return nil, err
		}

		receivedMeta := decodeResultMetadata(received.GetMeta())
		meta = meta.CombineMetadata(receivedMeta)
		result, err := decodeCompleteTagsResponse(received, query.CompleteNameOnly)
		if err != nil {
			return nil, err
		}

		tags = append(tags, result...)
	}

	return &storage.CompleteTagsResult{
		CompleteNameOnly: query.CompleteNameOnly,
		CompletedTags:    tags,
		Metadata:         meta,
	}, nil
}

func (c *grpcClient) Close() error {
	return c.connection.Close()
}
