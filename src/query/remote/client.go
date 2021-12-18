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
	goerrors "errors"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/errors"
	rpc "github.com/m3db/m3/src/query/generated/proto/rpcpb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/pools"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"github.com/m3db/m3/src/query/util/logging"
	xgrpc "github.com/m3db/m3/src/x/grpc"
	"github.com/m3db/m3/src/x/instrument"
)

const (
	initResultSize             = 10
	healthCheckInterval        = 60 * time.Second
	healthCheckTimeout         = 5 * time.Second
	healthCheckMetricName      = "health-check"
	healthCheckMetricResultTag = "result"
)

var (
	errAlreadyClosed = goerrors.New("already closed")

	errQueryStorageMetadataAttributesNotImplemented = goerrors.New(
		"remote storage does not implement QueryStorageMetadataAttributes",
	)

	// NB(r): These options tries to ensure we don't let connections go stale
	// and cause failed RPCs as a result.
	defaultDialOptions = []grpc.DialOption{
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			// After a duration of this time if the client doesn't see any activity it
			// pings the server to see if the transport is still alive.
			// If set below 10s, a minimum value of 10s will be used instead.
			Time: 10 * time.Second,
			// After having pinged for keepalive check, the client waits for a duration
			// of Timeout and if no activity is seen even after that the connection is
			// closed.
			Timeout: 20 * time.Second,
			// If true, client sends keepalive pings even with no active RPCs. If false,
			// when there are no active RPCs, Time and Timeout will be ignored and no
			// keepalive pings will be sent.
			PermitWithoutStream: true,
		}),
	}
)

// Client is the remote GRPC client.
type Client interface {
	storage.Querier
	Close() error
}

type grpcClient struct {
	state       grpcClientState
	client      rpc.QueryClient
	connection  *grpc.ClientConn
	poolWrapper *pools.PoolWrapper
	once        sync.Once
	pools       encoding.IteratorPools
	poolErr     error
	opts        m3.Options
	logger      *zap.Logger
	metrics     grpcClientMetrics
}

type grpcClientState struct {
	sync.RWMutex
	closed  bool
	closeCh chan struct{}
}

type grpcClientMetrics struct {
	healthCheckSuccess tally.Counter
	healthCheckError   tally.Counter
}

func newGRPCClientMetrics(s tally.Scope) grpcClientMetrics {
	s = s.SubScope("remote-client")
	return grpcClientMetrics{
		healthCheckSuccess: s.Tagged(map[string]string{
			healthCheckMetricResultTag: "success",
		}).Counter(healthCheckMetricName),
		healthCheckError: s.Tagged(map[string]string{
			healthCheckMetricResultTag: "error",
		}).Counter(healthCheckMetricName),
	}
}

// NewGRPCClient creates a new remote GRPC client.
func NewGRPCClient(
	name string,
	addresses []string,
	poolWrapper *pools.PoolWrapper,
	opts m3.Options,
	instrumentOpts instrument.Options,
	additionalDialOpts ...grpc.DialOption,
) (Client, error) {
	if len(addresses) == 0 {
		return nil, errors.ErrNoClientAddresses
	}

	// Set name if using a named client.
	if remote := strings.TrimSpace(name); remote != "" {
		instrumentOpts = instrumentOpts.
			SetMetricsScope(instrumentOpts.MetricsScope().Tagged(map[string]string{
				"remote-name": remote,
			}))
	}

	scope := instrumentOpts.MetricsScope()
	interceptorOpts := xgrpc.InterceptorInstrumentOptions{Scope: scope}

	resolver := newStaticResolver(addresses)
	balancer := grpc.RoundRobin(resolver)
	dialOptions := append([]grpc.DialOption{
		grpc.WithBalancer(balancer),
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(xgrpc.UnaryClientInterceptor(interceptorOpts)),
		grpc.WithStreamInterceptor(xgrpc.StreamClientInterceptor(interceptorOpts)),
	}, defaultDialOptions...)
	dialOptions = append(dialOptions, additionalDialOpts...)
	cc, err := grpc.Dial("", dialOptions...)
	if err != nil {
		return nil, err
	}

	client := rpc.NewQueryClient(cc)
	c := &grpcClient{
		state: grpcClientState{
			closeCh: make(chan struct{}),
		},
		client:      client,
		connection:  cc,
		poolWrapper: poolWrapper,
		opts:        opts,
		logger:      instrumentOpts.Logger(),
		metrics:     newGRPCClientMetrics(scope),
	}
	go c.healthCheckUntilClosed()
	return c, nil
}

func (c *grpcClient) QueryStorageMetadataAttributes(
	ctx context.Context,
	queryStart, queryEnd time.Time,
	opts *storage.FetchOptions,
) ([]storagemetadata.Attributes, error) {
	return nil, errQueryStorageMetadataAttributesNotImplemented
}

func (c *grpcClient) healthCheckUntilClosed() {
	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()

	for {
		if c.closed() {
			return // Abort early, closed already.
		}

		// Perform immediately so first check isn't delayed.
		err := c.healthCheck()

		if c.closed() {
			return // Don't report results, closed already.
		}

		if err != nil {
			c.metrics.healthCheckError.Inc(1)
			c.logger.Debug("remote storage client health check failed",
				zap.Error(err))
		} else {
			c.metrics.healthCheckSuccess.Inc(1)
		}

		select {
		case <-c.state.closeCh:
			return
		case <-ticker.C:
			// Continue to next check.
			continue
		}
	}
}

func (c *grpcClient) healthCheck() error {
	ctx, cancel := context.WithTimeout(context.Background(),
		healthCheckTimeout)
	_, err := c.client.Health(ctx, &rpc.HealthRequest{})
	cancel()
	return err
}

func (c *grpcClient) closed() bool {
	c.state.RLock()
	closed := c.state.closed
	c.state.RUnlock()
	return closed
}

func (c *grpcClient) waitForPools() (encoding.IteratorPools, error) {
	c.once.Do(func() {
		c.pools, c.poolErr = c.poolWrapper.WaitForIteratorPools(poolTimeout)
	})

	return c.pools, c.poolErr
}

func (c *grpcClient) FetchProm(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (storage.PromResult, error) {
	result, err := c.fetchRaw(ctx, query, options)
	if err != nil {
		return storage.PromResult{}, err
	}

	return storage.SeriesIteratorsToPromResult(
		ctx,
		result,
		c.opts.ReadWorkerPool(),
		c.opts.TagOptions(),
		c.opts.PromConvertOptions(),
		options)
}

func (c *grpcClient) fetchRaw(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (consolidators.SeriesFetchResult, error) {
	result, err := c.FetchCompressed(ctx, query, options)
	if err != nil {
		return consolidators.SeriesFetchResult{}, err
	}

	return result.FinalResult()
}

func (c *grpcClient) FetchCompressed(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (consolidators.MultiFetchResult, error) {
	if err := options.BlockType.Validate(); err != nil {
		// This is an invariant error; should not be able to get to here.
		return nil, instrument.InvariantErrorf("invalid block type on "+
			"fetch, got: %v with error %v", options.BlockType, err)
	}

	pools, err := c.waitForPools()
	if err != nil {
		return nil, err
	}

	request, err := encodeFetchRequest(query, options)
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

	fanout := consolidators.NamespaceCoversAllQueryRange
	matchOpts := c.opts.SeriesConsolidationMatchOptions()
	tagOpts := c.opts.TagOptions()
	limitOpts := consolidators.LimitOptions{
		Limit:             options.SeriesLimit,
		RequireExhaustive: options.RequireExhaustive,
	}
	result := consolidators.NewMultiFetchResult(fanout, pools, matchOpts, tagOpts, limitOpts)
	for {
		select {
		// If query is killed during gRPC streaming, close the channel
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		recvResult, err := fetchClient.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		receivedMeta := decodeResultMetadata(recvResult.GetMeta())
		iters, err := DecodeCompressedFetchResponse(recvResult, pools)
		result.Add(consolidators.MultiFetchResults{
			SeriesIterators: iters,
			Metadata:        receivedMeta,
			Attrs:           storagemetadata.Attributes{},
			Err:             err,
		})
	}

	return result, nil
}

func (c *grpcClient) FetchBlocks(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (block.Result, error) {
	// Override options with whatever is the current specified lookback duration.
	opts := c.opts.SetLookbackDuration(
		options.LookbackDurationOrDefault(c.opts.LookbackDuration()))

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
		m, err := decodeSearchResponse(received, pools, c.opts.TagOptions())
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
) (*consolidators.CompleteTagsResult, error) {
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

	tags := make([]consolidators.CompletedTag, 0, initResultSize)
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

	return &consolidators.CompleteTagsResult{
		CompleteNameOnly: query.CompleteNameOnly,
		CompletedTags:    tags,
		Metadata:         meta,
	}, nil
}

func (c *grpcClient) Close() error {
	c.state.Lock()
	defer c.state.Unlock()

	if c.state.closed {
		return errAlreadyClosed
	}
	c.state.closed = true

	close(c.state.closeCh)
	return c.connection.Close()
}
