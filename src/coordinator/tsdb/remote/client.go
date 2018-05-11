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

	"github.com/m3db/m3db/src/coordinator/errors"
	"github.com/m3db/m3db/src/coordinator/generated/proto/rpc"
	"github.com/m3db/m3db/src/coordinator/storage"
	"github.com/m3db/m3db/src/coordinator/ts"
	"github.com/m3db/m3db/src/coordinator/util/logging"

	"google.golang.org/grpc"
)

// Client is an interface
type Client interface {
	storage.Querier
	storage.Appender
	Close() error
}

type grpcClient struct {
	client     rpc.QueryClient
	connection *grpc.ClientConn
}

// NewGrpcClient creates grpc client
func NewGrpcClient(addresses []string, additionalDialOpts ...grpc.DialOption) (Client, error) {
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
		client:     client,
		connection: cc,
	}, nil
}

// Fetch reads from remote client storage
func (c *grpcClient) Fetch(ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (*storage.FetchResult, error) {
	// Send the id from the client to the remote server so that provides logging
	id := logging.ReadContextID(ctx)
	fetchClient, err := c.client.Fetch(ctx, EncodeFetchMessage(query, id))
	if err != nil {
		return nil, err
	}

	defer fetchClient.CloseSend()

	tsSeries := make([]*ts.Series, 0)
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
		rpcSeries := result.GetSeries()
		tsSeries = append(tsSeries, DecodeFetchResult(ctx, rpcSeries)...)
	}

	return &storage.FetchResult{LocalOnly: false, SeriesList: tsSeries}, nil
}

func (c *grpcClient) FetchTags(ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (*storage.SearchResults, error) {
	return nil, nil
}

// Write writes to remote client storage
func (c *grpcClient) Write(ctx context.Context, query *storage.WriteQuery) error {
	client := c.client

	writeClient, err := client.Write(ctx)
	if err != nil {
		return err
	}

	id := logging.ReadContextID(ctx)
	rpcQuery := EncodeWriteMessage(query, id)
	err = writeClient.Send(rpcQuery)

	if err != nil && err != io.EOF {
		return err
	}

	_, err = writeClient.CloseAndRecv()
	if err == io.EOF {
		return nil
	}
	return err
}

func (c *grpcClient) FetchBlocks(
	ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (storage.BlockResult, error) {
	return storage.BlockResult{}, errors.ErrNotImplemented
}

// Close closes the underlying connection
func (c *grpcClient) Close() error {
	return c.connection.Close()
}
