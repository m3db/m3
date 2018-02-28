package remote

import (
	"context"
	"io"

	"github.com/m3db/m3coordinator/errors"
	"github.com/m3db/m3coordinator/generated/proto/m3coordinator"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/ts"
	"github.com/m3db/m3coordinator/util/logging"

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
func NewGrpcClient(addresses []string) (Client, error) {
	if len(addresses) == 0 {
		return nil, errors.ErrNoClientAddresses
	}
	resolver := newStaticResolver(addresses)
	balancer := grpc.RoundRobin(resolver)
	dialOption := grpc.WithBalancer(balancer)
	cc, err := grpc.Dial("", grpc.WithInsecure(), dialOption)
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

// Close closes the underlying connection
func (c *grpcClient) Close() error {
	return c.connection.Close()
}
