package remote

import (
	"context"
	"io"
	"net"

	"github.com/m3db/m3coordinator/generated/proto/m3coordinator"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/util/logging"
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
	logging.InitWithCores(nil)

	return server
}

// StartNewGrpcServer starts server on given address
func StartNewGrpcServer(server *grpc.Server, address string) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	go server.Serve(lis)
	return nil
}

// Fetch reads from local storage
func (s *grpcServer) Fetch(query *rpc.FetchQuery, stream rpc.Query_FetchServer) error {
	storeQuery, id, err := DecodeFetchQuery(query)
	ctx := logging.NewContextWithID(context.TODO(), id)
	logger := logging.WithContext(ctx)
	defer logger.Sync()
	if err != nil {
		logger.Error("Unable to decode fetch query", zap.Any("error", err))
		return err
	}

	result, err := s.storage.Fetch(ctx, storeQuery)
	if err != nil {
		logger.Error("Unable to fetch local query", zap.Any("error", err))
		return err
	}
	return stream.Send(EncodeFetchResult(result))
}

// Write writes to local storage
func (s *grpcServer) Write(stream rpc.Query_WriteServer) error {
	for {
		write, err := stream.Recv()
		ctx := context.TODO()
		logger := logging.WithContext(ctx)
		defer logger.Sync()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			logger.Error("Unable to use remote write", zap.Any("error", err))
			return err
		}
		query, id := DecodeWriteQuery(write)
		ctx = logging.NewContextWithID(context.TODO(), id)
		logger = logging.WithContext(ctx)

		err = s.storage.Write(ctx, query)
		if err != nil {
			logger.Error("Unable to write local query", zap.Any("error", err))
			return err
		}
	}
}
