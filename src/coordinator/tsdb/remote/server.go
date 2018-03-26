package remote

import (
	"io"
	"net"

	"github.com/m3db/m3coordinator/generated/proto/rpc"
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

// Fetch reads from local storage
func (s *grpcServer) Fetch(message *rpc.FetchMessage, stream rpc.Query_FetchServer) error {
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
