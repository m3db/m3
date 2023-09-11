// Copyright (c) 2020 Uber Technologies, Inc.
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

package grpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	testpb "github.com/m3db/m3/src/x/generated/proto/test"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestInterceptors(t *testing.T) {
	server, client, scope := testServerClient(t)
	defer server.Close()

	_, err := client.PingEmpty(context.Background(), &testpb.Empty{})
	require.NoError(t, err)

	_, err = client.PingError(context.Background(), &testpb.PingRequest{
		ErrorCodeReturned: uint32(codes.FailedPrecondition),
	})
	require.Error(t, err)

	ss, err := client.PingList(context.Background(), &testpb.PingRequest{})
	require.NoError(t, err)

	var recv int64
	for {
		_, err := ss.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		recv++
	}

	expectedCounters := map[string]int64{
		"grpc_client_handled_total+grpc_code=FailedPrecondition,grpc_method=PingError,grpc_service=m3.test.TestService,grpc_type=unary": 1,
		"grpc_client_handled_total+grpc_code=OK,grpc_method=PingEmpty,grpc_service=m3.test.TestService,grpc_type=unary":                 1,
		"grpc_client_handled_total+grpc_code=OK,grpc_method=PingList,grpc_service=m3.test.TestService,grpc_type=server_stream":          1,
		"grpc_client_msg_received_total+grpc_method=PingEmpty,grpc_service=m3.test.TestService,grpc_type=unary":                         0,
		"grpc_client_msg_received_total+grpc_method=PingError,grpc_service=m3.test.TestService,grpc_type=unary":                         0,
		"grpc_client_msg_received_total+grpc_method=PingList,grpc_service=m3.test.TestService,grpc_type=server_stream":                  recv,
		"grpc_client_msg_sent_total+grpc_method=PingEmpty,grpc_service=m3.test.TestService,grpc_type=unary":                             0,
		"grpc_client_msg_sent_total+grpc_method=PingError,grpc_service=m3.test.TestService,grpc_type=unary":                             0,
		"grpc_client_msg_sent_total+grpc_method=PingList,grpc_service=m3.test.TestService,grpc_type=server_stream":                      1,
		"grpc_client_started_total+grpc_method=PingEmpty,grpc_service=m3.test.TestService,grpc_type=unary":                              1,
		"grpc_client_started_total+grpc_method=PingError,grpc_service=m3.test.TestService,grpc_type=unary":                              1,
		"grpc_client_started_total+grpc_method=PingList,grpc_service=m3.test.TestService,grpc_type=server_stream":                       1,
	}

	snapshot := scope.Snapshot()

	// Enable to see current values:
	// for k, v := range snapshot.Counters() {
	// 	fmt.Printf("\"%s\": %d,\n", k, v.Value())
	// }
	// for k, v := range snapshot.Histograms() {
	// 	fmt.Printf("%s = %v\n", k, v.Values())
	// }

	for k, v := range expectedCounters {
		counter, ok := snapshot.Counters()[k]
		require.True(t, ok, fmt.Sprintf("metric missing: %s", k))
		require.Equal(t, v, counter.Value())
	}

	expectedHistogramsTotalBucketCounts := map[string]int64{
		"grpc_client_handling_seconds+grpc_method=PingEmpty,grpc_service=m3.test.TestService,grpc_type=unary":                 1,
		"grpc_client_handling_seconds+grpc_method=PingError,grpc_service=m3.test.TestService,grpc_type=unary":                 1,
		"grpc_client_handling_seconds+grpc_method=PingList,grpc_service=m3.test.TestService,grpc_type=server_stream":          1,
		"grpc_client_msg_recv_handling_seconds+grpc_method=PingList,grpc_service=m3.test.TestService,grpc_type=server_stream": recv + 1,
		"grpc_client_msg_send_handling_seconds+grpc_method=PingList,grpc_service=m3.test.TestService,grpc_type=server_stream": 1,
	}
	for k, v := range expectedHistogramsTotalBucketCounts {
		histogram, ok := snapshot.Histograms()[k]
		require.True(t, ok, fmt.Sprintf("metric missing: %s", k))

		var actualBucketCount int64
		for _, bucketCount := range histogram.Values() {
			actualBucketCount += bucketCount
		}
		for _, bucketCount := range histogram.Durations() {
			actualBucketCount += bucketCount
		}
		require.Equal(t, v, actualBucketCount, fmt.Sprintf("metric bucket count mismatch: %s", k))
	}
}

func testServerClient(t *testing.T) (net.Listener, testpb.TestServiceClient, tally.TestScope) {
	serverListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	testpb.RegisterTestServiceServer(server, testService{})

	go func() {
		server.Serve(serverListener)
	}()

	s, _ := tally.NewRootScope(tally.ScopeOptions{Separator: "_"}, 0)
	scope, ok := s.(tally.TestScope)
	require.True(t, ok)

	clientConn, err := grpc.Dial(
		serverListener.Addr().String(),
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithUnaryInterceptor(UnaryClientInterceptor(InterceptorInstrumentOptions{Scope: s})),
		grpc.WithStreamInterceptor(StreamClientInterceptor(InterceptorInstrumentOptions{Scope: s})),
		grpc.WithTimeout(2*time.Second))
	require.NoError(t, err)
	return serverListener, testpb.NewTestServiceClient(clientConn), scope
}

type testService struct {
}

func (s testService) PingEmpty(ctx context.Context, _ *testpb.Empty) (*testpb.PingResponse, error) {
	return &testpb.PingResponse{Value: "ping response", Counter: 42}, nil
}

func (s testService) Ping(ctx context.Context, ping *testpb.PingRequest) (*testpb.PingResponse, error) {
	return &testpb.PingResponse{Value: ping.Value, Counter: 42}, nil
}

func (s testService) PingError(ctx context.Context, ping *testpb.PingRequest) (*testpb.Empty, error) {
	code := codes.Code(ping.ErrorCodeReturned)
	return nil, status.Errorf(code, "an error")
}

func (s testService) PingList(ping *testpb.PingRequest, stream testpb.TestService_PingListServer) error {
	if ping.ErrorCodeReturned != 0 {
		return status.Errorf(codes.Code(ping.ErrorCodeReturned), "an_error")
	}
	for i := 0; i < 20; i++ {
		stream.Send(&testpb.PingResponse{Value: ping.Value, Counter: int32(i)})
	}
	return nil
}
