// Copyright (c) 2021 Uber Technologies, Inc.
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

package opentelemetry

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
)

func TestConfiguration(t *testing.T) {
	ctx := context.Background()
	ln := localListener(t)
	stop := otlpTraceServer(t, ln)
	defer stop()

	cfg := Configuration{
		ServiceName: "foo",
		Endpoint:    ln.Addr().String(),
		Insecure:    true,
		Attributes:  map[string]string{"bar": "baz"},
	}

	tracerProvider, err := cfg.NewTracerProvider(ctx, tally.NoopScope,
		TracerProviderOptions{})
	require.NoError(t, err)
	require.NotNil(t, tracerProvider)
}

// nopTraceService accepts and discards exported spans, standing in for a real
// OTLP collector so NewTracerProvider has something to dial.
type nopTraceService struct {
	coltracepb.UnimplementedTraceServiceServer
}

func (nopTraceService) Export(
	context.Context, *coltracepb.ExportTraceServiceRequest,
) (*coltracepb.ExportTraceServiceResponse, error) {
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

func otlpTraceServer(t *testing.T, ln net.Listener) func() {
	t.Helper()
	srv := grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(srv, nopTraceService{})
	go func() { _ = srv.Serve(ln) }()
	return srv.Stop
}

func localListener(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	return ln
}
