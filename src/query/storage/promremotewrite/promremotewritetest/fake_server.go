package promremotewritetest

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"

	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/stretchr/testify/require"
)

type FakePromRemoteWriteServer struct {
	lastWriteRequest *prompb.WriteRequest
	lastHTTPRequest  *http.Request
	addr             string
}

func NewFakePromRemoteWriteServer(t *testing.T) (*FakePromRemoteWriteServer, func()) {
	server := &FakePromRemoteWriteServer{}
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)
	HTTPServer := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			server.lastHTTPRequest = request
			req, err := remote.DecodeWriteRequest(request.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			server.lastWriteRequest = req
		}),
	}
	go func() {
		if err := HTTPServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			fmt.Printf("unexpected erver error %v \n", err)
		}
	}()
	server.addr = listener.Addr().String()
	return server, func() {
		require.NoError(t, HTTPServer.Shutdown(context.TODO()))
		require.NoError(t, HTTPServer.Close())
	}
}

func (s *FakePromRemoteWriteServer) GetLastRequest() *prompb.WriteRequest {
	return s.lastWriteRequest
}

func (s *FakePromRemoteWriteServer) GetLastHTTPRequest() *http.Request {
	return s.lastHTTPRequest
}

func (s *FakePromRemoteWriteServer) HTTPAddr() string {
	return fmt.Sprintf("http://%s", s.addr)
}

func (s *FakePromRemoteWriteServer) Reset() {
	s.lastWriteRequest = nil
	s.lastHTTPRequest = nil
}
