package promremotewritetest

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"

	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/stretchr/testify/require"
)

type FakePromRemoteWriteServer struct {
	mu               sync.Mutex
	lastWriteRequest *prompb.WriteRequest
	lastHTTPRequest  *http.Request
	addr             string
	respErr          error
}

func NewFakePromRemoteWriteServer(t *testing.T) (*FakePromRemoteWriteServer, func()) {
	server := &FakePromRemoteWriteServer{}
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)
	HTTPServer := &http.Server{Handler: http.HandlerFunc(server.handle)}
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

func (s *FakePromRemoteWriteServer) handle(w http.ResponseWriter, request *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastHTTPRequest = request
	req, err := remote.DecodeWriteRequest(request.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.lastWriteRequest = req
	if s.respErr != nil {
		http.Error(w, s.respErr.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *FakePromRemoteWriteServer) GetLastRequest() *prompb.WriteRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastWriteRequest
}

func (s *FakePromRemoteWriteServer) GetLastHTTPRequest() *http.Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastHTTPRequest
}

func (s *FakePromRemoteWriteServer) HTTPAddr() string {
	return fmt.Sprintf("http://%s", s.addr)
}

func (s *FakePromRemoteWriteServer) SetError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.respErr = err
}

func (s *FakePromRemoteWriteServer) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.respErr = nil
	s.lastWriteRequest = nil
	s.lastHTTPRequest = nil
}
