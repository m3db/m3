// Package promremotetest provides test utilities.
package promremotetest

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"

	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/stretchr/testify/require"
)

// FakeServer is a fake http server handling prometheus remote write. Intended for test usage.
type FakeServer struct {
	mu               sync.Mutex
	lastWriteRequest *prompb.WriteRequest
	lastHTTPRequest  *http.Request
	addr             string
	respErr          error
}

// NewServer creates new instance of a fake server.
func NewServer(t *testing.T) (*FakeServer, func()) {
	server := &FakeServer{}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	HTTPServer := &http.Server{Handler: http.HandlerFunc(server.handle)}
	go func() {
		if err := HTTPServer.Serve(listener); err != nil && errors.Is(err, http.ErrServerClosed) {
			//nolint: forbidigo
			fmt.Printf("unexpected server error %v \n", err)
		}
	}()
	server.addr = listener.Addr().String()
	return server, func() {
		require.NoError(t, HTTPServer.Shutdown(context.TODO()))
		require.NoError(t, HTTPServer.Close())
	}
}

func (s *FakeServer) handle(w http.ResponseWriter, request *http.Request) {
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

// GetLastRequest returns last recorded request.
func (s *FakeServer) GetLastRequest() *prompb.WriteRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastWriteRequest
}

// GetLastHTTPRequest returns last recorded http request.
func (s *FakeServer) GetLastHTTPRequest() *http.Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastHTTPRequest
}

// HTTPAddr http address of a server.
func (s *FakeServer) HTTPAddr() string {
	return fmt.Sprintf("http://%s", s.addr)
}

// SetError sets error that will be returned for all incoming requests.
func (s *FakeServer) SetError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.respErr = err
}

// Reset resets state to default.
func (s *FakeServer) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.respErr = nil
	s.lastWriteRequest = nil
	s.lastHTTPRequest = nil
}
