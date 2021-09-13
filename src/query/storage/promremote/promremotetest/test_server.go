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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPromServer is a fake http server handling prometheus remote write. Intended for test usage.
type TestPromServer struct {
	mu               sync.Mutex
	lastWriteRequest *prompb.WriteRequest
	lastHTTPRequest  *http.Request
	addr             string
	respErr          error
	t                *testing.T
}

// NewServer creates new instance of a fake server.
func NewServer(t *testing.T) (*TestPromServer, func()) {
	testPromServer := &TestPromServer{t: t}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	mux := http.NewServeMux()
	mux.HandleFunc("/write", testPromServer.handleWrite)

	svr := &http.Server{Handler: mux}
	go func() {
		if err := svr.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			//nolint: forbidigo
			fmt.Printf("unexpected testPromServer error %v \n", err)
		}
	}()
	testPromServer.addr = listener.Addr().String()
	return testPromServer, func() {
		require.NoError(t, svr.Shutdown(context.TODO()))
		require.NoError(t, svr.Close())
	}
}

func (s *TestPromServer) handleWrite(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastHTTPRequest = r
	assert.Equal(s.t, r.Header.Get("content-encoding"), "snappy")
	assert.Equal(s.t, r.Header.Get("content-type"), "application/x-protobuf")

	req, err := remote.DecodeWriteRequest(r.Body)
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
func (s *TestPromServer) GetLastRequest() *prompb.WriteRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastWriteRequest
}

// GetLastHTTPRequest returns last recorded http request.
func (s *TestPromServer) GetLastHTTPRequest() *http.Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastHTTPRequest
}

// WriteAddr http address of a write endpoint.
func (s *TestPromServer) WriteAddr() string {
	return fmt.Sprintf("http://%s/write", s.addr)
}

// SetError sets error that will be returned for all incoming requests.
func (s *TestPromServer) SetError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.respErr = err
}

// Reset resets state to default.
func (s *TestPromServer) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.respErr = nil
	s.lastWriteRequest = nil
	s.lastHTTPRequest = nil
}
