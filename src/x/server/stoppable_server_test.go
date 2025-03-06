package server

import (
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	xtls "github.com/m3db/m3/src/x/tls"
)

func testStoppableTCPServer(addr string) (*server, *mockHandler, *int32, *int32) {
	return testServer(addr, xtls.Disabled, false)
}

func TestStoppableServerListenStopAndClose(t *testing.T) {
	s, h, numAdded, numRemoved := testStoppableTCPServer(testListenAddress)

	var (
		numClients  = 9
		expectedRes []string
	)

	err := s.ListenAndServe()
	require.NoError(t, err)
	listenAddr := s.listener.Addr().String()

	for i := 0; i < numClients; i++ {
		conn, err := net.Dial("tcp", listenAddr)
		require.NoError(t, err)

		msg := fmt.Sprintf("msg%d", i)
		expectedRes = append(expectedRes, msg)

		_, err = conn.Write([]byte(msg))
		require.NoError(t, err)
	}

	for h.called() < numClients {
		time.Sleep(100 * time.Millisecond)
	}
	waitFor(func() bool { return h.called() == numClients })

	require.False(t, h.isClosed())

	s.Stop()
	_, err = net.Dial("tcp", listenAddr)
	require.True(
		t,
		errors.Is(err, syscall.ECONNREFUSED),
		"connection attempt on Stop()ed server should result in ECONNREFUSED, got %v instead",
		err,
	)

	// Server should still handle the already existing active connections.
	require.False(t, h.isClosed())

	s.Close()

	require.True(t, h.isClosed())
	require.Equal(t, int32(numClients), atomic.LoadInt32(numAdded))
	require.Equal(t, int32(numClients), atomic.LoadInt32(numRemoved))
	require.Equal(t, numClients, h.called())
	require.Equal(t, expectedRes, h.res())
}
