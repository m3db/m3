// Copyright (c) 2022 Uber Technologies, Inc.
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

package remote

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// Test_staticResolver is a very basic test that staticResolver works. See TestRoundRobinClientRpc
// for a test of round-robin behavior.
func Test_staticResolver(t *testing.T) {
	// startGRPCServer starts a server on a random port. It handles cleanup as well.
	startGRPCServer := func(t *testing.T) net.Addr {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)

		// N.B.: server.Stop closes the listener too.

		server := grpc.NewServer()
		// No need to explicitly wait for server to be up; GRPC dial will do that for us (retry until up)
		go func() {
			require.NoError(t, server.Serve(lis))
		}()
		t.Cleanup(server.Stop)
		return lis.Addr()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	addr := startGRPCServer(t)
	_, err := grpc.DialContext(
		ctx,
		_staticResolverURL,
		grpc.WithResolvers(newStaticResolverBuilder([]string{addr.String()})),
		grpc.WithInsecure(),
		grpc.WithBlock(),
	)

	require.NoError(t, err)
}
