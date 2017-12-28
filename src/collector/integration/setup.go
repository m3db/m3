// Copyright (c) 2017 Uber Technologies, Inc.
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

package integration

import (
	"errors"
	"flag"
	"testing"

	"github.com/m3db/m3collector/backend"
	msgpackbackend "github.com/m3db/m3collector/backend/msgpack"
	msgpackserver "github.com/m3db/m3collector/integration/msgpack"
	"github.com/m3db/m3collector/reporter"
	"github.com/m3db/m3metrics/matcher"
	"github.com/m3db/m3metrics/matcher/cache"
	"github.com/m3db/m3x/server"

	"github.com/stretchr/testify/require"
)

var (
	serverAddrArg          = flag.String("serverAddr", "localhost:6789", "server address")
	errServerStartTimedOut = errors.New("server took too long to start")
	errServerStopTimedOut  = errors.New("server took too long to stop")
)

type testSetup struct {
	opts       testOptions
	backend    backend.Server
	reporter   reporter.Reporter
	serverAddr string
	server     server.Server
}

func newTestSetup(t *testing.T, opts testOptions) *testSetup {
	if opts == nil {
		opts = newTestOptions()
	}

	// Create reporter.
	cache := cache.NewCache(opts.CacheOptions())
	matcher, err := matcher.NewMatcher(cache, opts.MatcherOptions())
	require.NoError(t, err)
	backend := msgpackbackend.NewServer(opts.BackendOptions())
	reporter := reporter.NewReporter(matcher, backend, opts.ReporterOptions())

	// Create server.
	serverAddr := *serverAddrArg
	if addr := opts.ServerAddr(); addr != "" {
		serverAddr = addr
	}
	server := msgpackserver.NewServer(serverAddr, opts.ServerOptions())

	return &testSetup{
		opts:       opts,
		backend:    backend,
		reporter:   reporter,
		serverAddr: serverAddr,
		server:     server,
	}
}

func (ts *testSetup) Reporter() reporter.Reporter { return ts.reporter }

func (ts *testSetup) newClient() *client {
	return newClient(ts.serverAddr, ts.opts.ClientConnectTimeout())
}

func (ts *testSetup) waitUntilServerIsUp() error {
	c := ts.newClient()
	defer c.close()

	serverIsUp := func() bool { return c.testConnection() }
	if waitUntil(serverIsUp, ts.opts.ServerStateChangeTimeout()) {
		return nil
	}
	return errServerStartTimedOut
}

func (ts *testSetup) waitUntilServerIsDown() error {
	c := ts.newClient()
	defer c.close()

	serverIsDown := func() bool { return !c.testConnection() }
	if waitUntil(serverIsDown, ts.opts.ServerStateChangeTimeout()) {
		return nil
	}
	return errServerStopTimedOut
}

func (ts *testSetup) startServer() error {
	if err := ts.server.ListenAndServe(); err != nil {
		return err
	}
	if err := ts.waitUntilServerIsUp(); err != nil {
		return err
	}
	return ts.backend.Open()
}

func (ts *testSetup) stopServer() error {
	ts.server.Close()
	return ts.waitUntilServerIsDown()
}

func (ts *testSetup) close() {
	ts.reporter.Close()
	ts.server.Close()
}
