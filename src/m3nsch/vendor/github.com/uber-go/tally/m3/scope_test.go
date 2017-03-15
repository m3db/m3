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

package m3

import (
	"sync"
	"testing"
	"time"

	"github.com/uber-go/tally"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var commonTags = map[string]string{"env": "test"}

type doneFn func()

func newTestReporterScope(
	t *testing.T,
	addr string,
	scopePrefix string,
	scopeTags map[string]string,
) (Reporter, tally.Scope, doneFn) {
	r, err := NewReporter(Options{
		HostPorts:          []string{addr},
		Service:            "testService",
		CommonTags:         commonTags,
		IncludeHost:        includeHost,
		MaxQueueSize:       queueSize,
		MaxPacketSizeBytes: maxPacketSize,
	})
	require.NoError(t, err)

	scope, closer := tally.NewCachedRootScope(
		scopePrefix, scopeTags, r, shortInterval,
		tally.DefaultSeparator)

	return r, scope, func() {
		assert.NoError(t, closer.Close())

		// Ensure reporter is closed too
		var open, readStatus bool
		select {
		case _, open = <-r.(*reporter).metCh:
			readStatus = true
		default:
		}
		assert.True(t, readStatus)
		assert.False(t, open)
	}
}

// TestScope tests that scope works as expected
func TestScope(t *testing.T) {
	var wg sync.WaitGroup
	server := newFakeM3Server(t, &wg, true, Compact)
	go server.Serve()
	defer server.Close()

	tags := map[string]string{"testTag": "TestValue", "testTag2": "TestValue2"}

	_, scope, close := newTestReporterScope(t, server.Addr, "honk", tags)
	defer close()

	wg.Add(1)

	timer := scope.Timer("dazzle")
	timer.Start().Stop()

	wg.Wait()

	require.Equal(t, 1, len(server.Service.getBatches()))
	require.NotNil(t, server.Service.getBatches()[0])

	emittedTimers := server.Service.getBatches()[0].GetMetrics()
	require.Equal(t, 1, len(emittedTimers))
	require.Equal(t, "honk.dazzle", emittedTimers[0].GetName())
}

// TestScopeCounter tests that scope works as expected
func TestScopeCounter(t *testing.T) {
	var wg sync.WaitGroup
	server := newFakeM3Server(t, &wg, true, Compact)
	go server.Serve()
	defer server.Close()

	tags := map[string]string{"testTag": "TestValue", "testTag2": "TestValue2"}

	_, scope, close := newTestReporterScope(t, server.Addr, "honk", tags)
	defer close()

	wg.Add(1)

	counter := scope.Counter("foobar")
	counter.Inc(42)

	wg.Wait()

	require.Equal(t, 1, len(server.Service.getBatches()))
	require.NotNil(t, server.Service.getBatches()[0])

	emittedTimers := server.Service.getBatches()[0].GetMetrics()
	require.Equal(t, 1, len(emittedTimers))
	require.Equal(t, "honk.foobar", emittedTimers[0].GetName())
}

// TestScopeGauge tests that scope works as expected
func TestScopeGauge(t *testing.T) {
	var wg sync.WaitGroup
	server := newFakeM3Server(t, &wg, true, Compact)
	go server.Serve()
	defer server.Close()

	tags := map[string]string{"testTag": "TestValue", "testTag2": "TestValue2"}

	_, scope, close := newTestReporterScope(t, server.Addr, "honk", tags)
	defer close()

	wg.Add(1)

	gauge := scope.Gauge("foobaz")
	gauge.Update(42)

	wg.Wait()

	require.Equal(t, 1, len(server.Service.getBatches()))
	require.NotNil(t, server.Service.getBatches()[0])

	emittedTimers := server.Service.getBatches()[0].GetMetrics()
	require.Equal(t, 1, len(emittedTimers))
	require.Equal(t, "honk.foobaz", emittedTimers[0].GetName())
}

func BenchmarkScopeReportTimer(b *testing.B) {
	backend, err := NewReporter(Options{
		HostPorts:          []string{"127.0.0.1:4444"},
		Service:            "my-service",
		MaxQueueSize:       10000,
		MaxPacketSizeBytes: maxPacketSize,
	})
	if err != nil {
		b.Error(err.Error())
		return
	}

	scope, closer := tally.NewCachedRootScope(
		"bench",
		nil,
		backend,
		1*time.Second,
		tally.DefaultSeparator,
	)

	perEndpointScope := scope.Tagged(
		map[string]string{
			"endpointid": "health",
			"handlerid":  "health",
		},
	)
	timer := perEndpointScope.Timer("inbound.latency")
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			timer.Record(500)
		}
	})

	b.StopTimer()
	closer.Close()
	b.StartTimer()
}
