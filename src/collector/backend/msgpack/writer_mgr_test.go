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

package msgpack

import (
	"errors"
	"testing"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"

	"github.com/stretchr/testify/require"
)

func TestWriterManagerAddInstancesClosed(t *testing.T) {
	mgr := newInstanceWriterManager(testServerOptions()).(*writerManager)
	mgr.closed = true
	require.Equal(t, errInstanceWriterManagerClosed, mgr.AddInstances(nil))
}

func TestWriterManagerAddInstancesSingleRef(t *testing.T) {
	mgr := newInstanceWriterManager(testServerOptions()).(*writerManager)

	// Add instance lists twice and assert the writer refcount matches expectation.
	for i := 0; i < 2; i++ {
		require.NoError(t, mgr.AddInstances([]services.PlacementInstance{testPlacementInstance}))
	}
	require.Equal(t, 1, len(mgr.writers))
	w, exists := mgr.writers[testPlacementInstance.ID()]
	require.True(t, exists)
	require.Equal(t, int32(2), w.refCount.n)
}

func TestWriterManagerRemoveInstancesClosed(t *testing.T) {
	mgr := newInstanceWriterManager(testServerOptions()).(*writerManager)
	mgr.closed = true
	require.Equal(t, errInstanceWriterManagerClosed, mgr.RemoveInstances(nil))
}

func TestWriterManagerRemoveInstancesSuccess(t *testing.T) {
	mgr := newInstanceWriterManager(testServerOptions()).(*writerManager)

	// Add instance lists twice.
	for i := 0; i < 2; i++ {
		require.NoError(t, mgr.AddInstances([]services.PlacementInstance{testPlacementInstance}))
	}
	require.Equal(t, 1, len(mgr.writers))

	// Remove the instance list once and assert they are not closed.
	require.NoError(t, mgr.RemoveInstances([]services.PlacementInstance{testPlacementInstance}))
	require.Equal(t, 1, len(mgr.writers))
	w := mgr.writers[testPlacementInstance.ID()].instanceWriter.(*writer)
	require.False(t, w.closed)

	// Remove the instance list again and assert the writer is now removed.
	nonexistent := placement.NewInstance().
		SetID("nonexistent").
		SetEndpoint("nonexistentAddress")
	toRemove := append([]services.PlacementInstance{nonexistent, testPlacementInstance})
	require.NoError(t, mgr.RemoveInstances(toRemove))
	require.Equal(t, 0, len(mgr.writers))
	require.True(t, w.closed)
}

func TestWriterManagerWriteToClosed(t *testing.T) {
	mgr := newInstanceWriterManager(testServerOptions()).(*writerManager)
	mgr.closed = true
	err := mgr.WriteTo([]services.PlacementInstance{testPlacementInstance}, 0, testCounter, testPoliciesList)
	require.Equal(t, errInstanceWriterManagerClosed, err)
}

func TestWriterManagerWriteToNoInstances(t *testing.T) {
	mgr := newInstanceWriterManager(testServerOptions()).(*writerManager)
	mgr.closed = false
	err := mgr.WriteTo([]services.PlacementInstance{testPlacementInstance}, 0, testCounter, testPoliciesList)
	require.Error(t, err)
}

func TestWriterManagerWriteToSuccess(t *testing.T) {
	var (
		instances = []services.PlacementInstance{
			testPlacementInstance,
			placement.NewInstance().
				SetID("foo").
				SetEndpoint("fooAddr"),
		}
		shardRes uint32
		muRes    unaggregated.MetricUnion
		plRes    policy.PoliciesList
	)
	mgr := newInstanceWriterManager(testServerOptions()).(*writerManager)
	mgr.writers[instances[0].ID()] = &refCountedWriter{
		refCount: refCount{n: 1},
		instanceWriter: &mockInstanceWriter{
			writeFn: func(shard uint32, mu unaggregated.MetricUnion, pl policy.PoliciesList) error {
				shardRes = shard
				muRes = mu
				plRes = pl
				return nil
			},
		},
	}
	mgr.writers[instances[1].ID()] = &refCountedWriter{
		refCount: refCount{n: 1},
		instanceWriter: &mockInstanceWriter{
			writeFn: func(shard uint32, mu unaggregated.MetricUnion, pl policy.PoliciesList) error {
				return errors.New("write error")
			},
		},
	}

	require.Error(t, mgr.WriteTo(instances, 0, testCounter, testPoliciesList))
	require.Equal(t, 2, len(mgr.writers))
	require.Equal(t, uint32(0), shardRes)
	require.Equal(t, testCounter, muRes)
	require.Equal(t, testPoliciesList, plRes)
}

func TestWriterManagerFlushClosed(t *testing.T) {
	mgr := newInstanceWriterManager(testServerOptions()).(*writerManager)
	mgr.closed = true
	require.Equal(t, errInstanceWriterManagerClosed, mgr.Flush())
}

func TestWriterManagerFlushPartialError(t *testing.T) {
	var (
		numFlushes int
		instances  = []services.PlacementInstance{
			testPlacementInstance,
			placement.NewInstance().
				SetID("foo").
				SetEndpoint("fooAddr"),
		}
	)

	mgr := newInstanceWriterManager(testServerOptions()).(*writerManager)
	mgr.writers[instances[0].ID()] = &refCountedWriter{
		refCount: refCount{n: 1},
		instanceWriter: &mockInstanceWriter{
			flushFn: func() error { numFlushes++; return nil },
		},
	}
	mgr.writers[instances[1].ID()] = &refCountedWriter{
		refCount: refCount{n: 1},
		instanceWriter: &mockInstanceWriter{
			flushFn: func() error { return errors.New("test flush error") },
		},
	}
	require.Error(t, mgr.Flush())
	require.Equal(t, 1, numFlushes)
}

func TestWriterManagerCloseAlreadyClosed(t *testing.T) {
	mgr := newInstanceWriterManager(testServerOptions()).(*writerManager)
	mgr.closed = true
	require.Equal(t, errInstanceWriterManagerClosed, mgr.Close())
}

func TestWriterManagerCloseSuccess(t *testing.T) {
	mgr := newInstanceWriterManager(testServerOptions()).(*writerManager)

	// Add instance list and close.
	require.NoError(t, mgr.AddInstances([]services.PlacementInstance{testPlacementInstance}))
	require.NoError(t, mgr.Close())
	require.True(t, mgr.closed)
	for _, w := range mgr.writers {
		require.True(t, w.instanceWriter.(*writer).closed)
	}
}

type mockWriteFn func(shard uint32, mu unaggregated.MetricUnion, pl policy.PoliciesList) error
type mockFlushFn func() error

type mockInstanceWriter struct {
	writeFn mockWriteFn
	flushFn mockFlushFn
}

func (mw *mockInstanceWriter) Write(
	shard uint32,
	mu unaggregated.MetricUnion,
	pl policy.PoliciesList,
) error {
	return mw.writeFn(shard, mu, pl)
}

func (mw *mockInstanceWriter) Flush() error { return mw.flushFn() }
func (mw *mockInstanceWriter) Close() error { return nil }
