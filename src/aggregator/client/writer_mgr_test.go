// Copyright (c) 2018 Uber Technologies, Inc.
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

package client

import (
	"errors"
	"strings"
	"testing"

	"github.com/m3db/m3/src/cluster/placement"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var (
	testPlacementInstance = placement.NewInstance().
		SetID("testInstanceID").
		SetEndpoint("testInstanceAddress")
)

func TestWriterManagerAddInstancesClosed(t *testing.T) {
	mgr := newInstanceWriterManager(testOptions()).(*writerManager)
	mgr.closed = true
	require.Equal(t, errInstanceWriterManagerClosed, mgr.AddInstances(nil))
}

func TestWriterManagerAddInstancesSingleRef(t *testing.T) {
	mgr := newInstanceWriterManager(testOptions()).(*writerManager)

	// Add instance lists twice and assert the writer refcount matches expectation.
	for i := 0; i < 2; i++ {
		require.NoError(t, mgr.AddInstances([]placement.Instance{testPlacementInstance}))
	}
	require.Equal(t, 1, len(mgr.writers))
	w, exists := mgr.writers[testPlacementInstance.ID()]
	require.True(t, exists)
	require.Equal(t, int32(2), w.refCount.n)
}

func TestWriterManagerRemoveInstancesClosed(t *testing.T) {
	mgr := newInstanceWriterManager(testOptions()).(*writerManager)
	mgr.closed = true
	require.Equal(t, errInstanceWriterManagerClosed, mgr.RemoveInstances(nil))
}

func TestWriterManagerRemoveInstancesSuccess(t *testing.T) {
	mgr := newInstanceWriterManager(testOptions()).(*writerManager)

	// Add instance lists twice.
	for i := 0; i < 2; i++ {
		require.NoError(t, mgr.AddInstances([]placement.Instance{testPlacementInstance}))
	}
	require.Equal(t, 1, len(mgr.writers))

	// Remove the instance list once and assert they are not closed.
	require.NoError(t, mgr.RemoveInstances([]placement.Instance{testPlacementInstance}))
	require.Equal(t, 1, len(mgr.writers))
	w := mgr.writers[testPlacementInstance.ID()].instanceWriter.(*writer)
	require.False(t, w.closed)

	// Remove the instance list again and assert the writer is now removed.
	nonexistent := placement.NewInstance().
		SetID("nonexistent").
		SetEndpoint("nonexistentAddress")
	toRemove := append([]placement.Instance{nonexistent, testPlacementInstance})
	require.NoError(t, mgr.RemoveInstances(toRemove))
	require.Equal(t, 0, len(mgr.writers))
	require.True(t, w.closed)
}

func TestWriterManagerWriteUntimedClosed(t *testing.T) {
	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    testCounter,
			metadatas: testStagedMetadatas,
		},
	}
	mgr := newInstanceWriterManager(testOptions()).(*writerManager)
	mgr.closed = true
	err := mgr.Write(testPlacementInstance, 0, payload)
	require.Equal(t, errInstanceWriterManagerClosed, err)
}

func TestWriterManagerWriteUntimedNoInstances(t *testing.T) {
	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    testCounter,
			metadatas: testStagedMetadatas,
		},
	}
	mgr := newInstanceWriterManager(testOptions()).(*writerManager)
	mgr.closed = false
	err := mgr.Write(testPlacementInstance, 0, payload)
	require.Error(t, err)
}

func TestWriterManagerWriteUntimedSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		instances = []placement.Instance{
			testPlacementInstance,
			placement.NewInstance().
				SetID("foo").
				SetEndpoint("fooAddr"),
		}
		shardRes   uint32
		payloadRes payloadUnion
	)
	writer := NewMockinstanceWriter(ctrl)
	writer.EXPECT().
		Write(gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			shard uint32,
			payload payloadUnion,
		) error {
			shardRes = shard
			payloadRes = payload
			return nil
		})
	mgr := newInstanceWriterManager(testOptions()).(*writerManager)
	mgr.writers[instances[0].ID()] = &refCountedWriter{
		refCount:       refCount{n: 1},
		instanceWriter: writer,
	}

	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    testCounter,
			metadatas: testStagedMetadatas,
		},
	}
	require.NoError(t, mgr.Write(testPlacementInstance, 0, payload))
	require.Equal(t, 1, len(mgr.writers))
	require.Equal(t, uint32(0), shardRes)
	require.Equal(t, untimedType, payloadRes.payloadType)
	require.Equal(t, testCounter, payloadRes.untimed.metric)
	require.Equal(t, testStagedMetadatas, payloadRes.untimed.metadatas)
}

func TestWriterManagerFlushClosed(t *testing.T) {
	mgr := newInstanceWriterManager(testOptions()).(*writerManager)
	mgr.closed = true
	require.Equal(t, errInstanceWriterManagerClosed, mgr.Flush())
}

func TestWriterManagerFlushPartialError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		numFlushes int
		instances  = []placement.Instance{
			testPlacementInstance,
			placement.NewInstance().
				SetID("foo").
				SetEndpoint("fooAddr"),
		}
	)

	writer1 := NewMockinstanceWriter(ctrl)
	writer1.EXPECT().
		Flush().
		DoAndReturn(func() error {
			numFlushes++
			return nil
		})
	errTestFlush := errors.New("test flush error")
	writer2 := NewMockinstanceWriter(ctrl)
	writer2.EXPECT().
		Flush().
		DoAndReturn(func() error {
			return errTestFlush
		})
	mgr := newInstanceWriterManager(testOptions()).(*writerManager)
	mgr.writers[instances[0].ID()] = &refCountedWriter{
		refCount:       refCount{n: 1},
		instanceWriter: writer1,
	}
	mgr.writers[instances[1].ID()] = &refCountedWriter{
		refCount:       refCount{n: 1},
		instanceWriter: writer2,
	}
	err := mgr.Flush()
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), errTestFlush.Error()))
	require.Equal(t, 1, numFlushes)
}

func TestWriterManagerCloseAlreadyClosed(t *testing.T) {
	mgr := newInstanceWriterManager(testOptions()).(*writerManager)
	mgr.closed = true
	require.Equal(t, errInstanceWriterManagerClosed, mgr.Close())
}

func TestWriterManagerCloseSuccess(t *testing.T) {
	mgr := newInstanceWriterManager(testOptions()).(*writerManager)

	// Add instance list and close.
	require.NoError(t, mgr.AddInstances([]placement.Instance{testPlacementInstance}))
	require.NoError(t, mgr.Close())
	require.True(t, mgr.closed)
	for _, w := range mgr.writers {
		require.True(t, w.instanceWriter.(*writer).closed)
	}
}
