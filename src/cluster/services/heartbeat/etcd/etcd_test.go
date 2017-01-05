// Copyright (c) 2016 Uber Technologies, Inc.
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

package etcd

import (
	"math/rand"
	"testing"
	"time"

	"github.com/coreos/etcd/integration"
	"github.com/m3db/m3cluster/services/heartbeat"
	"github.com/stretchr/testify/require"
)

func TestKeys(t *testing.T) {
	s := "service"
	id := "instance"

	require.Equal(t, "_hb/service/instance", heartbeatKey(s, id))
	require.Equal(t, "_hb/service", servicePrefix(s))
	require.Equal(t, "instance", instanceID(heartbeatKey(s, id), s))
}

func TestStore(t *testing.T) {
	store, closeFn := testStore(t)
	defer closeFn()

	err := store.Heartbeat("s", "i1", 1*time.Second)
	require.NoError(t, err)
	err = store.Heartbeat("s", "i2", 2*time.Second)
	require.NoError(t, err)

	ids, err := store.Get("s")
	require.NoError(t, err)
	require.Equal(t, 2, len(ids))
	require.Contains(t, ids, "i1")
	require.Contains(t, ids, "i2")

	for {
		ids, err = store.Get("s")
		require.NoError(t, err)
		if len(ids) == 1 {
			break
		}
	}
	require.Equal(t, 1, len(ids))
	require.NotContains(t, ids, "i1")
	require.Contains(t, ids, "i2")

	for {
		ids, err = store.Get("s")
		require.NoError(t, err)
		if len(ids) == 0 {
			break
		}
	}
	require.Equal(t, 0, len(ids))
	require.NotContains(t, ids, "i1")
	require.NotContains(t, ids, "i2")
}

func testStore(t *testing.T) (heartbeat.Store, func()) {
	ecluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 3})
	ec := ecluster.Client(rand.Intn(3))

	closer := func() {
		ecluster.Terminate(t)
		ec.Watcher.Close()
	}

	return NewStore(ec), closer
}
