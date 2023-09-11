// Copyright (c) 2020  Uber Technologies, Inc.
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
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"
	xwatch "github.com/m3db/m3/src/x/watch"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestClusterNamespacesWatcherGet(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	watchable := xwatch.NewWatchable()
	var watcher ClusterNamespacesWatcher = &clusterNamespacesWatcher{watchable: watchable}
	defer watcher.Close()

	// Get from empty watch.
	namespaces := watcher.Get()
	require.Nil(t, namespaces)

	// Get when watch is set.
	namespaces = createClusterNamespaces(t, ctrl)
	err := watchable.Update(namespaces)
	require.NoError(t, err)

	require.Equal(t, namespaces, watcher.Get())
}

func TestClusterNamespacesWatcherUpdater(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	watcher := NewClusterNamespacesWatcher()
	defer watcher.Close()

	require.Nil(t, watcher.Get())

	namespaces := createClusterNamespaces(t, ctrl)
	err := watcher.Update(namespaces)
	require.NoError(t, err)

	require.Equal(t, namespaces, watcher.Get())
}

func TestClusterNamespacesWatcherRegisterListener(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	watcher := NewClusterNamespacesWatcher()
	defer watcher.Close()

	namespaces := createClusterNamespaces(t, ctrl)

	updateCh := make(chan bool)
	listener := &testListener{
		expected: namespaces,
		updateCh: updateCh,
		t:        t,
	}
	watcher.RegisterListener(listener)

	err := watcher.Update(namespaces)
	require.NoError(t, err)

	<-updateCh
}

func TestClusterNamespacesWatcherRegisterListenerNamespacesAlreadySet(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	watcher := NewClusterNamespacesWatcher()
	defer watcher.Close()

	namespaces := createClusterNamespaces(t, ctrl)

	err := watcher.Update(namespaces)
	require.NoError(t, err)

	updateCh := make(chan bool, 1)
	listener := &testListener{
		expected: namespaces,
		updateCh: updateCh,
		t:        t,
	}
	watcher.RegisterListener(listener)

	<-updateCh

	// Add a short sleep to ensure we don't receive a duplicate (and undesired) call
	time.Sleep(250 * time.Millisecond)

	require.Equal(t, 1, listener.callCount)
}

func createClusterNamespaces(t *testing.T, ctrl *gomock.Controller) ClusterNamespaces {
	session := client.NewMockSession(ctrl)
	namespace, err := newAggregatedClusterNamespace(AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("2s:1d"),
		Resolution:  2 * time.Second,
		Retention:   24 * time.Hour,
		Session:     session,
	})
	require.NoError(t, err)

	return ClusterNamespaces{namespace}
}

type testListener struct {
	updateCh  chan bool
	expected  ClusterNamespaces
	t         *testing.T
	callCount int
}

func (l *testListener) OnUpdate(namespaces ClusterNamespaces) {
	require.Equal(l.t, l.expected, namespaces)
	l.updateCh <- true
	l.callCount++
}
