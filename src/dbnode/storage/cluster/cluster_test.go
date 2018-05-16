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

package cluster

import (
	"fmt"
	"testing"

	"github.com/m3db/m3db/src/dbnode/sharding"
	"github.com/m3db/m3db/src/dbnode/storage"
	"github.com/m3db/m3db/src/dbnode/topology"
	"github.com/m3db/m3db/src/dbnode/topology/testutil"
	xwatch "github.com/m3db/m3x/watch"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

type restoreFn func()

func mockNewStorageDatabase(
	ctrl *gomock.Controller,
) (*storage.MockDatabase, restoreFn) {
	var mock *storage.MockDatabase
	if ctrl != nil {
		mock = storage.NewMockDatabase(ctrl)
	}
	restore := setNewStorageDatabase(func(
		shardSet sharding.ShardSet,
		opts storage.Options,
	) (storage.Database, error) {
		if mock == nil {
			return nil, fmt.Errorf("no injected storage database")
		}
		return mock, nil
	})
	return mock, restore
}

func setNewStorageDatabase(fn newStorageDatabaseFn) restoreFn {
	prevNewStorageDatabase := newStorageDatabase
	newStorageDatabase = fn
	return func() {
		newStorageDatabase = prevNewStorageDatabase
	}
}

type mockTopoInitProperties struct {
	topology         *topology.MockDynamicTopology
	propogateViewsCh chan struct{}
}

func newMockTopoInit(
	t *testing.T,
	ctrl *gomock.Controller,
	viewsCh <-chan testutil.TopologyView,
) (
	*topology.MockInitializer,
	mockTopoInitProperties,
) {
	init := topology.NewMockInitializer(ctrl)

	watch := xwatch.NewWatchable()

	// Make the propagate views channel large so it never blocks
	propogateViewsCh := make(chan struct{}, 128)

	go func() {
		for {
			v, ok := <-viewsCh
			if !ok {
				break
			}

			m, err := v.Map()
			require.NoError(t, err)
			watch.Update(m)

			propogateViewsCh <- struct{}{}
		}
	}()

	_, w, err := watch.Watch()
	require.NoError(t, err)

	topo := topology.NewMockDynamicTopology(ctrl)
	topo.EXPECT().Watch().Return(topology.NewMapWatch(w), nil)

	init.EXPECT().Init().Return(topo, nil)

	return init, mockTopoInitProperties{
		topology:         topo,
		propogateViewsCh: propogateViewsCh,
	}
}
