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

package storage

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	xtest "github.com/m3db/m3/src/x/test"
)

func TestDatabaseBootstrapWithBootstrapError(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()

	opts := DefaultTestOptions()
	now := time.Now()
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return now
	}))

	id := ident.StringID("a")
	meta, err := namespace.NewMetadata(id, namespace.NewOptions())
	require.NoError(t, err)

	ns := NewMockdatabaseNamespace(ctrl)
	namespaces := []databaseNamespace{ns}

	db := NewMockdatabase(ctrl)
	db.EXPECT().OwnedNamespaces().Return(namespaces, nil)

	m := NewMockdatabaseMediator(ctrl)
	m.EXPECT().DisableFileOpsAndWait()
	m.EXPECT().EnableFileOps().AnyTimes()

	bsm := newBootstrapManager(db, m, opts).(*bootstrapManager)
	// Don't sleep.
	bsm.sleepFn = func(time.Duration) {}

	gomock.InOrder(
		ns.EXPECT().PrepareBootstrap(gomock.Any()).Return([]databaseShard{}, nil),
		ns.EXPECT().Metadata().Return(meta),
		ns.EXPECT().ID().Return(id),
		ns.EXPECT().
			Bootstrap(gomock.Any(), gomock.Any()).
			Return(fmt.Errorf("an error")).
			Do(func(ctx context.Context, bootstrapResult bootstrap.NamespaceResult) {
				// After returning an error, make sure we don't re-enqueue.
				require.Equal(t, Bootstrapping, bsm.state)
				bsm.bootstrapFn = func() error {
					require.Equal(t, Bootstrapping, bsm.state)
					return nil
				}
			}),
	)

	ctx := context.NewContext()
	defer ctx.Close()

	require.Equal(t, BootstrapNotStarted, bsm.state)

	result, err := bsm.Bootstrap()

	require.NoError(t, err)
	require.Equal(t, Bootstrapped, bsm.state)
	require.Equal(t, 1, len(result.ErrorsBootstrap))
	require.Equal(t, "an error", result.ErrorsBootstrap[0].Error())
}

func TestDatabaseBootstrapSubsequentCallsQueued(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()

	opts := DefaultTestOptions()
	now := time.Now()
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return now
	}))

	m := NewMockdatabaseMediator(ctrl)
	m.EXPECT().DisableFileOpsAndWait()
	m.EXPECT().EnableFileOps().AnyTimes()

	db := NewMockdatabase(ctrl)
	bsm := newBootstrapManager(db, m, opts).(*bootstrapManager)
	ns := NewMockdatabaseNamespace(ctrl)
	id := ident.StringID("testBootstrap")
	meta, err := namespace.NewMetadata(id, namespace.NewOptions())
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)

	ns.EXPECT().PrepareBootstrap(gomock.Any()).Return([]databaseShard{}, nil).AnyTimes()
	ns.EXPECT().Metadata().Return(meta).AnyTimes()

	ns.EXPECT().
		Bootstrap(gomock.Any(), gomock.Any()).
		Return(nil).
		Do(func(arg0, arg1 interface{}) {
			defer wg.Done()

			// Enqueue the second bootstrap
			_, err := bsm.Bootstrap()
			assert.Error(t, err)
			assert.Equal(t, errBootstrapEnqueued, err)
			assert.False(t, bsm.IsBootstrapped())
			bsm.RLock()
			assert.Equal(t, true, bsm.hasPending)
			bsm.RUnlock()

			// Expect the second bootstrap call
			ns.EXPECT().Bootstrap(gomock.Any(), gomock.Any()).Return(nil)
		})
	ns.EXPECT().
		ID().
		Return(id).
		Times(2)
	db.EXPECT().
		OwnedNamespaces().
		Return([]databaseNamespace{ns}, nil).
		Times(2)

	_, err = bsm.Bootstrap()
	require.Nil(t, err)
}

func TestDatabaseBootstrapBootstrapHooks(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()

	opts := DefaultTestOptions()
	now := time.Now()
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return now
	}))

	m := NewMockdatabaseMediator(ctrl)
	m.EXPECT().DisableFileOpsAndWait()
	m.EXPECT().EnableFileOps().AnyTimes()

	db := NewMockdatabase(ctrl)
	bsm := newBootstrapManager(db, m, opts).(*bootstrapManager)

	numNamespaces := 3
	namespaces := make([]databaseNamespace, 0, 3)
	for i := 0; i < numNamespaces; i++ {
		ns := NewMockdatabaseNamespace(ctrl)
		id := ident.StringID("testBootstrap")
		meta, err := namespace.NewMetadata(id, namespace.NewOptions())
		require.NoError(t, err)

		var wg sync.WaitGroup
		wg.Add(1)

		numShards := 8
		shards := make([]databaseShard, 0, numShards)
		for j := 0; j < numShards; j++ {
			shard := NewMockdatabaseShard(ctrl)
			shard.EXPECT().IsBootstrapped().Return(false)
			shard.EXPECT().IsBootstrapped().Return(true)
			shard.EXPECT().UpdateFlushStates().Times(2)
			shard.EXPECT().ID().Return(uint32(j)).AnyTimes()
			shards = append(shards, shard)
		}

		ns.EXPECT().PrepareBootstrap(gomock.Any()).Return(shards, nil).AnyTimes()
		ns.EXPECT().Metadata().Return(meta).AnyTimes()

		ns.EXPECT().
			Bootstrap(gomock.Any(), gomock.Any()).
			Return(nil).
			Do(func(arg0, arg1 interface{}) {
				defer wg.Done()

				// Enqueue the second bootstrap
				_, err := bsm.Bootstrap()
				assert.Error(t, err)
				assert.Equal(t, errBootstrapEnqueued, err)
				assert.False(t, bsm.IsBootstrapped())
				bsm.RLock()
				assert.Equal(t, true, bsm.hasPending)
				bsm.RUnlock()

				// Expect the second bootstrap call
				ns.EXPECT().Bootstrap(gomock.Any(), gomock.Any()).Return(nil)
			})
		ns.EXPECT().
			ID().
			Return(id).
			Times(2)
		namespaces = append(namespaces, ns)
	}
	db.EXPECT().
		OwnedNamespaces().
		Return(namespaces, nil).
		Times(2)

	_, err := bsm.Bootstrap()
	require.Nil(t, err)
}
