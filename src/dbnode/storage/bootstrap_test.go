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
	"github.com/m3db/m3/src/x/ident"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatabaseBootstrapWithBootstrapError(t *testing.T) {
	ctrl := gomock.NewController(t)
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
	db.EXPECT().GetOwnedNamespaces().Return(namespaces, nil)

	m := NewMockdatabaseMediator(ctrl)
	m.EXPECT().DisableFileOps()
	m.EXPECT().EnableFileOps().AnyTimes()

	bsm := newBootstrapManager(db, m, opts).(*bootstrapManager)
	// Don't sleep.
	bsm.sleepFn = func(time.Duration) {}

	gomock.InOrder(
		ns.EXPECT().GetOwnedShards().Return([]databaseShard{}),
		ns.EXPECT().Metadata().Return(meta),
		ns.EXPECT().ID().Return(id),
		ns.EXPECT().
			Bootstrap(gomock.Any()).
			Return(fmt.Errorf("an error")).
			Do(func(bootstrapResult bootstrap.NamespaceResult) {
				// After returning an error, make sure we don't re-enqueue.
				bsm.bootstrapFn = func() error {
					return nil
				}
			}),
	)

	result, err := bsm.Bootstrap()
	require.NoError(t, err)

	require.Equal(t, 1, len(result.ErrorsBootstrap))
	require.Equal(t, "an error", result.ErrorsBootstrap[0].Error())
}

func TestDatabaseBootstrapSubsequentCallsQueued(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := DefaultTestOptions()
	now := time.Now()
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return now
	}))

	m := NewMockdatabaseMediator(ctrl)
	m.EXPECT().DisableFileOps()
	m.EXPECT().EnableFileOps().AnyTimes()

	db := NewMockdatabase(ctrl)
	bsm := newBootstrapManager(db, m, opts).(*bootstrapManager)
	ns := NewMockdatabaseNamespace(ctrl)
	id := ident.StringID("testBootstrap")
	meta, err := namespace.NewMetadata(id, namespace.NewOptions())
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)

	ns.EXPECT().GetOwnedShards().Return([]databaseShard{}).AnyTimes()
	ns.EXPECT().Metadata().Return(meta).AnyTimes()

	ns.EXPECT().
		Bootstrap(gomock.Any()).
		Return(nil).
		Do(func(arg0 interface{}) {
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
			ns.EXPECT().Bootstrap(gomock.Any()).Return(nil)
		})
	ns.EXPECT().
		ID().
		Return(id).
		Times(2)
	db.EXPECT().
		GetOwnedNamespaces().
		Return([]databaseNamespace{ns}, nil).
		Times(2)

	_, err = bsm.Bootstrap()
	require.Nil(t, err)
}
