// Copyright (c) 2020 Uber Technologies, Inc.
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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/dbnode/persist"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"

	"github.com/stretchr/testify/require"
)

func TestColdFlushManagerFlushAlreadyInProgress(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		mockPersistManager = persist.NewMockManager(ctrl)
		mockFlushPersist   = persist.NewMockFlushPreparer(ctrl)

		// Channels used to coordinate cold flushing
		startCh = make(chan struct{}, 1)
		doneCh  = make(chan struct{}, 1)
	)
	defer func() {
		close(startCh)
		close(doneCh)
	}()

	mockFlushPersist.EXPECT().DoneFlush().Return(nil)
	mockPersistManager.EXPECT().StartFlushPersist().Do(func() {
		startCh <- struct{}{}
		<-doneCh
	}).Return(mockFlushPersist, nil)

	nsOpts := namespaceOptions
	testOpts := DefaultTestOptions().SetPersistManager(mockPersistManager)
	db := newMockdatabase(ctrl)
	db.EXPECT().Options().Return(testOpts).AnyTimes()
	db.EXPECT().IsBootstrapped().Return(true).AnyTimes()
	ns := NewMockdatabaseNamespace(ctrl)
	ns.EXPECT().Options().Return(nsOpts).AnyTimes()
	ns.EXPECT().ID().Return(ident.StringID("ns1")).AnyTimes()
	shard := NewMockdatabaseShard(ctrl)
	shard.EXPECT().ID().Return(uint32(0)).AnyTimes()
	shard.EXPECT().IsBootstrapped().Return(true).AnyTimes()
	shard.EXPECT().FlushState(gomock.Any()).Return(fileOpState{}, nil).AnyTimes()
	shard.EXPECT().CleanupExpiredFileSets(gomock.Any()).Return(nil).AnyTimes()
	shard.EXPECT().CleanupCompactedFileSets().Return(nil).AnyTimes()
	ns.EXPECT().OwnedShards().Return([]databaseShard{shard}).AnyTimes()
	ns.EXPECT().ColdFlush([]databaseShard{shard}, mockFlushPersist).Return(nil).AnyTimes()
	db.EXPECT().OwnedNamespaces().Return([]databaseNamespace{ns}, nil).AnyTimes()

	cfm := newColdFlushManager(db, mockPersistManager, testOpts).(*coldFlushManager)
	cfm.pm = mockPersistManager

	var (
		wg  sync.WaitGroup
		now = time.Unix(0, 0)
	)
	wg.Add(2)

	// Goroutine 1 should successfully flush.
	go func() {
		defer wg.Done()
		require.True(t, cfm.Run(now))
	}()

	// Goroutine 2 should indicate already flushing.
	go func() {
		defer wg.Done()

		// Wait until we start the cold flushing process.
		<-startCh

		// Ensure it doesn't allow a parallel flush.
		require.False(t, cfm.Run(now))

		// Allow the cold flush to finish.
		doneCh <- struct{}{}
	}()

	wg.Wait()

}

func TestColdFlushManagerFlushDoneFlushError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		fakeErr            = errors.New("fake error while marking flush done")
		mockPersistManager = persist.NewMockManager(ctrl)
		mockFlushPersist   = persist.NewMockFlushPreparer(ctrl)
	)

	mockFlushPersist.EXPECT().DoneFlush().Return(fakeErr)
	mockPersistManager.EXPECT().StartFlushPersist().Return(mockFlushPersist, nil)

	testOpts := DefaultTestOptions().SetPersistManager(mockPersistManager)
	db := newMockdatabase(ctrl)
	db.EXPECT().Options().Return(testOpts).AnyTimes()

	cfm := newColdFlushManager(db, mockPersistManager, testOpts).(*coldFlushManager)
	cfm.pm = mockPersistManager

	flushNamespaces := newFlushableNamespaces([]databaseNamespace{})
	require.EqualError(t, fakeErr, cfm.coldFlush(flushNamespaces).Error())
}

func TestColdFlushManagerIgnoreRetryableError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		retryableError     = xerrors.NewRetryableError(errors.New("fake error while marking flush done"))
		mockPersistManager = persist.NewMockManager(ctrl)
		mockFlushPersist   = persist.NewMockFlushPreparer(ctrl)
	)

	mockFlushPersist.EXPECT().DoneFlush().Return(retryableError)
	mockPersistManager.EXPECT().StartFlushPersist().Return(mockFlushPersist, nil)

	testOpts := DefaultTestOptions().SetPersistManager(mockPersistManager)

	shard := NewMockdatabaseShard(ctrl)
	shard.EXPECT().ID().Return(uint32(0)).AnyTimes()
	shard.EXPECT().IsBootstrapped().Return(false).AnyTimes()

	ns := NewMockdatabaseNamespace(ctrl)
	ns.EXPECT().ID().Return(ident.StringID("ns1")).AnyTimes()
	ns.EXPECT().OwnedShards().Return([]databaseShard{shard}).AnyTimes()

	nses := []databaseNamespace{ns}

	db := newMockdatabase(ctrl)
	db.EXPECT().Options().Return(testOpts).AnyTimes()
	db.EXPECT().OwnedNamespaces().Return(nses, nil).AnyTimes()

	cfm := newColdFlushManager(db, mockPersistManager, testOpts).(*coldFlushManager)
	cfm.pm = mockPersistManager

	require.True(t, cfm.Run(time.Now()))
}
