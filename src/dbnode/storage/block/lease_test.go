// Copyright (c) 2019 Uber Technologies, Inc.
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

package block

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestRegisterLeaser(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		leaser   = NewMockLeaser(ctrl)
		leaseMgr = NewLeaseManager(nil)
	)

	require.NoError(t, leaseMgr.RegisterLeaser(leaser))
	require.Equal(t, errLeaserAlreadyRegistered, leaseMgr.RegisterLeaser(leaser))
}

func TestUnregisterLeaser(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		leaser1  = NewMockLeaser(ctrl)
		leaser2  = NewMockLeaser(ctrl)
		leaseMgr = NewLeaseManager(nil)
	)

	// Cant unregister if not registered.
	require.Equal(t, errLeaserNotRegistered, leaseMgr.UnregisterLeaser(leaser1))
	require.Equal(t, errLeaserNotRegistered, leaseMgr.UnregisterLeaser(leaser2))

	// Register.
	require.NoError(t, leaseMgr.RegisterLeaser(leaser1))
	require.NoError(t, leaseMgr.RegisterLeaser(leaser2))

	// Ensure registered.
	require.Equal(t, errLeaserAlreadyRegistered, leaseMgr.RegisterLeaser(leaser1))
	require.Equal(t, errLeaserAlreadyRegistered, leaseMgr.RegisterLeaser(leaser2))

	// Ensure unregistering works.
	require.NoError(t, leaseMgr.UnregisterLeaser(leaser1))
	require.NoError(t, leaseMgr.RegisterLeaser(leaser1))

	// Ensure unregistering leaser1 does not unregister leaser2.
	require.Equal(t, errLeaserAlreadyRegistered, leaseMgr.RegisterLeaser(leaser2))
}

func TestOpenLease(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		leaser    = NewMockLeaser(ctrl)
		verifier  = NewMockLeaseVerifier(ctrl)
		leaseMgr  = NewLeaseManager(verifier)
		leaseDesc = LeaseDescriptor{
			Namespace:  ident.StringID("test-ns"),
			Shard:      1,
			BlockStart: xtime.Now().Truncate(2 * time.Hour),
		}
		leaseState = LeaseState{
			Volume: 1,
		}
	)
	verifier.EXPECT().VerifyLease(leaseDesc, leaseState)

	require.Equal(t, errLeaserNotRegistered, leaseMgr.OpenLease(leaser, leaseDesc, leaseState))
	require.NoError(t, leaseMgr.RegisterLeaser(leaser))
	require.NoError(t, leaseMgr.OpenLease(leaser, leaseDesc, leaseState))
}

func TestOpenLeaseErrorIfNoVerifier(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		leaser    = NewMockLeaser(ctrl)
		leaseMgr  = NewLeaseManager(nil)
		leaseDesc = LeaseDescriptor{
			Namespace:  ident.StringID("test-ns"),
			Shard:      1,
			BlockStart: xtime.Now().Truncate(2 * time.Hour),
		}
		leaseState = LeaseState{
			Volume: 1,
		}
	)

	require.NoError(t, leaseMgr.RegisterLeaser(leaser))
	require.Equal(t, errOpenLeaseVerifierNotSet, leaseMgr.OpenLease(leaser, leaseDesc, leaseState))

	verifier := NewMockLeaseVerifier(ctrl)
	verifier.EXPECT().VerifyLease(leaseDesc, leaseState)
	require.NoError(t, leaseMgr.SetLeaseVerifier(verifier))
	require.NoError(t, leaseMgr.OpenLease(leaser, leaseDesc, leaseState))
}

func TestOpenLatestLease(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		leaser    = NewMockLeaser(ctrl)
		verifier  = NewMockLeaseVerifier(ctrl)
		leaseMgr  = NewLeaseManager(verifier)
		leaseDesc = LeaseDescriptor{
			Namespace:  ident.StringID("test-ns"),
			Shard:      1,
			BlockStart: xtime.Now().Truncate(2 * time.Hour),
		}
		leaseState = LeaseState{
			Volume: 1,
		}
	)
	verifier.EXPECT().LatestState(leaseDesc).Return(leaseState, nil)

	require.Equal(t, errLeaserNotRegistered, leaseMgr.OpenLease(leaser, leaseDesc, leaseState))
	require.NoError(t, leaseMgr.RegisterLeaser(leaser))
	latestState, err := leaseMgr.OpenLatestLease(leaser, leaseDesc)
	require.NoError(t, err)
	require.Equal(t, leaseState, latestState)
}

func TestOpenLatestLeaseErrorIfNoVerifier(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		leaser    = NewMockLeaser(ctrl)
		leaseMgr  = NewLeaseManager(nil)
		leaseDesc = LeaseDescriptor{
			Namespace:  ident.StringID("test-ns"),
			Shard:      1,
			BlockStart: xtime.Now().Truncate(2 * time.Hour),
		}
		leaseState = LeaseState{
			Volume: 1,
		}
	)
	require.NoError(t, leaseMgr.RegisterLeaser(leaser))
	_, err := leaseMgr.OpenLatestLease(leaser, leaseDesc)
	require.Equal(t, errOpenLeaseVerifierNotSet, err)

	verifier := NewMockLeaseVerifier(ctrl)
	verifier.EXPECT().LatestState(leaseDesc).Return(leaseState, nil)
	require.NoError(t, leaseMgr.SetLeaseVerifier(verifier))
	latestState, err := leaseMgr.OpenLatestLease(leaser, leaseDesc)
	require.NoError(t, err)
	require.Equal(t, leaseState, latestState)
}

func TestUpdateOpenLeases(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		verifier = NewMockLeaseVerifier(ctrl)
		leaseMgr = NewLeaseManager(verifier)

		leaseDesc = LeaseDescriptor{
			Namespace:  ident.StringID("test-ns"),
			Shard:      1,
			BlockStart: xtime.Now().Truncate(2 * time.Hour),
		}
		leaseState = LeaseState{
			Volume: 1,
		}
		leasers = []*MockLeaser{NewMockLeaser(ctrl), NewMockLeaser(ctrl)}
	)
	verifier.EXPECT().VerifyLease(leaseDesc, leaseState).Times(len(leasers))

	// Expect that the leasers return NoOpenLease the first time to simulate the situation
	// where they don't have an open lease on the LeaseDescriptor that should be updated.
	leasers[0].EXPECT().
		UpdateOpenLease(leaseDesc, leaseState).
		Return(NoOpenLease, nil)
	leasers[1].EXPECT().
		UpdateOpenLease(leaseDesc, leaseState).
		Return(NoOpenLease, nil)

	// Expect that the leasers return UpdateOpenLease the second time to simulate the situation
	// where they do have an open lease on the LeaseDescriptor that should be updated.
	leasers[0].EXPECT().
		UpdateOpenLease(leaseDesc, leaseState).
		Return(UpdateOpenLease, nil)
	leasers[1].EXPECT().
		UpdateOpenLease(leaseDesc, leaseState).
		Return(UpdateOpenLease, nil)

	// Expect that the first leaser returns an error the third time to simulate the situation
	// where one of the leasers returns an error and make sure that UpdateOpenLeases() bails out
	// early and returns an error.
	leasers[0].EXPECT().
		UpdateOpenLease(leaseDesc, leaseState).
		Return(UpdateOpenLeaseResult(0), errors.New("some-error"))

	for _, leaser := range leasers {
		require.NoError(t, leaseMgr.RegisterLeaser(leaser))
	}

	// First time the leasers will return that they didn't have an open lease.
	result, err := leaseMgr.UpdateOpenLeases(leaseDesc, leaseState)
	require.NoError(t, err)
	require.Equal(t, result, UpdateLeasesResult{LeasersNoOpenLease: 2})

	for _, leaser := range leasers {
		err := leaseMgr.OpenLease(leaser, leaseDesc, leaseState)
		require.NoError(t, err)
	}

	// Second time the leasers will return that they did have an open lease.
	result, err = leaseMgr.UpdateOpenLeases(leaseDesc, leaseState)
	require.NoError(t, err)
	require.Equal(t, result, UpdateLeasesResult{LeasersUpdatedLease: 2})

	// Third time the first leaser will return an error.
	result, err = leaseMgr.UpdateOpenLeases(leaseDesc, leaseState)
	require.Error(t, err)
}

func TestUpdateOpenLeasesErrorIfNoVerifier(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		leaseMgr  = NewLeaseManager(nil)
		leaseDesc = LeaseDescriptor{
			Namespace:  ident.StringID("test-ns"),
			Shard:      1,
			BlockStart: xtime.Now().Truncate(2 * time.Hour),
		}
		leaseState = LeaseState{
			Volume: 1,
		}
		leasers = []*MockLeaser{NewMockLeaser(ctrl), NewMockLeaser(ctrl)}
	)

	for _, leaser := range leasers {
		require.NoError(t, leaseMgr.RegisterLeaser(leaser))
	}

	// First time the leasers will return that they didn't have an open lease.
	_, err := leaseMgr.UpdateOpenLeases(leaseDesc, leaseState)
	require.Equal(t, errUpdateOpenLeasesVerifierNotSet, err)
}

// TestUpdateOpenLeasesDoesNotDeadlockIfLeasersCallsBack verifies that a deadlock does
// not occur if a Leaser calls OpenLease or OpenLatestLease while the LeaseManager is
// waiting for a call to UpdateOpenLease() to complete on the same leaser.
func TestUpdateOpenLeasesDoesNotDeadlockIfLeasersCallsBack(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		leaser   = NewMockLeaser(ctrl)
		verifier = NewMockLeaseVerifier(ctrl)
		leaseMgr = NewLeaseManager(verifier)
	)
	verifier.EXPECT().VerifyLease(gomock.Any(), gomock.Any()).AnyTimes()
	verifier.EXPECT().LatestState(gomock.Any()).AnyTimes()
	leaser.EXPECT().UpdateOpenLease(gomock.Any(), gomock.Any()).Do(func(_ LeaseDescriptor, _ LeaseState) {
		require.NoError(t, leaseMgr.OpenLease(leaser, LeaseDescriptor{}, LeaseState{}))
		_, err := leaseMgr.OpenLatestLease(leaser, LeaseDescriptor{})
		require.NoError(t, err)
	})

	require.NoError(t, leaseMgr.RegisterLeaser(leaser))
	_, err := leaseMgr.UpdateOpenLeases(LeaseDescriptor{}, LeaseState{})
	require.NoError(t, err)
}

// TestUpdateOpenLeasesConcurrentNotAllowed verifies that concurrent calls to UpdateOpenLeases()
// with the same descriptor are not allowed which ensures that leasers receive all
// updates and in the correct order.
func TestUpdateOpenLeasesConcurrentNotAllowed(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		leaser   = NewMockLeaser(ctrl)
		verifier = NewMockLeaseVerifier(ctrl)
		leaseMgr = NewLeaseManager(verifier)
		wg       sync.WaitGroup

		descriptor LeaseDescriptor
	)

	wg.Add(1)
	leaser.EXPECT().UpdateOpenLease(descriptor, gomock.Any()).Do(func(_ LeaseDescriptor, _ LeaseState) {
		go func() {
			defer wg.Done()
			_, err := leaseMgr.UpdateOpenLeases(descriptor, LeaseState{})
			require.Equal(t, errConcurrentUpdateOpenLeases, err)
		}()
		wg.Wait()
	})

	require.NoError(t, leaseMgr.RegisterLeaser(leaser))
	_, err := leaseMgr.UpdateOpenLeases(descriptor, LeaseState{})
	require.NoError(t, err)
}

func TestUpdateOpenLeasesDifferentDescriptorsConcurrent(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		leaser   = NewMockLeaser(ctrl)
		verifier = NewMockLeaseVerifier(ctrl)
		leaseMgr = NewLeaseManager(verifier)
		wg       sync.WaitGroup

		blockStart  = xtime.Now().Truncate(time.Hour)
		descriptor1 = LeaseDescriptor{Namespace: ident.StringID("ns"), BlockStart: blockStart, Shard: 1}
		descriptor2 = LeaseDescriptor{Namespace: ident.StringID("ns"), BlockStart: blockStart, Shard: 2}
	)

	wg.Add(1)
	leaser.EXPECT().UpdateOpenLease(descriptor1, gomock.Any()).Do(func(descriptor LeaseDescriptor, _ LeaseState) {
		go func() {
			defer wg.Done()
			_, err := leaseMgr.UpdateOpenLeases(descriptor2, LeaseState{})
			require.NoError(t, err)
		}()
		wg.Wait()
	})
	leaser.EXPECT().UpdateOpenLease(descriptor2, gomock.Any())

	require.NoError(t, leaseMgr.RegisterLeaser(leaser))
	_, err := leaseMgr.UpdateOpenLeases(descriptor1, LeaseState{})
	require.NoError(t, err)
}

// TestUpdateOpenLeasesConcurrencyTest spins up a number of goroutines to call UpdateOpenLeases(),
// OpenLease(), and OpenLatestLease() concurrently and ensure there are no deadlocks.
func TestUpdateOpenLeasesConcurrencyTest(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		leaser     = NewMockLeaser(ctrl)
		verifier   = NewMockLeaseVerifier(ctrl)
		leaseMgr   = NewLeaseManager(verifier)
		wg         sync.WaitGroup
		doneCh     = make(chan struct{}, 1)
		numWorkers = 1
	)
	verifier.EXPECT().VerifyLease(gomock.Any(), gomock.Any()).AnyTimes()
	verifier.EXPECT().LatestState(gomock.Any()).AnyTimes()
	leaser.EXPECT().UpdateOpenLease(gomock.Any(), gomock.Any()).Do(func(_ LeaseDescriptor, _ LeaseState) {
		// Call back into the LeaseManager from the Leaser on each call to UpdateOpenLease to ensure there
		// are no deadlocks there.
		require.NoError(t, leaseMgr.OpenLease(leaser, LeaseDescriptor{}, LeaseState{}))
		_, err := leaseMgr.OpenLatestLease(leaser, LeaseDescriptor{})
		require.NoError(t, err)
	}).AnyTimes()
	require.NoError(t, leaseMgr.RegisterLeaser(leaser))

	// One goroutine calling UpdateOpenLeases in a loop.
	wg.Add(1)
	go func() {
		for {
			select {
			case <-doneCh:
				wg.Done()
				return
			default:
				_, err := leaseMgr.UpdateOpenLeases(LeaseDescriptor{}, LeaseState{})
				if err != nil {
					panic(err)
				}
			}
		}
	}()

	// Several goroutines calling OpenLease and OpenLatestLease.
	for i := 0; i < numWorkers; i++ {
		wg.Add(2)
		go func() {
			for {
				select {
				case <-doneCh:
					wg.Done()
					return
				default:
					if err := leaseMgr.OpenLease(leaser, LeaseDescriptor{}, LeaseState{}); err != nil {
						panic(err)
					}
				}
			}
		}()

		go func() {
			for {
				select {
				case <-doneCh:
					wg.Done()
					return
				default:
					if _, err := leaseMgr.OpenLatestLease(leaser, LeaseDescriptor{}); err != nil {
						panic(err)
					}
				}
			}
		}()
	}

	time.Sleep(100 * time.Millisecond)
	close(doneCh)
	wg.Wait()
}
