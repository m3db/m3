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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/x/ident"
	"github.com/stretchr/testify/require"
)

func TestRegisterLeaser(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		leaser   = NewMockLeaser(ctrl)
		leaseMgr = NewLeaseManager(nil)
	)

	require.NoError(t, leaseMgr.RegisterLeaser(leaser))
	require.Equal(t, errLeaserAlreadyRegistered, leaseMgr.RegisterLeaser(leaser))
}

func TestUnregisterLeaser(t *testing.T) {
	ctrl := gomock.NewController(t)
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		leaser    = NewMockLeaser(ctrl)
		verifier  = NewMockLeaseVerifier(ctrl)
		leaseMgr  = NewLeaseManager(verifier)
		leaseDesc = LeaseDescriptor{
			Namespace:  ident.StringID("test-ns"),
			Shard:      1,
			BlockStart: time.Now().Truncate(2 * time.Hour),
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		leaser    = NewMockLeaser(ctrl)
		leaseMgr  = NewLeaseManager(nil)
		leaseDesc = LeaseDescriptor{
			Namespace:  ident.StringID("test-ns"),
			Shard:      1,
			BlockStart: time.Now().Truncate(2 * time.Hour),
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

func TestOpenLeaseForLatest(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		leaser    = NewMockLeaser(ctrl)
		verifier  = NewMockLeaseVerifier(ctrl)
		leaseMgr  = NewLeaseManager(verifier)
		leaseDesc = LeaseDescriptor{
			Namespace:  ident.StringID("test-ns"),
			Shard:      1,
			BlockStart: time.Now().Truncate(2 * time.Hour),
		}
		leaseState = LeaseState{
			Volume: 1,
		}
	)
	verifier.EXPECT().LatestState(leaseDesc).Return(leaseState, nil)

	require.Equal(t, errLeaserNotRegistered, leaseMgr.OpenLease(leaser, leaseDesc, leaseState))
	require.NoError(t, leaseMgr.RegisterLeaser(leaser))
	latestState, err := leaseMgr.OpenLeaseForLatest(leaser, leaseDesc)
	require.NoError(t, err)
	require.Equal(t, leaseState, latestState)
}

func TestOpenLeaseForLatestErrorIfNoVerifier(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		leaser    = NewMockLeaser(ctrl)
		leaseMgr  = NewLeaseManager(nil)
		leaseDesc = LeaseDescriptor{
			Namespace:  ident.StringID("test-ns"),
			Shard:      1,
			BlockStart: time.Now().Truncate(2 * time.Hour),
		}
		leaseState = LeaseState{
			Volume: 1,
		}
	)
	require.NoError(t, leaseMgr.RegisterLeaser(leaser))
	_, err := leaseMgr.OpenLeaseForLatest(leaser, leaseDesc)
	require.Equal(t, errOpenLeaseVerifierNotSet, err)

	verifier := NewMockLeaseVerifier(ctrl)
	verifier.EXPECT().LatestState(leaseDesc).Return(leaseState, nil)
	require.NoError(t, leaseMgr.SetLeaseVerifier(verifier))
	latestState, err := leaseMgr.OpenLeaseForLatest(leaser, leaseDesc)
	require.NoError(t, err)
	require.Equal(t, leaseState, latestState)
}

func TestUpdateOpenLeases(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		verifier = NewMockLeaseVerifier(ctrl)
		leaseMgr = NewLeaseManager(verifier)

		leaseDesc = LeaseDescriptor{
			Namespace:  ident.StringID("test-ns"),
			Shard:      1,
			BlockStart: time.Now().Truncate(2 * time.Hour),
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
		leaseMgr.OpenLease(leaser, leaseDesc, leaseState)
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		leaseMgr  = NewLeaseManager(nil)
		leaseDesc = LeaseDescriptor{
			Namespace:  ident.StringID("test-ns"),
			Shard:      1,
			BlockStart: time.Now().Truncate(2 * time.Hour),
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
