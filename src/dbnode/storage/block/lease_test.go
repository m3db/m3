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
		leaser   = NewMockLeaser(ctrl)
		leaseMgr = NewLeaseManager(nil)
	)

	require.Equal(t, errLeaserNotRegistered, leaseMgr.UnregisterLeaser(leaser))
	require.NoError(t, leaseMgr.RegisterLeaser(leaser))
	require.Equal(t, errLeaserAlreadyRegistered, leaseMgr.RegisterLeaser(leaser))
	require.NoError(t, leaseMgr.UnregisterLeaser(leaser))
	require.NoError(t, leaseMgr.RegisterLeaser(leaser))
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
	leasers[0].EXPECT().UpdateOpenLease(leaseDesc, leaseState).Return(NoOpenLease, nil)
	leasers[1].EXPECT().UpdateOpenLease(leaseDesc, leaseState).Return(NoOpenLease, nil)

	// Expect that the leasers return UpdateOpenLease the second time to simulate the situation
	// where they do have an open lease on the LeaseDescriptor that should be updated.
	leasers[0].EXPECT().UpdateOpenLease(leaseDesc, leaseState).Return(UpdateOpenLease, nil)
	leasers[1].EXPECT().UpdateOpenLease(leaseDesc, leaseState).Return(UpdateOpenLease, nil)

	for _, leaser := range leasers {
		require.NoError(t, leaseMgr.RegisterLeaser(leaser))
	}

	result, err := leaseMgr.UpdateOpenLeases(leaseDesc, leaseState)
	require.NoError(t, err)
	require.Equal(t, result, UpdateLeasesResult{LeasersNoOpenLease: 2})

	for _, leaser := range leasers {
		leaseMgr.OpenLease(leaseDesc, leaseState)
	}

	result, err := leaseMgr.UpdateOpenLeases(leaseDesc, leaseState)
	require.NoError(t, err)
	require.Equal(t, result, UpdateLeasesResult{LeasersUpdatedLease: 2})
}
