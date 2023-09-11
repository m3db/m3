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

package storage

import (
	"errors"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

var (
	testBlockDescriptor = block.LeaseDescriptor{
		Namespace:  ident.StringID("test-ns"),
		Shard:      0,
		BlockStart: xtime.Now().Truncate(2 * time.Hour),
	}
	testBlockLeaseState = block.LeaseState{Volume: 0}
)

func TestLeaseVerifierVerifyLeaseHandlesErrors(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		mockDB        = NewMockDatabase(ctrl)
		leaseVerifier = NewLeaseVerifier(mockDB)
	)
	mockDB.EXPECT().FlushState(
		testBlockDescriptor.Namespace,
		uint32(testBlockDescriptor.Shard),
		testBlockDescriptor.BlockStart,
	).Return(fileOpState{}, errors.New("some-error"))

	err := leaseVerifier.VerifyLease(testBlockDescriptor, testBlockLeaseState)
	require.Error(t, err)
}

func TestLeaseVerifierVerifyLeaseReturnsErrorIfNotLatestVolume(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		mockDB        = NewMockDatabase(ctrl)
		leaseVerifier = NewLeaseVerifier(mockDB)
	)
	mockDB.EXPECT().FlushState(
		testBlockDescriptor.Namespace,
		uint32(testBlockDescriptor.Shard),
		testBlockDescriptor.BlockStart,
	).Return(fileOpState{ColdVersionFlushed: testBlockLeaseState.Volume + 1}, nil)

	err := leaseVerifier.VerifyLease(testBlockDescriptor, testBlockLeaseState)
	require.Error(t, err)
}

func TestLeaseVerifierVerifyLeaseSuccessIfVolumeIsLatest(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		volumeNum     = 1
		state         = block.LeaseState{Volume: volumeNum}
		mockDB        = NewMockDatabase(ctrl)
		leaseVerifier = NewLeaseVerifier(mockDB)
	)
	mockDB.EXPECT().FlushState(
		testBlockDescriptor.Namespace,
		uint32(testBlockDescriptor.Shard),
		testBlockDescriptor.BlockStart,
	).Return(fileOpState{ColdVersionFlushed: volumeNum}, nil)

	require.NoError(t, leaseVerifier.VerifyLease(testBlockDescriptor, state))
}

func TestLeaseVerifierLatestStateHandlesErrors(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		mockDB        = NewMockDatabase(ctrl)
		leaseVerifier = NewLeaseVerifier(mockDB)
	)
	mockDB.EXPECT().FlushState(
		testBlockDescriptor.Namespace,
		uint32(testBlockDescriptor.Shard),
		testBlockDescriptor.BlockStart,
	).Return(fileOpState{}, errors.New("some-error"))

	_, err := leaseVerifier.LatestState(testBlockDescriptor)
	require.Error(t, err)
}

func TestLeaseVerifierLatestStateSuccess(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		mockDB        = NewMockDatabase(ctrl)
		leaseVerifier = NewLeaseVerifier(mockDB)
	)
	mockDB.EXPECT().FlushState(
		testBlockDescriptor.Namespace,
		uint32(testBlockDescriptor.Shard),
		testBlockDescriptor.BlockStart,
	).Return(fileOpState{ColdVersionFlushed: 1}, nil)

	state, err := leaseVerifier.LatestState(testBlockDescriptor)
	require.NoError(t, err)
	require.Equal(t, block.LeaseState{Volume: 1}, state)
}
