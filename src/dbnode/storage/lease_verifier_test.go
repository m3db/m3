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

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/x/ident"
	"github.com/stretchr/testify/require"
)

var (
	testBlockDescriptor = block.LeaseDescriptor{
		Namespace:  ident.StringID("test-ns"),
		Shard:      0,
		BlockStart: time.Now().Truncate(2 * time.Hour),
	}
	testBlockLeaseState = block.LeaseState{Volume: 0}
)

func TestLeaseVerifierHandlesErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		mockDB        = NewMockDatabase(ctrl)
		leaseVerifier = newLeaseVerifier(mockDB)
	)
	mockDB.EXPECT().FlushState(
		testBlockDescriptor.Namespace,
		uint32(testBlockDescriptor.Shard),
		testBlockDescriptor.BlockStart,
	).Return(fileOpState{}, errors.New("some-error"))

	err := leaseVerifier.VerifyLease(testBlockDescriptor, testBlockLeaseState)
	require.Error(t, err)
}

func TestLeaseVerifierReturnsErrorIfNotLatestVolume(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		mockDB        = NewMockDatabase(ctrl)
		leaseVerifier = newLeaseVerifier(mockDB)
	)
	mockDB.EXPECT().FlushState(
		testBlockDescriptor.Namespace,
		uint32(testBlockDescriptor.Shard),
		testBlockDescriptor.BlockStart,
	).Return(fileOpState{ColdVersion: testBlockLeaseState.Volume + 1}, nil)

	err := leaseVerifier.VerifyLease(testBlockDescriptor, testBlockLeaseState)
	require.Error(t, err)
}

func TestLeaseVerifierSuccessIfVolumeIsLatest(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		volumeNum     = 1
		state         = block.LeaseState{Volume: volumeNum}
		mockDB        = NewMockDatabase(ctrl)
		leaseVerifier = newLeaseVerifier(mockDB)
	)
	mockDB.EXPECT().FlushState(
		testBlockDescriptor.Namespace,
		uint32(testBlockDescriptor.Shard),
		testBlockDescriptor.BlockStart,
	).Return(fileOpState{ColdVersion: volumeNum}, nil)

	require.NoError(t, leaseVerifier.VerifyLease(testBlockDescriptor, state))
}
