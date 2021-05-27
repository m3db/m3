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
	"testing"
	"time"

	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"
	"github.com/stretchr/testify/assert"
)

func TestDatabaseIsBootstrappedAndDurable(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		validIsBootstrapped                  = true
		validShardSetAssignedAt              = xtime.ToUnixNano(time.Now())
		validLastBootstrapCompletionTime     = xtime.ToUnixNano(validShardSetAssignedAt.ToTime().Add(time.Second))
		validLastSuccessfulSnapshotStartTime = xtime.ToUnixNano(validLastBootstrapCompletionTime.ToTime().Add(time.Second))
		zeroTime                             xtime.UnixNano
	)
	testCases := []struct {
		title                           string
		isBootstrapped                  bool
		lastBootstrapCompletionTime     xtime.UnixNano
		lastSuccessfulSnapshotStartTime xtime.UnixNano
		shardSetAssignedAt              xtime.UnixNano
		expectedResult                  bool
	}{
		{
			title:                           "False is not bootstrapped",
			isBootstrapped:                  false,
			lastBootstrapCompletionTime:     validLastBootstrapCompletionTime,
			lastSuccessfulSnapshotStartTime: validLastSuccessfulSnapshotStartTime,
			shardSetAssignedAt:              validShardSetAssignedAt,
			expectedResult:                  false,
		},
		{
			title:                           "False if no last bootstrap completion time",
			isBootstrapped:                  validIsBootstrapped,
			lastBootstrapCompletionTime:     zeroTime,
			lastSuccessfulSnapshotStartTime: validLastSuccessfulSnapshotStartTime,
			shardSetAssignedAt:              validShardSetAssignedAt,
			expectedResult:                  false,
		},
		{
			title:                           "False if no last successful snapshot start time",
			isBootstrapped:                  validIsBootstrapped,
			lastBootstrapCompletionTime:     validLastBootstrapCompletionTime,
			lastSuccessfulSnapshotStartTime: zeroTime,
			shardSetAssignedAt:              validShardSetAssignedAt,
			expectedResult:                  false,
		},
		{
			title:                           "False if last snapshot start is not after last bootstrap completion time",
			isBootstrapped:                  validIsBootstrapped,
			lastBootstrapCompletionTime:     validLastBootstrapCompletionTime,
			lastSuccessfulSnapshotStartTime: validLastBootstrapCompletionTime,
			shardSetAssignedAt:              validShardSetAssignedAt,
			expectedResult:                  false,
		},
		{
			title:                           "False if last bootstrap completion time is not after shardset assigned at time",
			isBootstrapped:                  validIsBootstrapped,
			lastBootstrapCompletionTime:     validLastBootstrapCompletionTime,
			lastSuccessfulSnapshotStartTime: validLastBootstrapCompletionTime,
			shardSetAssignedAt:              validLastBootstrapCompletionTime,
			expectedResult:                  false,
		},
		{
			title:                           "False if last bootstrap completion time is not after/equal shardset assigned at time",
			isBootstrapped:                  validIsBootstrapped,
			lastBootstrapCompletionTime:     validLastBootstrapCompletionTime,
			lastSuccessfulSnapshotStartTime: validLastSuccessfulSnapshotStartTime,
			shardSetAssignedAt:              validLastBootstrapCompletionTime + xtime.UnixNano(xtime.Second),
			expectedResult:                  false,
		},
		{
			title:                           "True if all conditions are met",
			isBootstrapped:                  validIsBootstrapped,
			lastBootstrapCompletionTime:     validLastBootstrapCompletionTime,
			lastSuccessfulSnapshotStartTime: validLastSuccessfulSnapshotStartTime,
			shardSetAssignedAt:              validShardSetAssignedAt,
			expectedResult:                  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
			d, mapCh, _ := defaultTestDatabase(t, ctrl, Bootstrapped)
			defer func() {
				close(mapCh)
			}()

			mediator := NewMockdatabaseMediator(ctrl)
			d.mediator = mediator
			d.lastReceivedNewShards = tc.shardSetAssignedAt.ToTime()

			mediator.EXPECT().IsBootstrapped().Return(tc.isBootstrapped)
			if !tc.isBootstrapped {
				assert.Equal(t, tc.expectedResult, d.IsBootstrappedAndDurable())
				// Early return because other mock calls will not get called.
				return
			}

			if tc.lastBootstrapCompletionTime == 0 {
				mediator.EXPECT().LastBootstrapCompletionTime().Return(zeroTime, false)
				assert.Equal(t, tc.expectedResult, d.IsBootstrappedAndDurable())
				// Early return because other mock calls will not get called.
				return
			}

			mediator.EXPECT().LastBootstrapCompletionTime().Return(tc.lastBootstrapCompletionTime, true)

			if tc.lastSuccessfulSnapshotStartTime == 0 {
				mediator.EXPECT().LastSuccessfulSnapshotStartTime().Return(zeroTime, false)
				assert.Equal(t, tc.expectedResult, d.IsBootstrappedAndDurable())
				// Early return because other mock calls will not get called.
				return
			}

			mediator.EXPECT().LastSuccessfulSnapshotStartTime().Return(tc.lastSuccessfulSnapshotStartTime, true)

			assert.Equal(t, tc.expectedResult, d.IsBootstrappedAndDurable())
		})
	}
}
