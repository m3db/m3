// Copyright (c) 2024 Uber Technologies, Inc.
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

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/x/ident"
)

func TestNoopLeaseManager(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("RegisterLeaser", func(t *testing.T) {
		manager := &NoopLeaseManager{}
		mockLeaser := NewMockLeaser(ctrl)
		err := manager.RegisterLeaser(mockLeaser)
		require.NoError(t, err)
	})

	t.Run("UnregisterLeaser", func(t *testing.T) {
		manager := &NoopLeaseManager{}
		mockLeaser := NewMockLeaser(ctrl)
		err := manager.UnregisterLeaser(mockLeaser)
		require.NoError(t, err)
	})

	t.Run("OpenLease", func(t *testing.T) {
		manager := &NoopLeaseManager{}
		mockLeaser := NewMockLeaser(ctrl)
		descriptor := LeaseDescriptor{
			Namespace:  ident.StringID("test"),
			Shard:      123,
			BlockStart: 456,
		}
		state := LeaseState{
			Volume: 1,
		}
		err := manager.OpenLease(mockLeaser, descriptor, state)
		require.NoError(t, err)
	})

	t.Run("OpenLatestLease", func(t *testing.T) {
		manager := &NoopLeaseManager{}
		mockLeaser := NewMockLeaser(ctrl)
		descriptor := LeaseDescriptor{
			Namespace:  ident.StringID("test"),
			Shard:      123,
			BlockStart: 456,
		}
		state, err := manager.OpenLatestLease(mockLeaser, descriptor)
		require.NoError(t, err)
		require.Equal(t, LeaseState{}, state)
	})

	t.Run("UpdateOpenLeases", func(t *testing.T) {
		manager := &NoopLeaseManager{}
		descriptor := LeaseDescriptor{
			Namespace:  ident.StringID("test"),
			Shard:      123,
			BlockStart: 456,
		}
		state := LeaseState{
			Volume: 1,
		}
		result, err := manager.UpdateOpenLeases(descriptor, state)
		require.NoError(t, err)
		require.Equal(t, UpdateLeasesResult{}, result)
	})

	t.Run("SetLeaseVerifier", func(t *testing.T) {
		manager := &NoopLeaseManager{}
		mockVerifier := NewMockLeaseVerifier(ctrl)
		err := manager.SetLeaseVerifier(mockVerifier)
		require.NoError(t, err)
	})

	t.Run("AllMethodsWithNilInputs", func(t *testing.T) {
		manager := &NoopLeaseManager{}

		// All methods should handle nil inputs without panicking
		require.NotPanics(t, func() {
			err := manager.RegisterLeaser(nil)
			require.NoError(t, err)

			err = manager.UnregisterLeaser(nil)
			require.NoError(t, err)

			err = manager.OpenLease(nil, LeaseDescriptor{}, LeaseState{})
			require.NoError(t, err)

			state, err := manager.OpenLatestLease(nil, LeaseDescriptor{})
			require.NoError(t, err)
			require.Equal(t, LeaseState{}, state)

			result, err := manager.UpdateOpenLeases(LeaseDescriptor{}, LeaseState{})
			require.NoError(t, err)
			require.Equal(t, UpdateLeasesResult{}, result)

			err = manager.SetLeaseVerifier(nil)
			require.NoError(t, err)
		})
	})

	t.Run("ConsistentBehavior", func(t *testing.T) {
		manager := &NoopLeaseManager{}
		mockLeaser := NewMockLeaser(ctrl)
		descriptor := LeaseDescriptor{
			Namespace:  ident.StringID("test"),
			Shard:      123,
			BlockStart: 456,
		}
		state := LeaseState{
			Volume: 1,
		}

		// Multiple calls should have consistent behavior
		for i := 0; i < 3; i++ {
			err := manager.RegisterLeaser(mockLeaser)
			require.NoError(t, err)

			err = manager.UnregisterLeaser(mockLeaser)
			require.NoError(t, err)

			err = manager.OpenLease(mockLeaser, descriptor, state)
			require.NoError(t, err)

			state, err := manager.OpenLatestLease(mockLeaser, descriptor)
			require.NoError(t, err)
			require.Equal(t, LeaseState{}, state)

			result, err := manager.UpdateOpenLeases(descriptor, state)
			require.NoError(t, err)
			require.Equal(t, UpdateLeasesResult{}, result)
		}
	})
}
