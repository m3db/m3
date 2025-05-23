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

package middleware

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/dbnode/generated/proto/circuitbreaker"
)

func TestNewEnableProvider(t *testing.T) {
	provider := NewEnableProvider()
	assert.NotNil(t, provider)
	assert.False(t, provider.IsEnabled())
	assert.False(t, provider.IsShadowMode())
}

func TestNewNopEnableProvider(t *testing.T) {
	provider := NewNopEnableProvider()
	assert.NotNil(t, provider)
	assert.False(t, provider.IsEnabled())
	assert.False(t, provider.IsShadowMode())
}

func TestEnableProvider_WatchConfig(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := kv.NewMockStore(ctrl)
	mockWatch := kv.NewMockValueWatch(ctrl)
	mockValue := kv.NewMockValue(ctrl)
	logger := zap.NewNop()

	// Test-scoped variables to control config state
	var expectedEnabled bool
	var expectedShadowMode bool

	// Create a channel to simulate watch updates
	watchChan := make(chan struct{})
	mockWatch.EXPECT().C().Return(watchChan).AnyTimes()

	// Set up mocks to always return the current expected config
	mockStore.EXPECT().Watch(_configPath).Return(mockWatch, nil).AnyTimes()
	mockStore.EXPECT().Get(_configPath).Return(mockValue, nil).AnyTimes()
	mockValue.EXPECT().Unmarshal(gomock.Any()).DoAndReturn(func(v interface{}) error {
		proto := v.(*circuitbreaker.EnableConfigProto)
		proto.Enabled = expectedEnabled
		proto.ShadowMode = expectedShadowMode
		return nil
	}).AnyTimes()

	provider := NewEnableProvider()
	err := provider.WatchConfig(mockStore, logger)
	require.NoError(t, err)

	// Initial state should be false/false
	assert.False(t, provider.IsEnabled())
	assert.False(t, provider.IsShadowMode())

	// Test config update
	expectedEnabled = true
	expectedShadowMode = true
	watchChan <- struct{}{}
	time.Sleep(100 * time.Millisecond)
	assert.True(t, provider.IsEnabled())
	assert.True(t, provider.IsShadowMode())

	// Test error handling in watch loop
	mockStore.EXPECT().Get(_configPath).Return(nil, assert.AnError).AnyTimes()
	watchChan <- struct{}{}
	time.Sleep(100 * time.Millisecond)
	// Restore normal behavior
	mockStore.EXPECT().Get(_configPath).Return(mockValue, nil).AnyTimes()

	// Test unmarshal error
	mockValue.EXPECT().Unmarshal(gomock.Any()).Return(assert.AnError).AnyTimes()
	watchChan <- struct{}{}
	time.Sleep(100 * time.Millisecond)
	// Restore normal behavior
	mockValue.EXPECT().Unmarshal(gomock.Any()).DoAndReturn(func(v interface{}) error {
		proto := v.(*circuitbreaker.EnableConfigProto)
		proto.Enabled = expectedEnabled
		proto.ShadowMode = expectedShadowMode
		return nil
	}).AnyTimes()

	// Test multiple config updates
	updates := []struct {
		enabled    bool
		shadowMode bool
	}{
		{true, true},
		{false, true},
		{true, false},
		{false, false},
	}

	for _, update := range updates {
		expectedEnabled = update.enabled
		expectedShadowMode = update.shadowMode
		watchChan <- struct{}{}
		for i := 0; i < 10; i++ {
			if provider.IsEnabled() == update.enabled && provider.IsShadowMode() == update.shadowMode {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		assert.Equal(t, update.enabled, provider.IsEnabled())
		assert.Equal(t, update.shadowMode, provider.IsShadowMode())
	}

	// Test channel close
	close(watchChan)
	time.Sleep(100 * time.Millisecond)
	// After channel close, state should remain at last set values
	assert.Equal(t, expectedEnabled, provider.IsEnabled())
	assert.Equal(t, expectedShadowMode, provider.IsShadowMode())
}

func TestEnableProvider_DefaultValues(t *testing.T) {
	provider := NewEnableProvider()
	assert.False(t, provider.IsEnabled())
	assert.False(t, provider.IsShadowMode())
}

func TestNopEnableProvider_AllMethods(t *testing.T) {
	provider := NewNopEnableProvider()
	assert.False(t, provider.IsEnabled())
	assert.False(t, provider.IsShadowMode())

	// Test WatchConfig returns nil
	err := provider.WatchConfig(nil, nil)
	assert.NoError(t, err)
}
