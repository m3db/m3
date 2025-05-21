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

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/dbnode/generated/proto/circuitbreaker"
)

type mockValue struct {
	config *circuitbreaker.EnableConfigProto
}

func (v *mockValue) Unmarshal(msg proto.Message) error {
	if v.config == nil {
		return kv.ErrNotFound
	}
	config := msg.(*circuitbreaker.EnableConfigProto)
	config.Enabled = v.config.Enabled
	config.ShadowMode = v.config.ShadowMode
	return nil
}

func (v *mockValue) Version() int {
	return 1
}

func (v *mockValue) IsNewer(other kv.Value) bool {
	return true
}

type mockWatch struct {
	ch chan struct{}
}

func (w *mockWatch) C() <-chan struct{} {
	return w.ch
}

func (w *mockWatch) Get() kv.Value {
	return &mockValue{
		config: &circuitbreaker.EnableConfigProto{
			Enabled:    true,
			ShadowMode: true,
		},
	}
}

func (w *mockWatch) Close() {}

type mockStore struct {
	watch *mockWatch
	err   error
}

func (s *mockStore) Get(key string) (kv.Value, error) {
	return &mockValue{
		config: &circuitbreaker.EnableConfigProto{
			Enabled:    true,
			ShadowMode: true,
		},
	}, nil
}

func (s *mockStore) Watch(key string) (kv.ValueWatch, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.watch, nil
}

func (s *mockStore) Set(key string, v proto.Message) (int, error) {
	return 0, nil
}

func (s *mockStore) SetIfNotExists(key string, v proto.Message) (int, error) {
	return 0, nil
}

func (s *mockStore) CheckAndSet(key string, version int, v proto.Message) (int, error) {
	return 0, nil
}

func (s *mockStore) Delete(key string) (kv.Value, error) {
	return nil, nil
}

func (s *mockStore) History(key string, from, to int) ([]kv.Value, error) {
	return nil, nil
}

func TestEnableProvider_WatchConfig(t *testing.T) {
	watch := &mockWatch{
		ch: make(chan struct{}),
	}
	store := &mockStore{
		watch: watch,
	}
	logger := zap.NewNop()

	provider := NewEnableProvider()
	err := provider.WatchConfig(store, logger)
	require.NoError(t, err)

	// Simulate a config update
	watch.ch <- struct{}{}

	// Give some time for the goroutine to process the update
	time.Sleep(100 * time.Millisecond)

	// Verify the config was updated
	assert.True(t, provider.IsEnabled())
	assert.True(t, provider.IsShadowMode())
}

func TestEnableProvider_WatchConfig_Error(t *testing.T) {
	store := &mockStore{
		watch: nil,
		err:   kv.ErrNotFound,
	}
	logger := zap.NewNop()

	provider := NewEnableProvider()
	err := provider.WatchConfig(store, logger)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to watch circuit breaker middleware configuration")
}

func TestNopEnableProvider(t *testing.T) {
	provider := NewNopEnableProvider()
	assert.False(t, provider.IsEnabled())
	assert.False(t, provider.IsShadowMode())

	// Test WatchConfig returns nil
	err := provider.WatchConfig(nil, nil)
	assert.NoError(t, err)
}
