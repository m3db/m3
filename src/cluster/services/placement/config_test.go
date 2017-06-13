// Copyright (c) 2017 Uber Technologies, Inc.
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

package placement

import (
	"testing"
	"time"

	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3x/instrument"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestWatcherConfiguration(t *testing.T) {
	cfg := &WatcherConfiguration{
		Namespace:        "ns",
		Key:              "key",
		InitWatchTimeout: time.Second,
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := client.NewMockClient(ctrl)

	mem := mem.NewStore()
	m.EXPECT().Store(cfg.Namespace).Return(mem, nil)
	opts, err := cfg.NewOptions(m, instrument.NewOptions())
	require.NoError(t, err)

	require.Equal(t, cfg.Key, opts.StagedPlacementKey())
	require.Equal(t, cfg.InitWatchTimeout, opts.InitWatchTimeout())
	require.Equal(t, mem, opts.StagedPlacementStore())
}
