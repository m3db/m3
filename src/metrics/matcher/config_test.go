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

package matcher

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestConfigurationNewNamespaces(t *testing.T) {
	cfg := Configuration{
		InitWatchTimeout: time.Second,
		RulesKVConfig: kv.OverrideConfiguration{
			Namespace:   "RulesKVNamespace",
			Environment: "RulesKVEnvironment",
		},
		NamespacesKey:    "NamespacesKey",
		NamespaceTag:     "NamespaceTag",
		DefaultNamespace: "DefaultNamespace",
		NameTagKey:       "NameTagKey",
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := client.NewMockClient(ctrl)

	mem := mem.NewStore()
	m.EXPECT().Store(kv.NewOverrideOptions().SetEnvironment("RulesKVEnvironment").SetNamespace("RulesKVNamespace")).Return(mem, nil)

	opts, err := cfg.NewOptions(m, clock.NewOptions(), instrument.NewOptions())
	require.NoError(t, err)
	require.Equal(t, cfg.InitWatchTimeout, opts.InitWatchTimeout())
	require.Equal(t, cfg.NamespacesKey, opts.NamespacesKey())
}
