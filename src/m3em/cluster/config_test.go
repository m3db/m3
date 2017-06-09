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

package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	"github.com/m3db/m3x/instrument"
	xretry "github.com/m3db/m3x/retry"
)

func TestConfigurationAssignment(t *testing.T) {
	var (
		sessionToken         = "abc"
		sessionOverride      = true
		replication          = 3
		numShards            = 8192
		nodeConcurrency      = 10
		nodeOperationTimeout = time.Minute
		retryConf            = &xretry.Configuration{
			BackoffFactor:  2.0,
			Forever:        true,
			InitialBackoff: time.Second,
			Jitter:         true,
			MaxBackoff:     time.Minute,
			MaxRetries:     3,
		}
	)

	noopScope := tally.NoopScope
	iopts := instrument.NewOptions().SetMetricsScope(noopScope)

	conf := &Configuration{
		SessionToken:            &sessionToken,
		SessionOverride:         &sessionOverride,
		Relication:              &replication,
		NumShards:               &numShards,
		NodeConcurrency:         &nodeConcurrency,
		NodeOperationTimeout:    &nodeOperationTimeout,
		PlacementServiceRetrier: retryConf,
	}

	opts := conf.Options(iopts)
	require.Equal(t, sessionToken, opts.SessionToken())
	require.Equal(t, sessionOverride, opts.SessionOverride())
	require.Equal(t, replication, opts.Replication())
	require.Equal(t, numShards, opts.NumShards())
	require.Equal(t, nodeConcurrency, opts.NodeConcurrency())
	require.Equal(t, nodeOperationTimeout, opts.NodeOperationTimeout())
}
