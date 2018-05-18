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

package client

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/m3db/m3db/src/dbnode/topology"
	xconfig "github.com/m3db/m3x/config"
	"github.com/m3db/m3x/retry"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfiguration(t *testing.T) {
	in := `
writeConsistencyLevel: majority
readConsistencyLevel: unstrict_majority
connectConsistencyLevel: any
writeTimeout: 10s
fetchTimeout: 15s
connectTimeout: 20s
writeRetry:
    initialBackoff: 500ms
    backoffFactor: 3
    maxRetries: 2
    jitter: true
fetchRetry:
    initialBackoff: 500ms
    backoffFactor: 2
    maxRetries: 3
    jitter: true
backgroundHealthCheckFailLimit: 4
backgroundHealthCheckFailThrottleFactor: 0.5
hashing:
  seed: 42
`

	fd, err := ioutil.TempFile("", "config.yaml")
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, fd.Close())
		assert.NoError(t, os.Remove(fd.Name()))
	}()

	_, err = fd.Write([]byte(in))
	require.NoError(t, err)

	var cfg Configuration
	err = xconfig.LoadFile(&cfg, fd.Name(), xconfig.Options{})
	require.NoError(t, err)

	boolTrue := true
	expected := Configuration{
		WriteConsistencyLevel:   topology.ConsistencyLevelMajority,
		ReadConsistencyLevel:    topology.ReadConsistencyLevelUnstrictMajority,
		ConnectConsistencyLevel: topology.ConnectConsistencyLevelAny,
		WriteTimeout:            10 * time.Second,
		FetchTimeout:            15 * time.Second,
		ConnectTimeout:          20 * time.Second,
		WriteRetry: retry.Configuration{
			InitialBackoff: 500 * time.Millisecond,
			BackoffFactor:  3,
			MaxRetries:     2,
			Jitter:         &boolTrue,
		},
		FetchRetry: retry.Configuration{
			InitialBackoff: 500 * time.Millisecond,
			BackoffFactor:  2,
			MaxRetries:     3,
			Jitter:         &boolTrue,
		},
		BackgroundHealthCheckFailLimit:          4,
		BackgroundHealthCheckFailThrottleFactor: 0.5,
		HashingConfiguration: HashingConfiguration{
			Seed: 42,
		},
	}

	assert.Equal(t, expected, cfg)
}
