// +build integration_v2
// Copyright (c) 2021  Uber Technologies, Inc.
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

package docker

import (
	"context"
	"errors"
	"net/http"
	"path"
	"runtime"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v3"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPrometheus(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	_, filename, _, _ := runtime.Caller(0)
	prom := NewPrometheus(PrometheusOptions{
		Pool:      pool,
		PathToCfg: path.Join(path.Dir(filename), "config/prometheus.yml"),
	})
	require.NoError(t, prom.Setup())
	defer func() {
		assert.NoError(t, prom.Close())
	}()

	bo := backoff.NewConstantBackOff(1 * time.Second)
	var (
		attempts    = 0
		maxAttempts = 5
	)
	err = backoff.Retry(func() error {
		req, err := http.NewRequestWithContext(context.Background(), "GET", "http://0.0.0.0:9090/-/ready", nil)
		require.NoError(t, err)

		client := http.Client{}
		res, _ := client.Do(req)
		if res != nil && res.StatusCode == 200 {
			return nil
		}

		attempts++
		if attempts >= maxAttempts {
			return backoff.Permanent(errors.New("prometheus not ready"))
		}
		return errors.New("retryable error")
	}, bo)
	require.NoError(t, err)
}
