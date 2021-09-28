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

package inprocess

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewCoordinator(t *testing.T) {
	coord, err := NewCoordinatorFromYAML(defaultCoordConfig, CoordinatorOptions{})
	require.NoError(t, err)
	require.NoError(t, coord.Close())
}

func TestCreateAnotherCoordinatorInProcess(t *testing.T) {
	coord, err := NewCoordinatorFromYAML(defaultCoordConfig, CoordinatorOptions{})
	require.NoError(t, err)
	require.NoError(t, coord.Close())
}

func TestNewEmbeddedCoordinator(t *testing.T) {
	dbnode, err := NewDBNodeFromYAML(embeddedCoordConfig, DBNodeOptions{})
	require.NoError(t, err)

	d, ok := dbnode.(*dbNode)
	require.True(t, ok)
	require.True(t, d.started)

	_, err = NewEmbeddedCoordinator(d)
	require.NoError(t, err)

	require.NoError(t, dbnode.Close())
}

func TestNewEmbeddedCoordinatorNotStarted(t *testing.T) {
	var dbnode dbNode
	_, err := NewEmbeddedCoordinator(&dbnode)
	require.Error(t, err)
}

// TODO(nate): add more tests exercising other endpoints once dbnode impl is landed

const defaultCoordConfig = `
clusters:
  - namespaces:
      - namespace: default
        type: unaggregated
        retention: 1h
    client:
      config:
        service:
          env: default_env
          zone: embedded
          service: m3db
          cacheDir: "*"
          etcdClusters:
            - zone: embedded
              endpoints:
                - 127.0.0.1:2379
`

const embeddedCoordConfig = `
coordinator:
  clusters:
    - namespaces:
        - namespace: default
          type: unaggregated
          retention: 1h
      client:
        config:
          service:
            env: default_env
            zone: embedded
            service: m3db
            cacheDir: "*"
            etcdClusters:
              - zone: embedded
                endpoints:
                  - 127.0.0.1:2379

db:
  filesystem:
    filePathPrefix: "*"
  writeNewSeriesAsync: false
`
