// Copyright (c) 2022 Uber Technologies, Inc.
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

package etcdintegration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testKey = "test-key"
)

func TestCluster(t *testing.T) {
	c := NewCluster(t, &ClusterConfig{Size: 1})

	defer c.Terminate(t)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	testVal := newTestValue(t)
	_, err := c.RandClient().Put(ctx, testKey, testVal)
	require.NoError(t, err)

	resp, err := c.RandClient().Get(ctx, testKey)
	require.NoError(t, err)
	assert.Equal(t, testVal, string(resp.Kvs[0].Value))
}

func TestCluster_withBridge(t *testing.T) {
	c := NewCluster(t, &ClusterConfig{Size: 1, UseBridge: true})

	defer c.Terminate(t)

	c.Members[0].Bridge().DropConnections()
	c.Members[0].Bridge().Blackhole()

	ctx := context.Background()

	blackholedCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	_, err := c.RandClient().MemberList(blackholedCtx)
	require.EqualError(t, err, context.DeadlineExceeded.Error())

	c.Members[0].Bridge().Unblackhole()

	availCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = c.RandClient().MemberList(availCtx)
	require.NoError(t, err)
}

func newTestValue(t *testing.T) string {
	return fmt.Sprintf("%s-%d", t.Name(), time.Now().Unix())
}
