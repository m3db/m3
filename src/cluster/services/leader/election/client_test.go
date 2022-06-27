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

package election

import (
	"context"
	"testing"
	"time"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/integration"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testCluster struct {
	t       *testing.T
	cluster *integration.ClusterV3
}

func newTestCluster(t *testing.T) *testCluster {
	return &testCluster{
		t: t,
		cluster: integration.NewClusterV3(t, &integration.ClusterConfig{
			Size: 1,
		}),
	}
}

func (tc *testCluster) close() {
	tc.cluster.Terminate(tc.t)
}

func (tc *testCluster) etcdClient() *clientv3.Client {
	return tc.cluster.RandClient()
}

func (tc *testCluster) client(prefix string, options ...ClientOption) *Client {
	options = append([]ClientOption{WithSessionOptions(concurrency.WithTTL(5))}, options...)
	cl, err := NewClient(tc.etcdClient(), prefix, options...)
	require.NoError(tc.t, err)

	return cl
}

func TestNewClient(t *testing.T) {
	tc := newTestCluster(t)

	cl, err := NewClient(tc.etcdClient(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, cl)

	tc.close()

	_, err = NewClient(tc.etcdClient(), "foo")
	assert.Error(t, err)
}

func TestCampaign(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	cl := tc.client("")
	_, err := cl.Campaign(context.Background(), "foo")
	assert.NoError(t, err)

	ld, err := cl.Leader(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "foo", ld)
}

func TestCampaign_Multi(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	cl1 := tc.client("foo")
	_, err := cl1.Campaign(context.Background(), "1")
	assert.NoError(t, err)

	ld, _ := cl1.Leader(context.Background())
	assert.Equal(t, "1", ld)

	cl2 := tc.client("foo")
	ch := make(chan struct{})

	go func() {
		_, err := cl2.Campaign(context.Background(), "2")
		assert.NoError(t, err)
		close(ch)
	}()

	err = cl1.Resign(context.Background())
	assert.NoError(t, err)

	<-ch
	ld, _ = cl1.Leader(context.Background())
	assert.Equal(t, "2", ld)

	ld, _ = cl2.Leader(context.Background())
	assert.Equal(t, "2", ld)
}

func TestCampaign_DeadSession(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	cl1 := tc.client("")
	cl2 := tc.client("")

	_, err := cl1.Campaign(context.Background(), "1")
	assert.NoError(t, err)

	errC := make(chan error)
	go func() {
		_, err := cl2.Campaign(context.Background(), "2")
		errC <- err
	}()

	time.Sleep(time.Second)

	cl2.session.Close()

	select {
	case err := <-errC:
		assert.Equal(t, context.Canceled, err)
	case <-time.After(30 * time.Second):
		t.Error("should receive err from client2 after orphaning session")
	}

	// Expect that even though cl2 has a dead session it can still lookup leader
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ld, err := cl2.Leader(ctx)
	assert.Equal(t, "1", ld)
	assert.NoError(t, err)
}

func TestCampaign_DeadSession_Background(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	cl := tc.client("")

	ch, err := cl.Campaign(context.Background(), "1")
	assert.NoError(t, err)

	cl.session.Close()

	sent := false
	select {
	case <-ch:
		sent = true
	case <-time.After(10 * time.Second):
	}

	assert.True(t, sent, "should have received session dead signal")

	// new campaign should reset session
	_, err = cl.Campaign(context.Background(), "1")
	assert.NoError(t, err)

	ld, err := cl.Leader(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "1", ld)
}

func TestResign(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	cl := tc.client("")
	_, err := cl.Campaign(context.Background(), "foo")
	assert.NoError(t, err)

	err = cl.Resign(context.Background())
	assert.NoError(t, err)
}

func TestCampaign_ResignActive(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	cl1 := tc.client("foo")
	_, err := cl1.Campaign(context.Background(), "1")
	assert.NoError(t, err)

	ld, _ := cl1.Leader(context.Background())
	assert.Equal(t, "1", ld)

	cl2 := tc.client("foo")
	ch := make(chan struct{})
	ch2 := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		close(ch2)
		_, err := cl2.Campaign(ctx, "2")
		assert.Equal(t, context.Canceled, err)
		close(ch)
	}()

	<-ch2
	time.Sleep(200 * time.Millisecond)
	cancel()
	err = cl2.Resign(context.Background())
	assert.NoError(t, err)

	<-ch
	ld, _ = cl1.Leader(context.Background())
	assert.Equal(t, "1", ld)

	ld, _ = cl2.Leader(context.Background())
	assert.Equal(t, "1", ld)
}

func TestObserve(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	cl1 := tc.client("a")

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	obsC, err := cl1.Observe(ctx1)
	assert.NoError(t, err)

	bufC := make(chan string, 100)
	go func() {
		for v := range obsC {
			bufC <- v
		}
	}()

	_, err = cl1.Campaign(context.Background(), "1")
	assert.NoError(t, err)

	select {
	case <-time.After(time.Second):
		t.Error("expected to receive leader update within 1s")
	case v := <-bufC:
		assert.Equal(t, "1", v)
	}

	cl2 := tc.client("a")

	el2 := make(chan struct{})
	go func() {
		_, err := cl2.Campaign(context.Background(), "2")
		assert.NoError(t, err)
		close(el2)
	}()

	err = cl1.Resign(context.Background())
	assert.NoError(t, err)

	select {
	case <-time.After(5 * time.Second):
		t.Error("expected to be leader within 5s")
	case <-el2:
	}

	select {
	case <-time.After(time.Second):
		t.Error("expected to receive new leader within 1s")
	case v := <-bufC:
		assert.Equal(t, "2", v)
	}

	cancel1()
	select {
	case <-time.After(time.Second):
		t.Error("expected update channel to be closed within 1s")
	case _, ok := <-obsC:
		assert.False(t, ok)
	}
}

func TestClose(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	cl := tc.client("")

	ch, err := cl.Campaign(context.Background(), "1")
	assert.NoError(t, err)

	ld, err := cl.Leader(context.Background())
	assert.Equal(t, "1", ld)
	assert.NoError(t, err)

	err = cl.Close()
	assert.NoError(t, err)

	select {
	case <-ch:
	case <-time.After(30 * time.Second):
		t.Error("should have received close on campaign ch")
	}

	_, err = cl.Campaign(context.Background(), "foo")
	assert.Equal(t, ErrClientClosed, err)

	err = cl.Resign(context.Background())
	assert.Equal(t, ErrClientClosed, err)

	_, err = cl.Leader(context.Background())
	assert.Equal(t, ErrClientClosed, err)
}
