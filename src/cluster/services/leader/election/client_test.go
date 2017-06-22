package election

import (
	"context"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/integration"
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

	cl, err = NewClient(tc.etcdClient(), "foo")
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
