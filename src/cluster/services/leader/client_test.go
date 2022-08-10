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

package leader

/*
import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/cluster/services/leader/campaign"
	"github.com/m3db/m3/src/cluster/services/leader/election"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/framework/integration"
	"golang.org/x/net/context"
)

var (
	newStatus = campaign.NewStatus
	newErr    = campaign.NewErrorStatus
	followerS = newStatus(campaign.Follower)
	leaderS   = newStatus(campaign.Leader)
)

func waitForStates(ch <-chan campaign.Status, early bool, states ...campaign.Status) error {
	var seen []campaign.Status
	for s := range ch {
		seen = append(seen, s)
		// terminate early (before channel closes)
		if early && reflect.DeepEqual(seen, states) {
			return nil
		}
	}

	if !reflect.DeepEqual(seen, states) {
		return fmt.Errorf("states did not match: %v != %v", seen, states)
	}

	return nil
}

type testCluster struct {
	t       *testing.T
	cluster *integration.Cluster
}

func newTestCluster(t *testing.T) *testCluster {
	integration.BeforeTestExternal(t)
	return &testCluster{
		t: t,
		cluster: integration.NewCluster(t, &integration.ClusterConfig{
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

func (tc *testCluster) options() Options {
	sid := services.NewServiceID().
		SetEnvironment("e1").
		SetName("s1").
		SetZone("z1")

	eopts := services.NewElectionOptions().
		SetTTLSecs(5)

	return NewOptions().
		SetServiceID(sid).
		SetElectionOpts(eopts)
}

func (tc *testCluster) client() *client {
	svc, err := newClient(tc.etcdClient(), tc.options(), "")
	require.NoError(tc.t, err)

	return svc
}

func (tc *testCluster) service() services.LeaderService {
	svc, err := NewService(tc.etcdClient(), tc.options())
	require.NoError(tc.t, err)

	return svc
}

func (tc *testCluster) opts(val string) services.CampaignOptions {
	opts, err := services.NewCampaignOptions()
	require.NoError(tc.t, err)
	return opts.SetLeaderValue(val)
}

func TestNewClient(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc, err := newClient(tc.etcdClient(), tc.options(), "")
	assert.NoError(t, err)
	assert.NotNil(t, svc)
}

func TestNewClient_BadCluster(t *testing.T) {
	tc := newTestCluster(t)
	cl := tc.etcdClient()
	tc.close()

	_, err := newClient(cl, tc.options(), "")
	assert.Error(t, err)
}

func TestCampaign(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.client()

	sc, err := svc.campaign(tc.opts("foo"))
	assert.NoError(t, err)

	waitForStates(sc, true, followerS, leaderS)

	_, err = svc.campaign(tc.opts("foo2"))
	assert.Equal(t, ErrCampaignInProgress, err)

	err = svc.resign()
	assert.NoError(t, err)

	errC := make(chan error)
	go func() {
		errC <- waitForStates(sc, false, followerS)
	}()

	err = <-errC
	assert.NoError(t, err)

	sc, err = svc.campaign(tc.opts("foo3"))
	assert.NoError(t, err)

	waitForStates(sc, true, followerS, leaderS)

	err = svc.resign()
	assert.NoError(t, err)
	assert.NoError(t, waitForStates(sc, false, followerS))
}

func TestCampaign_Override(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.client()

	sc, err := svc.campaign(tc.opts("foo"))
	assert.NoError(t, err)
	assert.NoError(t, waitForStates(sc, true, followerS, leaderS))

	ld, err := svc.leader()
	assert.NoError(t, err)
	assert.Equal(t, "foo", ld)
}

func TestCampaign_Renew(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.client()
	sc, err := svc.campaign(tc.opts(""))
	assert.NoError(t, err)
	assert.NoError(t, waitForStates(sc, true, followerS, leaderS))

	err = svc.resign()
	assert.NoError(t, err)
	assert.NoError(t, waitForStates(sc, false, followerS))

	_, err = svc.leader()
	assert.Equal(t, ErrNoLeader, err)

	sc2, err := svc.campaign(tc.opts(""))
	assert.NoError(t, err)
	assert.NoError(t, waitForStates(sc2, true, followerS, leaderS))
}

func TestResign(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.client()

	sc, err := svc.campaign(tc.opts("i1"))
	assert.NoError(t, err)

	assert.NoError(t, waitForStates(sc, true, followerS, leaderS))

	ld, err := svc.leader()
	assert.NoError(t, err)
	assert.Equal(t, "i1", ld)

	err = svc.resign()
	assert.NoError(t, err)

	assert.NoError(t, waitForStates(sc, false, followerS))

	ld, err = svc.leader()
	assert.Equal(t, ErrNoLeader, err)
	assert.Equal(t, "", ld)
}

func TestResign_BlockingCampaign(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc1, svc2 := tc.client(), tc.client()

	sc1, err := svc1.campaign(tc.opts("i1"))
	assert.NoError(t, err)
	assert.NoError(t, waitForStates(sc1, true, followerS, leaderS))

	sc2, err := svc2.campaign(tc.opts("i2"))
	assert.NoError(t, err)
	assert.NoError(t, waitForStates(sc2, true, followerS))

	err = svc2.resign()
	assert.NoError(t, err)
	assert.NoError(t, waitForStates(sc2, false, newErr(context.Canceled)))
}

func TestResign_Early(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.client()

	err := svc.resign()
	assert.NoError(t, err)
}

func TestObserve(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc1 := tc.client()

	obsC, err := svc1.observe()
	assert.NoError(t, err)

	sc1, err := svc1.campaign(tc.opts("i1"))
	assert.NoError(t, err)
	assert.NoError(t, waitForStates(sc1, true, followerS, leaderS))

	select {
	case <-time.After(time.Second):
		t.Error("expected to receive leader update")
	case v := <-obsC:
		assert.Equal(t, "i1", v)
	}

	assert.NoError(t, svc1.close())
	select {
	case <-time.After(5 * time.Second):
		t.Error("expected client channel to be closed")
	case _, ok := <-obsC:
		assert.False(t, ok)
	}

	_, err = svc1.observe()
	assert.Equal(t, errClientClosed, err)
}

func testHandoff(t *testing.T, resign bool) {
	tc := newTestCluster(t)
	defer tc.close()

	svc1, svc2 := tc.client(), tc.client()

	sc1, err := svc1.campaign(tc.opts("i1"))
	assert.NoError(t, err)

	assert.NoError(t, waitForStates(sc1, true, followerS, leaderS))

	sc2, err := svc2.campaign(tc.opts("i2"))
	assert.NoError(t, err)

	assert.NoError(t, waitForStates(sc2, true, followerS))

	ld, err := svc1.leader()
	assert.NoError(t, err)
	assert.Equal(t, ld, "i1")

	if resign {
		err = svc1.resign()
		assert.NoError(t, waitForStates(sc1, false, followerS))
	} else {
		err = svc1.close()
		assert.NoError(t, waitForStates(sc1, false, newErr(election.ErrSessionExpired)))
	}
	assert.NoError(t, err)

	assert.NoError(t, waitForStates(sc2, true, leaderS))

	ld, err = svc2.leader()
	assert.NoError(t, err)
	assert.Equal(t, ld, "i2")
}

func TestCampaign_Cancel_Resign(t *testing.T) {
	testHandoff(t, true)
}

func TestCampaign_Cancel_Close(t *testing.T) {
	testHandoff(t, false)
}

func TestCampaign_Close_NonLeader(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc1, svc2 := tc.client(), tc.client()

	sc1, err := svc1.campaign(tc.opts("i1"))
	assert.NoError(t, err)
	assert.NoError(t, waitForStates(sc1, true, followerS, leaderS))

	sc2, err := svc2.campaign(tc.opts("i2"))
	assert.NoError(t, err)
	assert.NoError(t, waitForStates(sc2, true, followerS))

	ld, err := svc1.leader()
	assert.NoError(t, err)
	assert.Equal(t, ld, "i1")

	err = svc2.close()
	assert.NoError(t, err)
	assert.NoError(t, waitForStates(sc2, false, newErr(context.Canceled)))

	err = svc1.resign()
	assert.NoError(t, err)
	assert.NoError(t, waitForStates(sc1, false, followerS))

	_, err = svc2.leader()
	assert.Equal(t, errClientClosed, err)
}

func TestClose(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc := tc.client()

	sc, err := svc.campaign(tc.opts("i1"))
	assert.NoError(t, err)
	assert.NoError(t, waitForStates(sc, true, followerS, leaderS))

	ld, err := svc.leader()
	assert.NoError(t, err)
	assert.Equal(t, "i1", ld)

	err = svc.close()
	assert.NoError(t, err)
	assert.True(t, svc.isClosed())
	assert.NoError(t, waitForStates(sc, false, newErr(election.ErrSessionExpired)))

	err = svc.resign()
	assert.Equal(t, errClientClosed, err)

	_, err = svc.campaign(tc.opts(""))
	assert.Equal(t, errClientClosed, err)
}

func TestLeader(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.close()

	svc1, svc2 := tc.client(), tc.client()
	sc, err := svc1.campaign(tc.opts("i1"))
	assert.NoError(t, err)
	assert.NoError(t, waitForStates(sc, true, followerS, leaderS))

	ld, err := svc2.leader()
	assert.NoError(t, err)

	assert.Equal(t, "i1", ld)
}

func TestElectionPrefix(t *testing.T) {
	for args, exp := range map[*struct {
		env, name, eid string
	}]string{
		{"", "svc", ""}:       "_ld/svc/default",
		{"env", "svc", ""}:    "_ld/env/svc/default",
		{"", "svc", "foo"}:    "_ld/svc/foo",
		{"env", "svc", "foo"}: "_ld/env/svc/foo",
	} {
		sid := services.NewServiceID().
			SetEnvironment(args.env).
			SetName(args.name)

		pfx := electionPrefix(sid, args.eid)

		assert.Equal(t, exp, pfx)
	}
}
*/
