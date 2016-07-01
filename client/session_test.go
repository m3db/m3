// Copyright (c) 2016 Uber Technologies, Inc.
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
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/mocks"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/topology"

	"github.com/stretchr/testify/assert"
)

const (
	sessionTestReplicas = 3
	sessionTestShards   = 3
)

func newSessionTestOptions() m3db.ClientOptions {
	shardScheme, _ := sharding.NewShardScheme(0, sessionTestShards-1, func(id string) uint32 { return 0 })

	var hosts []m3db.Host
	for i := 0; i < sessionTestReplicas; i++ {
		hosts = append(hosts, topology.NewHost(fmt.Sprintf("testhost%d:9000", i)))
	}

	var hostShardSets []m3db.HostShardSet
	for _, host := range hosts {
		hostShardSets = append(hostShardSets, topology.NewHostShardSet(host, shardScheme.All()))
	}

	return NewOptions().TopologyType(topology.NewStaticTopologyType(
		topology.NewStaticTopologyTypeOptions().
			Replicas(sessionTestReplicas).
			ShardScheme(shardScheme).
			HostShardSets(hostShardSets)))
}

func TestSessionClusterConnectTimesOut(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestOptions()
	opts = opts.ClusterConnectTimeout(3 * clusterConnectWaitInterval)
	s, err := newSession(opts)
	assert.NoError(t, err)
	session := s.(*session)

	session.newHostQueueFn = func(
		host m3db.Host,
		writeBatchRequestPool writeBatchRequestPool,
		writeRequestArrayPool writeRequestArrayPool,
		opts m3db.ClientOptions,
	) hostQueue {
		hostQueue := mocks.NewMockhostQueue(ctrl)
		hostQueue.EXPECT().Open().Times(1)
		hostQueue.EXPECT().GetConnectionCount().Return(0).AnyTimes()
		hostQueue.EXPECT().Close().Times(1)
		return hostQueue
	}

	err = session.Open()
	assert.Error(t, err)
	assert.Equal(t, ErrClusterConnectTimeout, err)
}
