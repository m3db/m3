// Copyright (c) 2021 Uber Technologies, Inc.
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
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/topology"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber/tchannel-go/thrift"
)

func TestSessionOpts(t *testing.T) {
	clientOpts := client.NewOptions().
		SetIterationOptions(index.IterationOptions{
			IterateEqualTimestampStrategy: encoding.IterateLowestValue,
		}).
		SetReadConsistencyLevel(topology.ReadConsistencyLevelMajority)
	rcAll := rpc.ReadConsistency_ALL
	csHighest := rpc.EqualTimestampStrategy_HIGHEST_VALUE
	cases := []struct {
		name     string
		reqOpts  *rpc.ClusterQueryOptions
		sessOpts sessionOpts
	}{
		{
			name: "nil opts",
			sessOpts: sessionOpts{
				readConsistency:        topology.ReadConsistencyLevelMajority,
				equalTimestampStrategy: encoding.IterateLowestValue,
			},
		},
		{
			name:    "not set",
			reqOpts: &rpc.ClusterQueryOptions{},
			sessOpts: sessionOpts{
				readConsistency:        topology.ReadConsistencyLevelMajority,
				equalTimestampStrategy: encoding.IterateLowestValue,
			},
		},
		{
			name: "only read consistency",
			reqOpts: &rpc.ClusterQueryOptions{
				ReadConsistency: &rcAll,
			},
			sessOpts: sessionOpts{
				readConsistency:        topology.ReadConsistencyLevelAll,
				equalTimestampStrategy: encoding.IterateLowestValue,
				consistencyOverride:    true,
			},
		},
		{
			name: "only conflict strategy",
			reqOpts: &rpc.ClusterQueryOptions{
				ConflictResolutionStrategy: &csHighest,
			},
			sessOpts: sessionOpts{
				readConsistency:        topology.ReadConsistencyLevelMajority,
				equalTimestampStrategy: encoding.IterateHighestValue,
			},
		},
		{
			name: "both set",
			reqOpts: &rpc.ClusterQueryOptions{
				ReadConsistency:            &rcAll,
				ConflictResolutionStrategy: &csHighest,
			},
			sessOpts: sessionOpts{
				readConsistency:        topology.ReadConsistencyLevelAll,
				equalTimestampStrategy: encoding.IterateHighestValue,
				consistencyOverride:    true,
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctrl := xtest.NewController(t)
			defer ctrl.Finish()

			c := client.NewMockClient(ctrl)
			c.EXPECT().Options().Return(clientOpts).AnyTimes()
			sess := client.NewMockSession(ctrl)
			iters := encoding.NewMockSeriesIterators(ctrl)
			sess.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(
				iters, client.FetchResponseMetadata{}, nil)
			iters.EXPECT().Len().Return(0)
			iters.EXPECT().Iters().Return(nil)
			iters.EXPECT().Close()

			ctx, cancel := thrift.NewContext(time.Second)
			defer cancel()
			req := &rpc.QueryRequest{
				ClusterOptions: tc.reqOpts,
			}
			c.EXPECT().NewSessionWithOptions(tc.sessOpts).Return(sess, nil)
			s := NewService(c).(*service)
			res, err := s.Query(ctx, req)
			require.NoError(t, err)
			require.NotNil(t, res)
		})
	}
}

func (s sessionOpts) Matches(x interface{}) bool {
	if c, ok := x.(client.Options); ok {
		if s.equalTimestampStrategy != c.IterationOptions().IterateEqualTimestampStrategy {
			return false
		}
		if s.readConsistency != c.ReadConsistencyLevel() {
			return false
		}
		if s.consistencyOverride && c.RuntimeOptionsManager() != nil {
			return false
		}
		return true
	}
	return false
}

func (s sessionOpts) String() string {
	return fmt.Sprintf("%v %v", s.readConsistency, s.equalTimestampStrategy)
}

var _ gomock.Matcher = &sessionOpts{}
