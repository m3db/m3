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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/integration/resources"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
)

func TestNewDBNodeNoSetup(t *testing.T) {
	dbnode, err := NewDBNodeFromYAML(defaultDBNodeConfig, DBNodeOptions{})
	require.NoError(t, err)

	require.NoError(t, dbnode.Close())
}

func TestNewDBNode(t *testing.T) {
	_, closer := setupNode(t)
	closer()
}

func TestHealth(t *testing.T) {
	dbnode, closer := setupNode(t)
	defer closer()

	res, err := dbnode.Health()
	require.NoError(t, err)

	require.True(t, res.Ok)
}

func TestWaitForBootstrap(t *testing.T) {
	dbnode, closer := setupNode(t)
	defer closer()

	res, err := dbnode.Health()
	require.NoError(t, err)

	require.Equal(t, true, res.Bootstrapped)
}

func TestWriteFetchRoundtrip(t *testing.T) {
	dbnode, closer := setupNode(t)
	defer closer()

	var (
		id  = "foo"
		ts  = time.Now()
		val = 1.0
	)
	req := &rpc.WriteRequest{
		NameSpace: resources.UnaggName,
		ID:        id,
		Datapoint: &rpc.Datapoint{
			Timestamp: ts.Unix(),
			Value:     val,
		},
	}
	require.NoError(t, dbnode.WritePoint(req))

	freq := &rpc.FetchRequest{
		RangeStart: ts.Add(-1 * time.Minute).Unix(),
		RangeEnd:   ts.Add(1 * time.Minute).Unix(),
		NameSpace:  resources.UnaggName,
		ID:         id,
	}
	res, err := dbnode.Fetch(freq)
	require.NoError(t, err)

	require.Equal(t, 1, len(res.Datapoints))
	require.Equal(t, rpc.FetchResult_{
		Datapoints: []*rpc.Datapoint{
			{Timestamp: ts.Unix(), Value: val},
		},
	}, res.Datapoints[0])
}

func TestWriteTaggedFetchTaggedRoundtrip(t *testing.T) {
	dbnode, closer := setupNode(t)
	defer closer()

	var (
		id  = "fooTagged"
		ts  = time.Now()
		val = 1.0
	)
	req := &rpc.WriteTaggedRequest{
		NameSpace: resources.UnaggName,
		ID:        id,
		Datapoint: &rpc.Datapoint{
			Timestamp:         ts.UnixNano(),
			TimestampTimeType: rpc.TimeType_UNIX_NANOSECONDS,
			Value:             val,
		},
		Tags: []*rpc.Tag{
			{Name: "__name__", Value: id},
			{Name: "job", Value: "bar"},
		},
	}
	require.NoError(t, dbnode.WriteTaggedPoint(req))

	query := idx.NewTermQuery([]byte("job"), []byte("bar"))
	encoded, err := idx.Marshal(query)
	require.NoError(t, err)

	freq := &rpc.FetchTaggedRequest{
		RangeStart:    ts.Add(-1 * time.Minute).UnixNano(),
		RangeEnd:      ts.Add(1 * time.Minute).UnixNano(),
		NameSpace:     []byte(resources.UnaggName),
		RangeTimeType: rpc.TimeType_UNIX_NANOSECONDS,
		FetchData:     true,
		Query:         encoded,
	}
	res, err := dbnode.FetchTagged(freq)
	require.NoError(t, err)

	require.Equal(t, 1, len(res.Elements))
	require.Equal(t, id, string(res.Elements[0].ID))
	require.Equal(t, resources.UnaggName, string(res.Elements[0].NameSpace))

	// Validate Tags
	testTagDecoderPool := serialize.NewTagDecoderPool(
		serialize.NewTagDecoderOptions(serialize.TagDecoderOptionsConfig{}),
		pool.NewObjectPoolOptions())
	testTagDecoderPool.Init()

	dec := testTagDecoderPool.Get()
	dec.Reset(checked.NewBytes(res.Elements[0].EncodedTags, nil))

	require.True(t, dec.Next())
	validateTag(t, dec.Current(), "__name__", id)
	require.True(t, dec.Next())
	validateTag(t, dec.Current(), "job", "bar")
	require.False(t, dec.Next())
}

// TODO(nate): tests for remainder of interface

func validateTag(t *testing.T, tag ident.Tag, name string, value string) {
	require.Equal(t, name, tag.Name.String())
	require.Equal(t, value, tag.Value.String())
}

func setupNode(t *testing.T) (resources.Node, func()) {
	dbnode, err := NewDBNodeFromYAML(defaultDBNodeConfig, DBNodeOptions{})
	require.NoError(t, err)

	coord, err := NewCoordinatorFromYAML(defaultCoordConfig, CoordinatorOptions{})
	require.NoError(t, err)

	require.NoError(t, coord.WaitForNamespace(""))

	host, err := dbnode.HostDetails(9000)
	require.NoError(t, err)

	_, err = coord.CreateDatabase(admin.DatabaseCreateRequest{
		Type:              "cluster",
		NamespaceName:     resources.UnaggName,
		RetentionTime:     "1h",
		NumShards:         4,
		ReplicationFactor: 1,
		Hosts:             []*admin.Host{host},
	})
	require.NoError(t, err)

	require.NoError(t, dbnode.WaitForBootstrap())

	return dbnode, func() {
		assert.NoError(t, coord.Close())
		assert.NoError(t, dbnode.Close())
	}
}

const defaultDBNodeConfig = `
db:
  filesystem:
    filePathPrefix: "*"
  writeNewSeriesAsync: false
`
