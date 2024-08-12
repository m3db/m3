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

// Package simple contains code simple integration tests that writes and reads.
package simple

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/integration/resources"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
)

const (
	// TestSimpleDBNodeConfig is the test config for the dbnode.
	TestSimpleDBNodeConfig = `
db: {}
`

	// TestSimpleCoordinatorConfig is the test config for the coordinator.
	TestSimpleCoordinatorConfig = `
clusters:
  - namespaces:
      - namespace: default
        type: unaggregated
        retention: 48h
`
)

// RunTest contains the logic for running the simple test.
func RunTest(t *testing.T, m3 resources.M3Resources) {
	var (
		id  = "foo"
		val = 42.123456789
		ts  = time.Now()
	)

	encoderPool := newTagEncoderPool()
	encodedTags := encodeTags(t, encoderPool,
		ident.StringTag("city", "new_york"),
		ident.StringTag("endpoint", "/request"))

	// Write data point.
	req := &rpc.WriteTaggedBatchRawRequest{
		NameSpace: []byte(resources.UnaggName),
		Elements: []*rpc.WriteTaggedBatchRawRequestElement{
			{
				ID:          []byte(id),
				EncodedTags: encodedTags,
				Datapoint: &rpc.Datapoint{
					Timestamp:         ts.UnixNano(),
					TimestampTimeType: rpc.TimeType_UNIX_NANOSECONDS,
					Value:             val,
				},
			},
		},
	}
	dbnode := m3.Nodes()[0]

	require.NoError(t, dbnode.WriteTaggedBatchRaw(req))

	// Fetch tagged data point.
	query, err := idx.NewRegexpQuery([]byte("city"), []byte(".*"))
	require.NoError(t, err)

	encoded, err := idx.Marshal(query)
	require.NoError(t, err)

	freq := &rpc.FetchTaggedRequest{
		RangeStart:    0,
		RangeEnd:      ts.UnixNano(),
		NameSpace:     []byte(resources.UnaggName),
		RangeTimeType: rpc.TimeType_UNIX_NANOSECONDS,
		FetchData:     true,
		Query:         encoded,
	}

	logger, err := resources.NewLogger()
	require.NoError(t, err)
	err = resources.Retry(func() error {
		res, err := dbnode.FetchTagged(freq)
		if err != nil {
			return err
		}

		if len(res.Elements) != 1 {
			err = fmt.Errorf("expected datapoint not present")
			logger.Error("number of elements not equal to 1", zap.Error(err))
			return err
		}

		logger.Info("datapoint successfully fetched")
		return nil
	})
	require.NoError(t, err)

	coord := m3.Coordinator()
	require.NoError(t, coord.DeleteAllPlacements(resources.PlacementRequestOptions{
		Service: resources.ServiceTypeM3DB,
	}))
	require.NoError(t, coord.DeleteNamespace(resources.UnaggName))
}

func newTagEncoderPool() serialize.TagEncoderPool {
	encoderPool := serialize.
		NewTagEncoderPool(serialize.NewTagEncoderOptions(),
			pool.NewObjectPoolOptions().SetSize(1))
	encoderPool.Init()
	return encoderPool
}

func encodeTags(
	t *testing.T,
	pool serialize.TagEncoderPool,
	tags ...ident.Tag,
) []byte {
	encoder := pool.Get()

	seriesTags := ident.NewTags(tags...)
	err := encoder.Encode(ident.NewTagsIterator(seriesTags))
	require.NoError(t, err)

	data, ok := encoder.Data()
	require.True(t, ok)

	return data.Bytes()
}
