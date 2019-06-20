// Copyright (c) 2018 Uber Technologies, Inc.
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

package downsample

import (
	"bytes"
	"testing"

	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/mock"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
	xsync "github.com/m3db/m3/src/x/sync"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestDownsamplerFlushHandlerCopiesTags(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mock.NewMockStorage()

	tagEncoderOptions := serialize.NewTagEncoderOptions()
	tagDecoderOptions := serialize.NewTagDecoderOptions()
	tagEncoderPool := serialize.NewTagEncoderPool(tagEncoderOptions,
		pool.NewObjectPoolOptions().SetSize(1))
	tagEncoderPool.Init()
	tagDecoderPool := serialize.NewTagDecoderPool(tagDecoderOptions,
		pool.NewObjectPoolOptions().SetSize(1))
	tagDecoderPool.Init()

	pool := serialize.NewMockMetricTagsIteratorPool(ctrl)

	workers := xsync.NewWorkerPool(1)
	workers.Init()

	instrumentOpts := instrument.NewOptions()

	handler := newDownsamplerFlushHandler(store, pool, tagEncoderPool,
		tagDecoderPool, workers, models.NewTagOptions(), instrumentOpts)
	writer, err := handler.NewWriter(tally.NoopScope)
	require.NoError(t, err)

	var (
		expectedID = []byte("foo")
		tagName    = []byte("name")
		tagValue   = []byte("value")
	)
	iter := serialize.NewMockMetricTagsIterator(ctrl)
	gomock.InOrder(
		iter.EXPECT().Reset(expectedID),
		iter.EXPECT().NumTags().Return(1),
		iter.EXPECT().Next().Return(true),
		iter.EXPECT().Current().Return(tagName, tagValue),
		iter.EXPECT().Next().Return(false),
		iter.EXPECT().Err().Return(nil),
		iter.EXPECT().Close(),
	)

	pool.EXPECT().Get().Return(iter)

	// Write metric
	err = writer.Write(aggregated.ChunkedMetricWithStoragePolicy{
		ChunkedMetric: aggregated.ChunkedMetric{
			ChunkedID: id.ChunkedID{Data: expectedID},
			TimeNanos: 123,
			Value:     42.42,
		},
		StoragePolicy: policy.MustParseStoragePolicy("1s:1d"),
	})
	require.NoError(t, err)

	// Wait for flush
	err = writer.Flush()
	require.NoError(t, err)

	// Inspect the write
	writes := store.Writes()
	require.Equal(t, 1, len(writes))

	// Ensure tag pointers _DO_NOT_ match but equal to same content
	tags := writes[0].Tags()
	require.Equal(t, 1, tags.Remaining())

	require.True(t, tags.Next())
	tag := tags.Current()
	assert.True(t, bytes.Equal(tagName, tag.Name.Bytes()))
	assert.True(t, bytes.Equal(tagValue, tag.Value.Bytes()))
	assert.False(t, xtest.ByteSlicesBackedBySameData(tagName, tag.Name.Bytes()))
	assert.False(t, xtest.ByteSlicesBackedBySameData(tagValue, tag.Value.Bytes()))

	require.NoError(t, tags.Err())
}
