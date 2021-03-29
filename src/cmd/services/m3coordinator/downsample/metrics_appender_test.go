// Copyright (c) 2020 Uber Technologies, Inc.
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
	"errors"
	"fmt"
	"testing"

	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/metrics/matcher"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/rules"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSamplesAppenderPoolResetsTagsAcrossSamples(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	count := 3

	poolOpts := pool.NewObjectPoolOptions().SetSize(1)
	appenderPool := newMetricsAppenderPool(poolOpts)

	tagEncoderPool := serialize.NewTagEncoderPool(serialize.NewTagEncoderOptions(),
		poolOpts)
	tagEncoderPool.Init()

	size := 1
	tagDecoderPool := serialize.NewTagDecoderPool(
		serialize.NewTagDecoderOptions(serialize.TagDecoderOptionsConfig{
			CheckBytesWrapperPoolSize: &size,
		}), poolOpts)
	tagDecoderPool.Init()

	metricTagsIteratorPool := serialize.NewMetricTagsIteratorPool(tagDecoderPool, poolOpts)
	metricTagsIteratorPool.Init()

	for i := 0; i < count; i++ {
		matcher := matcher.NewMockMatcher(ctrl)
		matcher.EXPECT().ForwardMatch(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(encodedID id.ID, _, _ int64) rules.MatchResult {
				// NB: ensure tags are cleared correctly between runs.
				bs := encodedID.Bytes()

				decoder := tagDecoderPool.Get()
				decoder.Reset(checked.NewBytes(bs, nil))

				var id string
				for decoder.Next() {
					tag := decoder.Current()
					tagStr := fmt.Sprintf("%s-%s", tag.Name.String(), tag.Value.String())
					if len(id) == 0 {
						id = tagStr
					} else {
						id = fmt.Sprintf("%s,%s", id, tagStr)
					}
				}

				decoder.Close()
				return rules.NewMatchResult(1, 1,
					metadata.StagedMetadatas{},
					[]rules.IDWithMetadatas{
						{
							ID:        []byte(id),
							Metadatas: metadata.StagedMetadatas{},
						},
					},
					true,
				)
			})

		appender := appenderPool.Get()
		agg := aggregator.NewMockAggregator(ctrl)
		appender.reset(metricsAppenderOptions{
			tagEncoderPool:         tagEncoderPool,
			metricTagsIteratorPool: metricTagsIteratorPool,
			matcher:                matcher,
			agg:                    agg,
		})
		name := []byte(fmt.Sprint("foo", i))
		value := []byte(fmt.Sprint("bar", i))
		appender.AddTag(name, value)
		a, err := appender.SamplesAppender(SampleAppenderOptions{})
		require.NoError(t, err)

		agg.EXPECT().AddUntimed(gomock.Any(), gomock.Any()).DoAndReturn(
			func(u unaggregated.MetricUnion, _ metadata.StagedMetadatas) error {
				if u.CounterVal != int64(i) {
					return errors.New("wrong counter value")
				}

				// NB: expected ID is generated into human-readable form
				// from tags in ForwardMatch mock above. Also include the m3 type, which is included when matching.
				// nolint:scopelint
				expected := fmt.Sprintf("__m3_type__-gauge,foo%d-bar%d", i, i)
				if expected != u.ID.String() {
					// NB: if this fails, appender is holding state after Finalize.
					return fmt.Errorf("expected ID %s, got %s", expected, u.ID.String())
				}

				return nil
			},
		)

		require.NoError(t, a.SamplesAppender.AppendCounterSample(int64(i)))

		assert.False(t, a.IsDropPolicyApplied)
		appender.Finalize()
	}
}

func TestSamplesAppenderPoolResetsTagSimple(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	poolOpts := pool.NewObjectPoolOptions().SetSize(1)
	appenderPool := newMetricsAppenderPool(poolOpts)

	appender := appenderPool.Get()
	appender.AddTag([]byte("foo"), []byte("bar"))
	assert.Equal(t, 1, len(appender.originalTags.names))
	assert.Equal(t, 1, len(appender.originalTags.values))
	appender.Finalize()

	// NB: getting a new appender from the pool yields a clean appender.
	appender = appenderPool.Get()
	assert.Nil(t, appender.originalTags)
	appender.Finalize()
}
