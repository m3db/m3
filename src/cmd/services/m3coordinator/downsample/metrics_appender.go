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
	"fmt"
	"sort"
	"time"

	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/aggregator/client"
	"github.com/m3db/m3/src/metrics/matcher"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/x/serialize"
	"github.com/m3db/m3x/clock"
)

type metricsAppender struct {
	metricsAppenderOptions

	tags                 *tags
	multiSamplesAppender *multiSamplesAppender
}

// metricsAppenderOptions will have one of agg or clientRemote set.
type metricsAppenderOptions struct {
	agg          aggregator.Aggregator
	clientRemote client.Client

	defaultStagedMetadatas []metadata.StagedMetadatas
	clockOpts              clock.Options
	tagEncoder             serialize.TagEncoder
	matcher                matcher.Matcher
	metricTagsIteratorPool serialize.MetricTagsIteratorPool
}

func (a *metricsAppender) AddTag(name, value []byte) {
	a.tags.append(name, value)
}

func (a *metricsAppender) SamplesAppender(opts SampleAppenderOptions) (SamplesAppender, error) {
	// Sort tags
	sort.Sort(a.tags)

	// Encode tags and compute a temporary (unowned) ID
	a.tagEncoder.Reset()
	if err := a.tagEncoder.Encode(a.tags); err != nil {
		return nil, err
	}
	data, ok := a.tagEncoder.Data()
	if !ok {
		return nil, fmt.Errorf("unable to encode tags: names=%v, values=%v",
			a.tags.names, a.tags.values)
	}

	a.multiSamplesAppender.reset()
	unownedID := data.Bytes()

	// Match policies and rollups and build samples appender
	id := a.metricTagsIteratorPool.Get()
	id.Reset(unownedID)
	now := time.Now()
	nowNanos := now.UnixNano()
	fromNanos := nowNanos
	toNanos := nowNanos + 1
	matchResult := a.matcher.ForwardMatch(id, fromNanos, toNanos)
	id.Close()

	if opts.Override {
		for _, rule := range opts.OverrideRules.MappingRules {
			stagedMetadatas, err := rule.StagedMetadatas()
			if err != nil {
				return nil, err
			}
			a.multiSamplesAppender.addSamplesAppender(samplesAppender{
				agg:             a.agg,
				clientRemote:    a.clientRemote,
				unownedID:       unownedID,
				stagedMetadatas: stagedMetadatas,
			})
		}
	} else {
		// Always aggregate any default staged metadats
		for _, stagedMetadatas := range a.defaultStagedMetadatas {
			a.multiSamplesAppender.addSamplesAppender(samplesAppender{
				agg:             a.agg,
				clientRemote:    a.clientRemote,
				unownedID:       unownedID,
				stagedMetadatas: stagedMetadatas,
			})
		}

		stagedMetadatas := matchResult.ForExistingIDAt(nowNanos)
		if !stagedMetadatas.IsDefault() && len(stagedMetadatas) != 0 {
			// Only sample if going to actually aggregate
			a.multiSamplesAppender.addSamplesAppender(samplesAppender{
				agg:             a.agg,
				clientRemote:    a.clientRemote,
				unownedID:       unownedID,
				stagedMetadatas: stagedMetadatas,
			})
		}

		numRollups := matchResult.NumNewRollupIDs()
		for i := 0; i < numRollups; i++ {
			rollup := matchResult.ForNewRollupIDsAt(i, nowNanos)
			a.multiSamplesAppender.addSamplesAppender(samplesAppender{
				agg:             a.agg,
				clientRemote:    a.clientRemote,
				unownedID:       rollup.ID,
				stagedMetadatas: rollup.Metadatas,
			})
		}
	}

	return a.multiSamplesAppender, nil
}

func (a *metricsAppender) Reset() {
	a.tags.names = a.tags.names[:0]
	a.tags.values = a.tags.values[:0]
}

func (a *metricsAppender) Finalize() {
	a.tagEncoder.Finalize()
	a.tagEncoder = nil
}
