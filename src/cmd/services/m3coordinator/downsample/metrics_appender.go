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

	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3db/src/dbnode/serialize"
	"github.com/m3db/m3metrics/matcher"
	"github.com/m3db/m3x/clock"
)

type metricsAppender struct {
	metricsAppenderOptions

	tags                 *tags
	multiSamplesAppender *multiSamplesAppender
}

type metricsAppenderOptions struct {
	agg                     aggregator.Aggregator
	clockOpts               clock.Options
	tagEncoder              serialize.TagEncoder
	matcher                 matcher.Matcher
	encodedTagsIteratorPool *encodedTagsIteratorPool
}

func (a *metricsAppender) AddTag(name, value string) {
	a.tags.names = append(a.tags.names, name)
	a.tags.values = append(a.tags.values, name)
}

func (a *metricsAppender) SamplesAppender() (SamplesAppender, error) {
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

	unownedID := data.Bytes()

	// Match policies and rollups and build samples appender
	id := a.encodedTagsIteratorPool.Get()
	id.Reset(unownedID)
	now := time.Now()
	fromNanos := now.Add(-1 * a.clockOpts.MaxNegativeSkew()).UnixNano()
	toNanos := now.Add(1 * a.clockOpts.MaxPositiveSkew()).UnixNano()
	matchResult := a.matcher.ForwardMatch(id, fromNanos, toNanos)
	id.Close()

	policies := matchResult.MappingsAt(now.UnixNano())

	a.multiSamplesAppender.reset()
	a.multiSamplesAppender.addSamplesAppender(samplesAppender{
		agg:       a.agg,
		unownedID: unownedID,
		policies:  policies,
	})

	numRollups := matchResult.NumRollups()
	for i := 0; i < numRollups; i++ {
		if rollup, ok := matchResult.RollupsAt(i, now.UnixNano()); ok {
			a.multiSamplesAppender.addSamplesAppender(samplesAppender{
				agg:       a.agg,
				unownedID: rollup.ID,
				policies:  rollup.PoliciesList,
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
