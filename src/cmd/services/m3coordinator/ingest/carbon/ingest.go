// Copyright (c) 2019 Uber Technologies, Inc.
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

package ingestcarbon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"regexp"
	"sync"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3coordinator/downsample"
	"github.com/m3db/m3/src/cmd/services/m3coordinator/ingest"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/carbon"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/graphite/graphite"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/log"
	m3xserver "github.com/m3db/m3x/server"
	xsync "github.com/m3db/m3x/sync"
	xtime "github.com/m3db/m3x/time"

	"github.com/uber-go/tally"
)

var (
	// Used for parsing carbon names into tags.
	carbonSeparatorByte  = byte('.')
	carbonSeparatorBytes = []byte{carbonSeparatorByte}

	errCannotGenerateTagsFromEmptyName = errors.New("cannot generate tags from empty name")
	errIOptsMustBeSet                  = errors.New("carbon ingester options: instrument options must be st")
	errWorkerPoolMustBeSet             = errors.New("carbon ingester options: worker pool must be set")
)

// Options configures the ingester.
type Options struct {
	InstrumentOptions instrument.Options
	WorkerPool        xsync.PooledWorkerPool
	Timeout           time.Duration
}

// CarbonIngesterRules contains the carbon ingestion rules.
type CarbonIngesterRules struct {
	Rules []config.CarbonIngesterRuleConfiguration
}

// Validate validates the options struct.
func (o *Options) Validate() error {
	if o.InstrumentOptions == nil {
		return errIOptsMustBeSet
	}

	if o.WorkerPool == nil {
		return errWorkerPoolMustBeSet
	}

	return nil
}

// NewIngester returns an ingester for carbon metrics.
func NewIngester(
	downsamplerAndWriter ingest.DownsamplerAndWriter,
	rules CarbonIngesterRules,
	opts Options,
) (m3xserver.Handler, error) {
	err := opts.Validate()
	if err != nil {
		return nil, err
	}

	tagOpts := models.NewTagOptions().SetIDSchemeType(models.TypeGraphite)
	err = tagOpts.Validate()
	if err != nil {
		return nil, err
	}

	// Compile all the carbon ingestion rules into regexp so that we can
	// perform matching.
	compiledRules := []ruleAndRegex{}
	for _, rule := range rules.Rules {
		compiled, err := regexp.Compile(rule.Pattern)
		if err != nil {
			return nil, err
		}

		mappingRules := []downsample.MappingRule{}
		for _, currPolicy := range rule.Policies {
			mappingRule := downsample.MappingRule{
				// TODO(rartoul): Is passing second for precision correct here?
				Policies: policy.StoragePolicies{
					policy.NewStoragePolicy(currPolicy.Resolution, xtime.Second, currPolicy.Retention),
				},
			}
			if currPolicy.Aggregatation.Enabled {
				// TODO: Need to parse config string so its not always Mean
				mappingRule.Aggregations = []aggregation.Type{aggregation.Mean}
			}
			mappingRules = append(mappingRules, mappingRule)
		}

		compiledRules = append(compiledRules, ruleAndRegex{
			rule:         rule,
			regexp:       compiled,
			mappingRules: mappingRules,
		})
	}

	return &ingester{
		downsamplerAndWriter: downsamplerAndWriter,
		opts:                 opts,
		logger:               opts.InstrumentOptions.Logger(),
		tagOpts:              tagOpts,
		metrics: newCarbonIngesterMetrics(
			opts.InstrumentOptions.MetricsScope()),

		rules: compiledRules,
	}, nil
}

type ingester struct {
	downsamplerAndWriter ingest.DownsamplerAndWriter
	opts                 Options
	logger               log.Logger
	metrics              carbonIngesterMetrics
	tagOpts              models.TagOptions

	rules []ruleAndRegex
}

func (i *ingester) Handle(conn net.Conn) {
	var (
		wg     = sync.WaitGroup{}
		s      = carbon.NewScanner(conn, i.opts.InstrumentOptions)
		logger = i.opts.InstrumentOptions.Logger()
	)

	logger.Debug("handling new carbon ingestion connection")
	for s.Scan() {
		name, timestamp, value := s.Metric()
		// TODO(rartoul): Pool.
		// Copy name since scanner bytes are recycled.
		name = append([]byte(nil), name...)

		wg.Add(1)
		i.opts.WorkerPool.Go(func() {
			ok := i.write(name, timestamp, value)
			if ok {
				i.metrics.success.Inc(1)
			}
			wg.Done()
		})

		i.metrics.malformed.Inc(int64(s.MalformedCount))
		s.MalformedCount = 0
	}

	if err := s.Err(); err != nil {
		logger.Errorf("encountered error during carbon ingestion when scanning connection: %s", err)
	}

	logger.Debugf("waiting for outstanding carbon ingestion writes to complete")
	wg.Wait()
	logger.Debugf("all outstanding writes completed, shutting down carbon ingestion handler")

	// Don't close the connection, that is the server's responsibility.
}

func (i *ingester) write(name []byte, timestamp time.Time, value float64) bool {
	mappingRules := []downsample.MappingRule{}
	for _, rule := range i.rules {
		if rule.regexp.Match(name) {
			mappingRules = append(mappingRules, rule.mappingRules...)
		}
	}

	datapoints := []ts.Datapoint{{Timestamp: timestamp, Value: value}}
	// TODO(rartoul): Pool.
	tags, err := GenerateTagsFromName(name, i.tagOpts)
	if err != nil {
		i.logger.Errorf("err generating tags from carbon name: %s, err: %s",
			string(name), err)
		i.metrics.malformed.Inc(1)
		return false
	}

	var (
		ctx     = context.Background()
		cleanup func()
	)
	if i.opts.Timeout > 0 {
		ctx, cleanup = context.WithTimeout(ctx, i.opts.Timeout)
	}

	// TODO(rartoul): Modify interface so I can pass mapping rules. downsamplerAndWriter
	// will check if any aggregations are specified, and if so, write to the downsampler,
	// otherwise it will write to the storage.
	err = i.downsamplerAndWriter.Write(ctx, tags, datapoints, xtime.Second)
	if cleanup != nil {
		cleanup()
	}

	if err != nil {
		i.logger.Errorf("err writing carbon metric: %s, err: %s",
			string(name), err)
		i.metrics.err.Inc(1)
		return false
	}

	return true
}

func (i *ingester) Close() {
	// We don't maintain any state in-between connections so there is nothing to do here.
}

func newCarbonIngesterMetrics(m tally.Scope) carbonIngesterMetrics {
	return carbonIngesterMetrics{
		success:   m.Counter("success"),
		err:       m.Counter("error"),
		malformed: m.Counter("malformed"),
	}
}

type carbonIngesterMetrics struct {
	success   tally.Counter
	err       tally.Counter
	malformed tally.Counter
}

// GenerateTagsFromName accepts a carbon metric name and blows it up into a list of
// key-value pair tags such that an input like:
//      foo.bar.baz
// becomes
//      __g0__:foo
//      __g1__:bar
//      __g2__:baz
func GenerateTagsFromName(
	name []byte,
	opts models.TagOptions,
) (models.Tags, error) {
	if len(name) == 0 {
		return models.EmptyTags(), errCannotGenerateTagsFromEmptyName
	}

	var (
		numTags = bytes.Count(name, carbonSeparatorBytes) + 1
		tags    = make([]models.Tag, 0, numTags)
	)

	startIdx := 0
	tagNum := 0
	for i, charByte := range name {
		if charByte == carbonSeparatorByte {
			if i+1 < len(name) && name[i+1] == carbonSeparatorByte {
				return models.EmptyTags(),
					fmt.Errorf("carbon metric: %s has duplicate separator", string(name))
			}

			tags = append(tags, models.Tag{
				Name:  graphite.TagName(tagNum),
				Value: name[startIdx:i],
			})
			startIdx = i + 1
			tagNum++
		}
	}

	// Write out the final tag since the for loop above will miss anything
	// after the final separator. Note, that we make sure that the final
	// character in the name is not the separator because in that case there
	// would be no additional tag to add. I.E if the input was:
	//      foo.bar.baz
	// then the for loop would append foo and bar, but we would still need to
	// append baz, however, if the input was:
	//      foo.bar.baz.
	// then the foor loop would have appended foo, bar, and baz already.
	if name[len(name)-1] != carbonSeparatorByte {
		tags = append(tags, models.Tag{
			Name:  graphite.TagName(tagNum),
			Value: name[startIdx:],
		})
	}

	return models.NewTags(numTags, opts).AddTags(tags), nil
}
