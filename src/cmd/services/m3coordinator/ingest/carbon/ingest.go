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
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
	m3xserver "github.com/m3db/m3/src/x/server"
	xsync "github.com/m3db/m3/src/x/sync"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

const (
	maxResourcePoolNameSize = 1024
	maxPooledTagsSize       = 16
	defaultResourcePoolSize = 4096
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
	Debug             bool
	InstrumentOptions instrument.Options
	WorkerPool        xsync.PooledWorkerPool
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

	compiledRules, err := compileRules(rules)
	if err != nil {
		return nil, err
	}

	poolOpts := pool.NewObjectPoolOptions().
		SetInstrumentOptions(opts.InstrumentOptions).
		SetRefillLowWatermark(0).
		SetRefillHighWatermark(0).
		SetSize(defaultResourcePoolSize)

	resourcePool := pool.NewObjectPool(poolOpts)
	resourcePool.Init(func() interface{} {
		return &lineResources{
			name:       make([]byte, 0, maxResourcePoolNameSize),
			datapoints: make([]ts.Datapoint, 1),
			tags:       make([]models.Tag, 0, maxPooledTagsSize),
		}
	})

	return &ingester{
		downsamplerAndWriter: downsamplerAndWriter,
		opts:                 opts,
		logger:               opts.InstrumentOptions.Logger(),
		tagOpts:              tagOpts,
		metrics: newCarbonIngesterMetrics(
			opts.InstrumentOptions.MetricsScope()),

		rules: compiledRules,

		lineResourcesPool: resourcePool,
	}, nil
}

type ingester struct {
	downsamplerAndWriter ingest.DownsamplerAndWriter
	opts                 Options
	logger               *zap.Logger
	metrics              carbonIngesterMetrics
	tagOpts              models.TagOptions

	rules []ruleAndRegex

	lineResourcesPool pool.ObjectPool
}

func (i *ingester) Handle(conn net.Conn) {
	var (
		// Interfaces require a context be passed, but M3DB client already has timeouts
		// built in and allocating a new context each time is expensive so we just pass
		// the same context always and rely on M3DB client timeouts.
		ctx    = context.Background()
		wg     = sync.WaitGroup{}
		s      = carbon.NewScanner(conn, i.opts.InstrumentOptions)
		logger = i.opts.InstrumentOptions.Logger()
	)

	logger.Debug("handling new carbon ingestion connection")
	for s.Scan() {
		name, timestamp, value := s.Metric()

		resources := i.getLineResources()
		// Copy name since scanner bytes are recycled.
		resources.name = append(resources.name[:0], name...)

		wg.Add(1)
		i.opts.WorkerPool.Go(func() {
			ok := i.write(ctx, resources, timestamp, value)
			if ok {
				i.metrics.success.Inc(1)
			}
			// The contract is that after the DownsamplerAndWriter returns, any resources
			// that it needed to hold onto have already been copied.
			i.putLineResources(resources)
			wg.Done()
		})

		i.metrics.malformed.Inc(int64(s.MalformedCount))
		s.MalformedCount = 0
	}

	if err := s.Err(); err != nil {
		logger.Error("encountered error during carbon ingestion when scanning connection", zap.Error(err))
	}

	logger.Debug("waiting for outstanding carbon ingestion writes to complete")
	wg.Wait()
	logger.Debug("all outstanding writes completed, shutting down carbon ingestion handler")

	// Don't close the connection, that is the server's responsibility.
}

func (i *ingester) write(
	ctx context.Context,
	resources *lineResources,
	timestamp time.Time,
	value float64,
) bool {
	downsampleAndStoragePolicies := ingest.WriteOptions{
		// Set both of these overrides to true to indicate that only the exact mapping
		// rules and storage policies that we provide should be used and that all
		// default behavior (like performing all possible downsamplings and writing
		// all data to the unaggregated namespace in storage) should be ignored.
		DownsampleOverride: true,
		WriteOverride:      true,
	}

	for _, rule := range i.rules {
		if rule.rule.Pattern == graphite.MatchAllPattern || rule.regexp.Match(resources.name) {
			// Each rule should only have either mapping rules or storage policies so
			// one of these should be a no-op.
			downsampleAndStoragePolicies.DownsampleMappingRules = rule.mappingRules
			downsampleAndStoragePolicies.WriteStoragePolicies = rule.storagePolicies

			if i.opts.Debug {
				i.logger.Info("carbon metric matched by pattern",
					zap.String("name", string(resources.name)),
					zap.Any("pattern", rule.rule.Pattern),
					zap.Any("mappingRules", rule.mappingRules),
					zap.Any("storagePolicies", rule.storagePolicies))
			}
			// Break because we only want to apply one rule per metric based on which
			// ever one matches first.
			break
		}
	}

	if len(downsampleAndStoragePolicies.DownsampleMappingRules) == 0 &&
		len(downsampleAndStoragePolicies.WriteStoragePolicies) == 0 {
		// Nothing to do if none of the policies matched.
		if i.opts.Debug {
			i.logger.Info("no rules matched carbon metric, skipping",
				zap.String("name", string(resources.name)))
		}
		return false
	}

	resources.datapoints[0] = ts.Datapoint{Timestamp: timestamp, Value: value}
	tags, err := GenerateTagsFromNameIntoSlice(resources.name, i.tagOpts, resources.tags)
	if err != nil {
		i.logger.Error("err generating tags from carbon",
			zap.String("name", string(resources.name)), zap.Error(err))
		i.metrics.malformed.Inc(1)
		return false
	}

	err = i.downsamplerAndWriter.Write(
		ctx, tags, resources.datapoints, xtime.Second, downsampleAndStoragePolicies)

	if err != nil {
		i.logger.Error("err writing carbon metric",
			zap.String("name", string(resources.name)), zap.Error(err))
		i.metrics.err.Inc(1)
		return false
	}

	if i.opts.Debug {
		i.logger.Info("successfully wrote carbon metric",
			zap.String("name", string(resources.name)))
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
	return generateTagsFromName(name, opts, nil)
}

// GenerateTagsFromNameIntoSlice does the same thing as GenerateTagsFromName except
// it allows the caller to provide the slice into which the tags are appended.
func GenerateTagsFromNameIntoSlice(
	name []byte,
	opts models.TagOptions,
	tags []models.Tag,
) (models.Tags, error) {
	return generateTagsFromName(name, opts, tags)
}

func generateTagsFromName(
	name []byte,
	opts models.TagOptions,
	tags []models.Tag,
) (models.Tags, error) {
	if len(name) == 0 {
		return models.EmptyTags(), errCannotGenerateTagsFromEmptyName
	}

	numTags := bytes.Count(name, carbonSeparatorBytes) + 1

	if cap(tags) >= numTags {
		tags = tags[:0]
	} else {
		tags = make([]models.Tag, 0, numTags)
	}

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

	return models.Tags{Opts: opts, Tags: tags}, nil
}

// Compile all the carbon ingestion rules into regexp so that we can
// perform matching. Also, generate all the mapping rules and storage
// policies that we will need to pass to the DownsamplerAndWriter upfront
// so that we don't need to create them each time.
//
// Note that only one rule will be applied per metric and rules are applied
// such that the first one that matches takes precedence. As a result we need
// to make sure to maintain the order of the rules when we generate the compiled ones.
func compileRules(rules CarbonIngesterRules) ([]ruleAndRegex, error) {
	compiledRules := []ruleAndRegex{}
	for _, rule := range rules.Rules {
		compiled, err := regexp.Compile(rule.Pattern)
		if err != nil {
			return nil, err
		}

		storagePolicies := []policy.StoragePolicy{}
		for _, currPolicy := range rule.Policies {
			storagePolicy := policy.NewStoragePolicy(
				currPolicy.Resolution, xtime.Second, currPolicy.Retention)
			storagePolicies = append(storagePolicies, storagePolicy)
		}

		compiledRule := ruleAndRegex{
			rule:   rule,
			regexp: compiled,
		}

		if rule.Aggregation.EnabledOrDefault() {
			compiledRule.mappingRules = []downsample.MappingRule{downsample.MappingRule{
				Aggregations: []aggregation.Type{rule.Aggregation.TypeOrDefault()},
				Policies:     storagePolicies,
			}}
		} else {
			compiledRule.storagePolicies = storagePolicies
		}
		compiledRules = append(compiledRules, compiledRule)
	}

	return compiledRules, nil
}

func (i *ingester) getLineResources() *lineResources {
	return i.lineResourcesPool.Get().(*lineResources)
}

func (i *ingester) putLineResources(l *lineResources) {
	tooLargeForPool := cap(l.name) > maxResourcePoolNameSize ||
		len(l.datapoints) > 1 || // We always write one datapoint at a time.
		cap(l.datapoints) > 1 ||
		cap(l.tags) > maxPooledTagsSize

	if tooLargeForPool {
		return
	}

	// Reset.
	l.name = l.name[:0]
	l.datapoints[0] = ts.Datapoint{}
	for i := range l.tags {
		// Free pointers.
		l.tags[i] = models.Tag{}
	}
	l.tags = l.tags[:0]

	i.lineResourcesPool.Put(l)
}

type lineResources struct {
	name       []byte
	datapoints []ts.Datapoint
	tags       []models.Tag
}

type ruleAndRegex struct {
	rule            config.CarbonIngesterRuleConfiguration
	regexp          *regexp.Regexp
	mappingRules    []downsample.MappingRule
	storagePolicies []policy.StoragePolicy
}
