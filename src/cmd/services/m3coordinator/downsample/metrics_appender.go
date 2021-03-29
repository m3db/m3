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
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/aggregator/client"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/generated/proto/metricpb"
	"github.com/m3db/m3/src/metrics/matcher"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/graphite/graphite"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"

	"github.com/gogo/protobuf/jsonpb"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var errNoTags = errors.New("no tags provided")

type metricsAppenderPool struct {
	pool pool.ObjectPool
}

func newMetricsAppenderPool(opts pool.ObjectPoolOptions) *metricsAppenderPool {
	p := &metricsAppenderPool{
		pool: pool.NewObjectPool(opts),
	}
	p.pool.Init(func() interface{} {
		return newMetricsAppender(p)
	})
	return p
}

func (p *metricsAppenderPool) Get() *metricsAppender {
	appender := p.pool.Get().(*metricsAppender)
	// NB: reset appender.
	appender.NextMetric()
	return appender
}

func (p *metricsAppenderPool) Put(v *metricsAppender) {
	p.pool.Put(v)
}

type metricsAppender struct {
	metricsAppenderOptions

	pool *metricsAppenderPool

	multiSamplesAppender         *multiSamplesAppender
	curr                         metadata.StagedMetadata
	defaultStagedMetadatasCopies []metadata.StagedMetadatas
	mappingRuleStoragePolicies   []policy.StoragePolicy

	cachedEncoders []serialize.TagEncoder
	inuseEncoders  []serialize.TagEncoder

	originalTags *tags
	cachedTags   []*tags
	inuseTags    []*tags
}

// metricsAppenderOptions will have one of agg or clientRemote set.
type metricsAppenderOptions struct {
	agg          aggregator.Aggregator
	clientRemote client.Client

	defaultStagedMetadatasProtos []metricpb.StagedMetadatas
	matcher                      matcher.Matcher
	tagEncoderPool               serialize.TagEncoderPool
	metricTagsIteratorPool       serialize.MetricTagsIteratorPool

	clockOpts    clock.Options
	debugLogging bool
	logger       *zap.Logger
}

func newMetricsAppender(pool *metricsAppenderPool) *metricsAppender {
	return &metricsAppender{
		pool:                 pool,
		multiSamplesAppender: newMultiSamplesAppender(),
	}
}

// reset is called when pulled from the pool.
func (a *metricsAppender) reset(opts metricsAppenderOptions) {
	a.metricsAppenderOptions = opts

	// Copy over any previous inuse encoders to the cached encoders list.
	a.resetEncoders()

	// Make sure a.defaultStagedMetadatasCopies is right length.
	capRequired := len(opts.defaultStagedMetadatasProtos)
	if cap(a.defaultStagedMetadatasCopies) < capRequired {
		// Too short, reallocate.
		slice := make([]metadata.StagedMetadatas, capRequired)
		a.defaultStagedMetadatasCopies = slice
	} else {
		// Has enough capacity, take subslice.
		slice := a.defaultStagedMetadatasCopies[:capRequired]
		a.defaultStagedMetadatasCopies = slice
	}
}

func (a *metricsAppender) AddTag(name, value []byte) {
	if a.originalTags == nil {
		a.originalTags = a.tags()
	}
	a.originalTags.append(name, value)
}

func (a *metricsAppender) SamplesAppender(opts SampleAppenderOptions) (SamplesAppenderResult, error) {
	if a.originalTags == nil {
		return SamplesAppenderResult{}, errNoTags
	}
	tags := a.originalTags

	// NB (@shreyas): Add the metric type tag. The tag has the prefix
	// __m3_. All tags with that prefix are only used for the purpose of
	// filter match and then stripped off before we actually send to the aggregator.
	switch opts.MetricType {
	case ts.M3MetricTypeCounter:
		tags.append(metric.M3TypeTag, metric.M3CounterValue)
	case ts.M3MetricTypeGauge:
		tags.append(metric.M3TypeTag, metric.M3GaugeValue)
	case ts.M3MetricTypeTimer:
		tags.append(metric.M3TypeTag, metric.M3TimerValue)
	}

	// Sort tags
	sort.Sort(tags)

	// Encode tags and compute a temporary (unowned) ID
	tagEncoder := a.tagEncoder()
	if err := tagEncoder.Encode(tags); err != nil {
		return SamplesAppenderResult{}, err
	}
	data, ok := tagEncoder.Data()
	if !ok {
		return SamplesAppenderResult{}, fmt.Errorf("unable to encode tags: names=%v, values=%v",
			tags.names, tags.values)
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

	// filter out augmented metrics tags
	tags.filterPrefix(metric.M3MetricsPrefix)

	var dropApplyResult metadata.ApplyOrRemoveDropPoliciesResult
	if opts.Override {
		// Reuse a slice to keep the current staged metadatas we will apply.
		a.curr.Pipelines = a.curr.Pipelines[:0]

		for _, rule := range opts.OverrideRules.MappingRules {
			stagedMetadatas, err := rule.StagedMetadatas()
			if err != nil {
				return SamplesAppenderResult{}, err
			}

			a.debugLogMatch("downsampler applying override mapping rule",
				debugLogMatchOptions{Meta: stagedMetadatas})

			pipelines := stagedMetadatas[len(stagedMetadatas)-1]
			a.curr.Pipelines =
				append(a.curr.Pipelines, pipelines.Pipelines...)
		}

		if err := a.addSamplesAppenders(tags, a.curr); err != nil {
			return SamplesAppenderResult{}, err
		}
	} else {
		// Reuse a slice to keep the current staged metadatas we will apply.
		a.curr.Pipelines = a.curr.Pipelines[:0]

		// NB(r): First apply mapping rules to see which storage policies
		// have been applied, any that have been applied as part of
		// mapping rules that exact match a default storage policy will be
		// skipped when applying default rules, so as to avoid storing
		// the same metrics in the same namespace with the same metric
		// name and tags (i.e. overwriting each other).
		a.mappingRuleStoragePolicies = a.mappingRuleStoragePolicies[:0]

		ruleStagedMetadatas := matchResult.ForExistingIDAt(nowNanos)
		if !ruleStagedMetadatas.IsDefault() && len(ruleStagedMetadatas) != 0 {
			a.debugLogMatch("downsampler applying matched rule",
				debugLogMatchOptions{Meta: ruleStagedMetadatas})

			// Collect storage policies for all the current active mapping rules.
			// TODO: we should convert this to iterate over pointers
			// nolint:gocritic
			for _, stagedMetadata := range ruleStagedMetadatas {
				for _, pipe := range stagedMetadata.Pipelines {
					// Skip rollup rules unless configured otherwise.
					// We only want to consider mapping rules here,
					// as we still want to apply default mapping rules to
					// metrics that are rolled up to ensure the underlying metric
					// gets written to aggregated namespaces.
					if pipe.IsMappingRule() {
						for _, sp := range pipe.StoragePolicies {
							a.mappingRuleStoragePolicies =
								append(a.mappingRuleStoragePolicies, sp)
						}
					} else {
						a.debugLogMatch(
							"skipping rollup rule in populating active mapping rule policies",
							debugLogMatchOptions{},
						)
					}
				}
			}

			// Only sample if going to actually aggregate
			pipelines := ruleStagedMetadatas[len(ruleStagedMetadatas)-1]
			a.curr.Pipelines =
				append(a.curr.Pipelines, pipelines.Pipelines...)
		}

		// Always aggregate any default staged metadatas (unless
		// mapping rule has provided an override for a storage policy,
		// if so then skip aggregating for that storage policy).
		for idx, stagedMetadatasProto := range a.defaultStagedMetadatasProtos {
			// NB(r): Need to take copy of default staged metadatas as we
			// sometimes mutate it.
			stagedMetadatas := a.defaultStagedMetadatasCopies[idx]
			err := stagedMetadatas.FromProto(stagedMetadatasProto)
			if err != nil {
				return SamplesAppenderResult{},
					fmt.Errorf("unable to copy default staged metadatas: %v", err)
			}

			// Save the staged metadatas back to the idx so all slices can be reused.
			a.defaultStagedMetadatasCopies[idx] = stagedMetadatas

			stagedMetadataBeforeFilter := stagedMetadatas[:]
			if len(a.mappingRuleStoragePolicies) != 0 {
				// If mapping rules have applied aggregations for
				// storage policies then de-dupe so we don't have two
				// active aggregations for the same storage policy.
				stagedMetadatasAfterFilter := stagedMetadatas[:0]
				for _, stagedMetadata := range stagedMetadatas {
					pipesAfterFilter := stagedMetadata.Pipelines[:0]
					for _, pipe := range stagedMetadata.Pipelines {
						storagePoliciesAfterFilter := pipe.StoragePolicies[:0]
						for _, sp := range pipe.StoragePolicies {
							// Check aggregation for storage policy not already
							// set by a mapping rule.
							matchedByMappingRule := false
							for _, existing := range a.mappingRuleStoragePolicies {
								if sp.Equivalent(existing) {
									matchedByMappingRule = true
									a.debugLogMatch("downsampler skipping default mapping rule storage policy",
										debugLogMatchOptions{Meta: stagedMetadataBeforeFilter})
									break
								}
							}
							if !matchedByMappingRule {
								// Keep storage policy if not matched by mapping rule.
								storagePoliciesAfterFilter =
									append(storagePoliciesAfterFilter, sp)
							}
						}

						// Update storage policies slice after filtering.
						pipe.StoragePolicies = storagePoliciesAfterFilter

						if len(pipe.StoragePolicies) != 0 {
							// Keep storage policy if still has some storage policies.
							pipesAfterFilter = append(pipesAfterFilter, pipe)
						}
					}

					// Update pipelnes after filtering.
					stagedMetadata.Pipelines = pipesAfterFilter

					if len(stagedMetadata.Pipelines) != 0 {
						// Keep staged metadata if still has some pipelines.
						stagedMetadatasAfterFilter =
							append(stagedMetadatasAfterFilter, stagedMetadata)
					}
				}

				// Finally set the staged metadatas we're keeping
				// as those that were kept after filtering.
				stagedMetadatas = stagedMetadatasAfterFilter
			}

			// Now skip appending if after filtering there's no staged metadatas
			// after any filtering that's applied.
			if len(stagedMetadatas) == 0 {
				a.debugLogMatch("downsampler skipping default mapping rule completely",
					debugLogMatchOptions{Meta: stagedMetadataBeforeFilter})
				continue
			}

			a.debugLogMatch("downsampler applying default mapping rule",
				debugLogMatchOptions{Meta: stagedMetadatas})

			pipelines := stagedMetadatas[len(stagedMetadatas)-1]
			a.curr.Pipelines =
				append(a.curr.Pipelines, pipelines.Pipelines...)
		}

		// Apply drop policies results
		a.curr.Pipelines, dropApplyResult = a.curr.Pipelines.ApplyOrRemoveDropPolicies()

		if len(a.curr.Pipelines) > 0 && !a.curr.IsDropPolicyApplied() {
			// Send to downsampler if we have something in the pipeline.
			a.debugLogMatch("downsampler using built mapping staged metadatas",
				debugLogMatchOptions{Meta: []metadata.StagedMetadata{a.curr}})

			if err := a.addSamplesAppenders(tags, a.curr); err != nil {
				return SamplesAppenderResult{}, err
			}
		}

		numRollups := matchResult.NumNewRollupIDs()
		for i := 0; i < numRollups; i++ {
			rollup := matchResult.ForNewRollupIDsAt(i, nowNanos)

			a.debugLogMatch("downsampler applying matched rollup rule",
				debugLogMatchOptions{Meta: rollup.Metadatas, RollupID: rollup.ID})
			a.multiSamplesAppender.addSamplesAppender(samplesAppender{
				agg:             a.agg,
				clientRemote:    a.clientRemote,
				unownedID:       rollup.ID,
				stagedMetadatas: rollup.Metadatas,
			})
		}
	}

	dropPolicyApplied := dropApplyResult != metadata.NoDropPolicyPresentResult
	return SamplesAppenderResult{
		SamplesAppender:     a.multiSamplesAppender,
		IsDropPolicyApplied: dropPolicyApplied,
	}, nil
}

type debugLogMatchOptions struct {
	Meta          metadata.StagedMetadatas
	StoragePolicy policy.StoragePolicy
	RollupID      []byte
}

func (a *metricsAppender) debugLogMatch(str string, opts debugLogMatchOptions) {
	if !a.debugLogging {
		return
	}
	fields := []zapcore.Field{
		zap.String("tags", a.originalTags.String()),
	}
	if v := opts.RollupID; v != nil {
		fields = append(fields, zap.ByteString("rollupID", v))
	}
	if v := opts.Meta; v != nil {
		fields = append(fields, stagedMetadatasLogField(v))
	}
	if v := opts.StoragePolicy; v != policy.EmptyStoragePolicy {
		fields = append(fields, zap.Stringer("storagePolicy", v))
	}
	a.logger.Debug(str, fields...)
}

func (a *metricsAppender) NextMetric() {
	// Move the inuse encoders to cached as we should be done with using them.
	a.resetEncoders()
	a.resetTags()
}

func (a *metricsAppender) Finalize() {
	// Return to pool.
	a.pool.Put(a)
}

func (a *metricsAppender) tagEncoder() serialize.TagEncoder {
	// Take an encoder from the cached encoder list, if not present get one
	// from the pool. Add the returned encoder to the used list.
	var tagEncoder serialize.TagEncoder
	if len(a.cachedEncoders) == 0 {
		tagEncoder = a.tagEncoderPool.Get()
	} else {
		l := len(a.cachedEncoders)
		tagEncoder = a.cachedEncoders[l-1]
		a.cachedEncoders = a.cachedEncoders[:l-1]
	}
	a.inuseEncoders = append(a.inuseEncoders, tagEncoder)
	tagEncoder.Reset()
	return tagEncoder
}

func (a *metricsAppender) tags() *tags {
	// Take an encoder from the cached encoder list, if not present get one
	// from the pool. Add the returned encoder to the used list.
	var t *tags
	if len(a.cachedTags) == 0 {
		t = newTags()
	} else {
		l := len(a.cachedTags)
		t = a.cachedTags[l-1]
		a.cachedTags = a.cachedTags[:l-1]
	}
	a.inuseTags = append(a.inuseTags, t)
	t.names = t.names[:0]
	t.values = t.values[:0]
	t.reset()
	return t
}

func (a *metricsAppender) resetEncoders() {
	a.cachedEncoders = append(a.cachedEncoders, a.inuseEncoders...)
	for i := range a.inuseEncoders {
		a.inuseEncoders[i] = nil
	}
	a.inuseEncoders = a.inuseEncoders[:0]
}

func (a *metricsAppender) resetTags() {
	a.cachedTags = append(a.cachedTags, a.inuseTags...)
	for i := range a.inuseTags {
		a.inuseTags[i] = nil
	}
	a.inuseTags = a.inuseTags[:0]
	a.originalTags = nil
}

func (a *metricsAppender) addSamplesAppenders(originalTags *tags, stagedMetadata metadata.StagedMetadata) error {
	var (
		pipelines []metadata.PipelineMetadata
	)
	for _, pipeline := range stagedMetadata.Pipelines {
		// For pipeline which have tags to augment we generate and send
		// separate IDs. Other pipelines return the same.
		pipeline := pipeline
		if len(pipeline.Tags) == 0 && len(pipeline.GraphitePrefix) == 0 {
			pipelines = append(pipelines, pipeline)
			continue
		}

		tags := a.augmentTags(originalTags, pipeline.GraphitePrefix, pipeline.Tags, pipeline.AggregationID)

		sm := stagedMetadata
		sm.Pipelines = []metadata.PipelineMetadata{pipeline}

		appender, err := a.newSamplesAppender(tags, sm)
		if err != nil {
			return err
		}
		a.multiSamplesAppender.addSamplesAppender(appender)
	}

	if len(pipelines) == 0 {
		return nil
	}

	sm := stagedMetadata
	sm.Pipelines = pipelines

	appender, err := a.newSamplesAppender(originalTags, sm)
	if err != nil {
		return err
	}
	a.multiSamplesAppender.addSamplesAppender(appender)
	return nil
}

func (a *metricsAppender) newSamplesAppender(
	tags *tags,
	sm metadata.StagedMetadata,
) (samplesAppender, error) {
	tagEncoder := a.tagEncoder()
	if err := tagEncoder.Encode(tags); err != nil {
		return samplesAppender{}, err
	}
	data, ok := tagEncoder.Data()
	if !ok {
		return samplesAppender{}, fmt.Errorf("unable to encode tags: names=%v, values=%v", tags.names, tags.values)
	}
	return samplesAppender{
		agg:             a.agg,
		clientRemote:    a.clientRemote,
		unownedID:       data.Bytes(),
		stagedMetadatas: []metadata.StagedMetadata{sm},
	}, nil
}

func (a *metricsAppender) augmentTags(
	originalTags *tags,
	graphitePrefix [][]byte,
	t []models.Tag,
	id aggregation.ID,
) *tags {
	// Create the prefix tags if any.
	tags := a.tags()
	for i, path := range graphitePrefix {
		// Add the graphite prefix as the initial graphite tags.
		tags.append(graphite.TagName(i), path)
	}

	// Make a copy of the tags to augment.
	prefixes := len(graphitePrefix)
	for i := range originalTags.names {
		// If we applied prefixes then we need to parse and modify the original
		// tags. Check if the original tag was graphite type, if so add the
		// number of prefixes to the tag index and update.
		var (
			name  = originalTags.names[i]
			value = originalTags.values[i]
		)
		if prefixes > 0 {
			// If the tag seen is a graphite tag then offset it based on number
			// of prefixes we have seen.
			if index, ok := graphite.TagIndex(name); ok {
				name = graphite.TagName(index + prefixes)
			}
		}
		tags.append(name, value)
	}

	// Add any additional tags we need to.
	for _, tag := range t {
		// If the tag is not special tag, then just add it.
		if !bytes.HasPrefix(tag.Name, metric.M3MetricsPrefix) {
			if len(tag.Name) > 0 && len(tag.Value) > 0 {
				tags.append(tag.Name, tag.Value)
			}
			continue
		}

		// Handle m3 special tags.
		if bytes.Equal(tag.Name, metric.M3MetricsGraphiteAggregation) {
			// Add the aggregation tag as the last graphite tag.
			types, err := id.Types()
			if err != nil || len(types) == 0 {
				continue
			}
			var (
				count = tags.countPrefix(graphite.Prefix)
				name  = graphite.TagName(count)
				value = types[0].Name()
			)
			tags.append(name, value)
		}
	}
	return tags
}

func stagedMetadatasLogField(sm metadata.StagedMetadatas) zapcore.Field {
	json, err := stagedMetadatasJSON(sm)
	if err != nil {
		return zap.String("stagedMetadatasDebugErr", err.Error())
	}
	return zap.Any("stagedMetadatas", json)
}

func stagedMetadatasJSON(sm metadata.StagedMetadatas) (interface{}, error) {
	var pb metricpb.StagedMetadatas
	if err := sm.ToProto(&pb); err != nil {
		return nil, err
	}
	var buff bytes.Buffer
	if err := (&jsonpb.Marshaler{}).Marshal(&buff, &pb); err != nil {
		return nil, err
	}
	var result map[string]interface{}
	if err := json.Unmarshal(buff.Bytes(), &result); err != nil {
		return nil, err
	}
	return result, nil
}
