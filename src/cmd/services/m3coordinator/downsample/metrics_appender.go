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
	"fmt"
	"sort"
	"time"

	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/aggregator/client"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/generated/proto/metricpb"
	"github.com/m3db/m3/src/metrics/matcher"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/graphite/graphite"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/serialize"

	"github.com/golang/protobuf/jsonpb"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	m3MetricsPrefix              = []byte("__m3")
	m3MetricsGraphiteAggregation = []byte("__m3_graphite_aggregation__")
)

type metricsAppender struct {
	metricsAppenderOptions

	tags                         *tags
	multiSamplesAppender         *multiSamplesAppender
	curr                         metadata.StagedMetadata
	defaultStagedMetadatasCopies []metadata.StagedMetadatas
	mappingRuleStoragePolicies   []policy.StoragePolicy
}

// metricsAppenderOptions will have one of agg or clientRemote set.
type metricsAppenderOptions struct {
	agg          aggregator.Aggregator
	clientRemote client.Client

	defaultStagedMetadatasProtos []metricpb.StagedMetadatas
	tagEncoder                   serialize.TagEncoder
	matcher                      matcher.Matcher
	metricTagsIteratorPool       serialize.MetricTagsIteratorPool

	clockOpts    clock.Options
	debugLogging bool
	logger       *zap.Logger
}

func newMetricsAppender(opts metricsAppenderOptions) *metricsAppender {
	stagedMetadatasCopies := make([]metadata.StagedMetadatas,
		len(opts.defaultStagedMetadatasProtos))
	return &metricsAppender{
		metricsAppenderOptions:       opts,
		tags:                         newTags(),
		multiSamplesAppender:         newMultiSamplesAppender(),
		defaultStagedMetadatasCopies: stagedMetadatasCopies,
	}
}

func (a *metricsAppender) AddTag(name, value []byte) {
	a.tags.append(name, value)
}

func (a *metricsAppender) SamplesAppender(opts SampleAppenderOptions) (SamplesAppenderResult, error) {
	// Sort tags
	sort.Sort(a.tags)

	// Encode tags and compute a temporary (unowned) ID
	a.tagEncoder.Reset()
	if err := a.tagEncoder.Encode(a.tags); err != nil {
		return SamplesAppenderResult{}, err
	}
	data, ok := a.tagEncoder.Data()
	if !ok {
		return SamplesAppenderResult{}, fmt.Errorf("unable to encode tags: names=%v, values=%v",
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

	// Filter out the m3 custom tags once the ForwardMatch is complete. If
	// modified set the unowned ID to nil so that we know to recreate it later.
	if modified := a.tags.filterPrefix(m3MetricsPrefix); modified {
		unownedID = nil
	}

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

		appenders, err := a.newSamplesAppenders(a.curr)
		if err != nil {
			return SamplesAppenderResult{}, err
		}
		a.multiSamplesAppender.addSamplesAppenders(appenders)
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

		mappingRuleStagedMetadatas := matchResult.ForExistingIDAt(nowNanos)
		if !mappingRuleStagedMetadatas.IsDefault() && len(mappingRuleStagedMetadatas) != 0 {
			a.debugLogMatch("downsampler applying matched mapping rule",
				debugLogMatchOptions{Meta: mappingRuleStagedMetadatas})

			// Collect all the current active mapping rules
			for _, stagedMetadata := range mappingRuleStagedMetadatas {
				for _, pipe := range stagedMetadata.Pipelines {
					for _, sp := range pipe.StoragePolicies {
						a.mappingRuleStoragePolicies =
							append(a.mappingRuleStoragePolicies, sp)
					}
				}
			}

			// Only sample if going to actually aggregate
			pipelines := mappingRuleStagedMetadatas[len(mappingRuleStagedMetadatas)-1]
			a.curr.Pipelines =
				append(a.curr.Pipelines, pipelines.Pipelines...)
		}

		// Apply drop policies results
		a.curr.Pipelines, dropApplyResult = a.curr.Pipelines.ApplyOrRemoveDropPolicies()

		if len(a.curr.Pipelines) > 0 && !a.curr.IsDropPolicyApplied() {
			// Send to downsampler if we have something in the pipeline.
			a.debugLogMatch("downsampler using built mapping staged metadatas",
				debugLogMatchOptions{Meta: []metadata.StagedMetadata{a.curr}})

			appenders, err := a.newSamplesAppenders(a.curr)
			if err != nil {
				return SamplesAppenderResult{}, err
			}
			a.multiSamplesAppender.addSamplesAppenders(appenders)
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
		zap.String("tags", a.tags.String()),
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

func (a *metricsAppender) Reset() {
	a.tags.names = a.tags.names[:0]
	a.tags.values = a.tags.values[:0]
}

func (a *metricsAppender) Finalize() {
	a.tagEncoder.Finalize()
	a.tagEncoder = nil
}

func (a *metricsAppender) newSamplesAppenders(
	stagedMetadata metadata.StagedMetadata,
) ([]samplesAppender, error) {
	// Check if any pipelines have tags, if so
	appenders := make([]samplesAppender, 0, len(stagedMetadata.Pipelines))
	var pipelines []metadata.PipelineMetadata
	for _, pipeline := range stagedMetadata.Pipelines {
		// For pipeline which have tags to augment we generate and send
		// separate IDs. Other pipelines return the same.
		pipeline := pipeline
		if len(pipeline.Tags) == 0 {
			pipelines = append(pipelines, pipeline)
			continue
		}

		tags := newTags()
		for i := range a.tags.names {
			tags.append(a.tags.names[i], a.tags.values[i])
		}
		augmentTags(tags, pipeline.Tags, pipeline.AggregationID)

		sm := stagedMetadata
		sm.Pipelines = []metadata.PipelineMetadata{pipeline}

		appender, err := a.newSamplesAppender(tags, sm)
		if err != nil {
			return []samplesAppender{}, err
		}
		appenders = append(appenders, appender)
	}

	if len(pipelines) == 0 {
		return appenders, nil
	}

	sm := stagedMetadata
	sm.Pipelines = pipelines
	appender, err := a.newSamplesAppender(a.tags, sm)
	if err != nil {
		return []samplesAppender{}, err
	}
	return append(appenders, appender), nil
}

func (a *metricsAppender) newSamplesAppender(
	tags *tags,
	sm metadata.StagedMetadata,
) (samplesAppender, error) {
	a.tagEncoder.Reset()
	if err := a.tagEncoder.Encode(tags); err != nil {
		return samplesAppender{}, err
	}
	data, ok := a.tagEncoder.Data()
	if !ok {
		return samplesAppender{}, fmt.Errorf("unable to encode tags: names=%v, values=%v", tags.names, tags.values)
	}
	id := make([]byte, data.Len())
	copy(id, data.Bytes())
	return samplesAppender{
		agg:             a.agg,
		clientRemote:    a.clientRemote,
		unownedID:       id,
		stagedMetadatas: []metadata.StagedMetadata{sm},
	}, nil
}

func augmentTags(tags *tags, t []models.Tag, id aggregation.ID) bool {
	updated := false
	for _, tag := range t {
		if !bytes.HasPrefix(tag.Name, m3MetricsPrefix) {
			if len(tag.Name) > 0 && len(tag.Value) > 0 {
				tags.append(tag.Name, tag.Value)
				updated = true
			}
			continue
		}

		// Handle m3 special tags.
		if bytes.Equal(tag.Name, m3MetricsGraphiteAggregation) {
			types, err := id.Types()
			if err != nil || len(types) == 0 {
				continue
			}
			var (
				count = tags.countPrefix(graphite.Prefix)
				name  = graphite.TagName(count)
				value = types[0].Bytes()
			)
			tags.append(name, value)
			updated = true
		}
	}
	return updated
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
