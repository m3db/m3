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
	"github.com/m3db/m3/src/metrics/generated/proto/metricpb"
	"github.com/m3db/m3/src/metrics/matcher"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/serialize"

	"github.com/golang/protobuf/jsonpb"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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

		a.multiSamplesAppender.addSamplesAppender(samplesAppender{
			agg:             a.agg,
			clientRemote:    a.clientRemote,
			unownedID:       unownedID,
			stagedMetadatas: []metadata.StagedMetadata{a.curr},
		})
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
			mappingStagedMetadatas := []metadata.StagedMetadata{a.curr}
			a.debugLogMatch("downsampler using built mapping staged metadatas",
				debugLogMatchOptions{Meta: mappingStagedMetadatas})

			a.multiSamplesAppender.addSamplesAppender(samplesAppender{
				agg:             a.agg,
				clientRemote:    a.clientRemote,
				unownedID:       unownedID,
				stagedMetadatas: mappingStagedMetadatas,
			})
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
