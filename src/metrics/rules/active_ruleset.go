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

package rules

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/metadata"
	"github.com/m3db/m3metrics/metric"
	metricID "github.com/m3db/m3metrics/metric/id"
	mpipeline "github.com/m3db/m3metrics/pipeline"
	"github.com/m3db/m3metrics/pipeline/applied"
	xerrors "github.com/m3db/m3x/errors"
)

// Matcher matches metrics against rules to determine applicable policies.
type Matcher interface {
	// ForwardMatch matches the applicable policies for a metric id between [fromNanos, toNanos).
	ForwardMatch(id []byte, fromNanos, toNanos int64) MatchResult

	// ReverseMatch reverse matches the applicable policies for a metric id between [fromNanos, toNanos),
	// with aware of the metric type and aggregation type for the given id.
	ReverseMatch(id []byte, fromNanos, toNanos int64, mt metric.Type, at aggregation.Type) MatchResult
}

type activeRuleSet struct {
	version         int
	mappingRules    []*mappingRule
	rollupRules     []*rollupRule
	cutoverTimesAsc []int64
	tagsFilterOpts  filters.TagsFilterOptions
	newRollupIDFn   metricID.NewIDFn
	isRollupIDFn    metricID.MatchIDFn
	aggTypeOpts     aggregation.TypesOptions
}

func newActiveRuleSet(
	version int,
	mappingRules []*mappingRule,
	rollupRules []*rollupRule,
	tagsFilterOpts filters.TagsFilterOptions,
	newRollupIDFn metricID.NewIDFn,
	isRollupIDFn metricID.MatchIDFn,
	aggOpts aggregation.TypesOptions,
) *activeRuleSet {
	uniqueCutoverTimes := make(map[int64]struct{})
	for _, mappingRule := range mappingRules {
		for _, snapshot := range mappingRule.snapshots {
			uniqueCutoverTimes[snapshot.cutoverNanos] = struct{}{}
		}
	}
	for _, rollupRule := range rollupRules {
		for _, snapshot := range rollupRule.snapshots {
			uniqueCutoverTimes[snapshot.cutoverNanos] = struct{}{}
		}
	}

	cutoverTimesAsc := make([]int64, 0, len(uniqueCutoverTimes))
	for t := range uniqueCutoverTimes {
		cutoverTimesAsc = append(cutoverTimesAsc, t)
	}
	sort.Sort(int64Asc(cutoverTimesAsc))

	return &activeRuleSet{
		version:         version,
		mappingRules:    mappingRules,
		rollupRules:     rollupRules,
		cutoverTimesAsc: cutoverTimesAsc,
		tagsFilterOpts:  tagsFilterOpts,
		newRollupIDFn:   newRollupIDFn,
		isRollupIDFn:    isRollupIDFn,
		aggTypeOpts:     aggOpts,
	}
}

// The forward matching logic goes like this:
//
// Imagine you have the list of rules in the ruleset lined up vertically. Each rule may have one
// or more snapshots, each of which represents a change to that rule (e.g., filter change, policy
// change, etc.). These snapshots are naturally non-overlapping in time since only one snapshot
// can be active at a given point in time. As a result, if we use the x axis to represent time,
// then for each rule, a snapshot is active for some amount of time. IOW, if you pick a time and
// draw a vertical line across the set of rules, the snapshots of different ruels that intersect
// with the vertical line are the active rule snapshots for the ruleset.
//
// Now you have a list of times you need to perform rule matching at. Each matching time
// corresponds to a cutover time of a rule in the ruleset, because that's when matching the metric
// ID against this rule may lead to a different metadata including different storage policies and
// new rollup IDs to be generated or existing rollup IDs to stop being generated. The final match
// result is a collection of such metadata sorted by time in ascending order.
//
// NB(xichen): can further consolidate consecutive staged metadata to deduplicate.
func (as *activeRuleSet) ForwardMatch(
	id []byte,
	fromNanos, toNanos int64,
) MatchResult {
	var (
		currMatchRes     = as.forwardMatchAt(id, fromNanos)
		forExistingID    = metadata.StagedMetadatas{currMatchRes.forExistingID}
		forNewRollupIDs  = currMatchRes.forNewRollupIDs
		nextIdx          = as.nextCutoverIdx(fromNanos)
		nextCutoverNanos = as.cutoverNanosAt(nextIdx)
	)
	for nextIdx < len(as.cutoverTimesAsc) && nextCutoverNanos < toNanos {
		nextMatchRes := as.forwardMatchAt(id, nextCutoverNanos)
		forExistingID = mergeResultsForExistingID(forExistingID, nextMatchRes.forExistingID, nextCutoverNanos)
		forNewRollupIDs = mergeResultsForNewRollupIDs(forNewRollupIDs, nextMatchRes.forNewRollupIDs, nextCutoverNanos)
		nextIdx++
		nextCutoverNanos = as.cutoverNanosAt(nextIdx)
	}

	// The result expires when the beginning of the match time range reaches the first cutover time
	// after `fromNanos`, or the end of the match time range reaches the first cutover time after
	// `toNanos` among all active rules because the metric may then be matched against a different
	// set of rules.
	return NewMatchResult(as.version, nextCutoverNanos, forExistingID, forNewRollupIDs)
}

// TODO(xichen): look into whether we should simply pass in the aggregation type options
// here as opposed to setting it in the active ruleset options.
func (as *activeRuleSet) ReverseMatch(
	id []byte,
	fromNanos, toNanos int64,
	mt metric.Type,
	at aggregation.Type,
) MatchResult {
	var (
		nextIdx          = as.nextCutoverIdx(fromNanos)
		nextCutoverNanos = as.cutoverNanosAt(nextIdx)
		forExistingID    metadata.StagedMetadatas
		isRollupID       bool
	)

	// Determine whether the ID is a rollup metric ID.
	name, tags, err := as.tagsFilterOpts.NameAndTagsFn(id)
	if err == nil {
		isRollupID = as.isRollupIDFn(name, tags)
	}

	if currForExistingID, found := as.reverseMappingsFor(id, name, tags, isRollupID, fromNanos, mt, at); found {
		forExistingID = mergeResultsForExistingID(forExistingID, currForExistingID, fromNanos)
	}
	for nextIdx < len(as.cutoverTimesAsc) && nextCutoverNanos < toNanos {
		if nextForExistingID, found := as.reverseMappingsFor(id, name, tags, isRollupID, nextCutoverNanos, mt, at); found {
			forExistingID = mergeResultsForExistingID(forExistingID, nextForExistingID, nextCutoverNanos)
		}
		nextIdx++
		nextCutoverNanos = as.cutoverNanosAt(nextIdx)
	}
	return NewMatchResult(as.version, nextCutoverNanos, forExistingID, nil)
}

// NB(xichen): can further consolidate pipelines with the same aggregation ID
// and same applied pipeline but different storage policies to reduce amount of
// data that needed to be stored in memory and sent across the wire.
func (as *activeRuleSet) forwardMatchAt(
	id []byte,
	timeNanos int64,
) forwardMatchResult {
	mappingResults := as.mappingsForNonRollupID(id, timeNanos)
	rollupResults := as.rollupResultsFor(id, timeNanos)
	forExistingID := mappingResults.forExistingID.
		merge(rollupResults.forExistingID).
		unique().
		toStagedMetadata()
	forNewRollupIDs := make([]IDWithMetadatas, 0, len(rollupResults.forNewRollupIDs))
	for _, idWithMatchResult := range rollupResults.forNewRollupIDs {
		stagedMetadata := idWithMatchResult.matchResults.unique().toStagedMetadata()
		newIDWithMetadatas := IDWithMetadatas{
			ID:        idWithMatchResult.id,
			Metadatas: metadata.StagedMetadatas{stagedMetadata},
		}
		forNewRollupIDs = append(forNewRollupIDs, newIDWithMetadatas)
	}
	sort.Sort(IDWithMetadatasByIDAsc(forNewRollupIDs))
	return forwardMatchResult{
		forExistingID:   forExistingID,
		forNewRollupIDs: forNewRollupIDs,
	}
}

func (as *activeRuleSet) mappingsForNonRollupID(
	id []byte,
	timeNanos int64,
) mappingResults {
	var (
		cutoverNanos int64
		pipelines    []metadata.PipelineMetadata
	)
	for _, mappingRule := range as.mappingRules {
		snapshot := mappingRule.activeSnapshot(timeNanos)
		if snapshot == nil {
			continue
		}
		if !snapshot.filter.Matches(id) {
			continue
		}
		// Make sure the cutover time tracks the latest cutover time among all matching
		// mapping rules to represent the correct time of rule change.
		if cutoverNanos < snapshot.cutoverNanos {
			cutoverNanos = snapshot.cutoverNanos
		}
		// If the mapping rule snapshot is a tombstoned snapshot, its cutover time is
		// recorded to indicate a rule change, but its policies are no longer in effect.
		if snapshot.tombstoned {
			continue
		}
		pipeline := metadata.PipelineMetadata{
			AggregationID:   snapshot.aggregationID,
			StoragePolicies: snapshot.storagePolicies.Clone(),
		}
		pipelines = append(pipelines, pipeline)
	}
	// NB: The pipeline list should never be empty as the resulting pipelines are
	// used to determine how the *existing* ID is aggregated and retained. If there
	// are no rule match, the default pipeline list is used.
	if len(pipelines) == 0 {
		pipelines = metadata.DefaultPipelineMetadatas.Clone()
	}
	return mappingResults{
		forExistingID: ruleMatchResults{cutoverNanos: cutoverNanos, pipelines: pipelines},
	}
}

func (as *activeRuleSet) rollupResultsFor(id []byte, timeNanos int64) rollupResults {
	var (
		cutoverNanos  int64
		rollupTargets []rollupTarget
	)
	for _, rollupRule := range as.rollupRules {
		snapshot := rollupRule.activeSnapshot(timeNanos)
		if snapshot == nil {
			continue
		}
		if !snapshot.filter.Matches(id) {
			continue
		}
		// Make sure the cutover time tracks the latest cutover time among all matching
		// rollup rules to represent the correct time of rule change.
		if cutoverNanos < snapshot.cutoverNanos {
			cutoverNanos = snapshot.cutoverNanos
		}
		// If the rollup rule snapshot is a tombstoned snapshot, its cutover time is
		// recorded to indicate a rule change, but its rollup targets are no longer in effect.
		if snapshot.tombstoned {
			continue
		}
		for _, target := range snapshot.targets {
			rollupTargets = append(rollupTargets, target.clone())
		}
	}
	// NB: could log the matching error here if needed.
	res, _ := as.toRollupResults(id, cutoverNanos, rollupTargets)
	return res
}

// toRollupMatchResult applies the rollup operation in each rollup pipelines contained
// in the rollup targets against the matching ID to determine the resulting new rollup
// ID. It additionally distinguishes rollup pipelines whose first operation is a rollup
// operation from those that aren't since the former pipelines are applied against the
// original metric ID and the latter are applied against new rollup IDs due to the
// application of the rollup operation.
// nolint: unparam
func (as *activeRuleSet) toRollupResults(
	id []byte,
	cutoverNanos int64,
	targets []rollupTarget,
) (rollupResults, error) {
	if len(targets) == 0 {
		return rollupResults{}, nil
	}

	// If we cannot extract tags from the id, this is likely an invalid
	// metric and we bail early.
	_, sortedTagPairBytes, err := as.tagsFilterOpts.NameAndTagsFn(id)
	if err != nil {
		return rollupResults{}, err
	}

	var (
		multiErr           = xerrors.NewMultiError()
		pipelines          = make([]metadata.PipelineMetadata, 0, len(targets))
		newRollupIDResults = make([]idWithMatchResults, 0, len(targets))
		tagPairs           []metricID.TagPair
	)

	for _, target := range targets {
		pipeline := target.Pipeline
		// A rollup target should always have a non-empty pipeline but
		// just being defensive here.
		if pipeline.IsEmpty() {
			err = fmt.Errorf("target %v has empty pipeline", target)
			multiErr = multiErr.Add(err)
			continue
		}
		var (
			aggregationID aggregation.ID
			rollupID      []byte
			numSteps      = pipeline.Len()
			firstOp       = pipeline.At(0)
			toApply       mpipeline.Pipeline
		)
		switch firstOp.Type {
		case mpipeline.AggregationOpType:
			aggregationID, err = aggregation.CompressTypes(firstOp.Aggregation.Type)
			if err != nil {
				err = fmt.Errorf("target %v operation 0 aggregation type compression error: %v", target, err)
				multiErr = multiErr.Add(err)
				continue
			}
			toApply = pipeline.SubPipeline(1, numSteps)
		case mpipeline.TransformationOpType:
			aggregationID = aggregation.DefaultID
			toApply = pipeline
		case mpipeline.RollupOpType:
			tagPairs = tagPairs[:0]
			var matched bool
			rollupID, matched = as.matchRollupTarget(
				sortedTagPairBytes,
				firstOp.Rollup.NewName,
				firstOp.Rollup.Tags,
				tagPairs,
				matchRollupTargetOptions{generateRollupID: true},
			)
			if !matched {
				// The incoming metric ID did not match the rollup target.
				continue
			}
			aggregationID = firstOp.Rollup.AggregationID
			toApply = pipeline.SubPipeline(1, numSteps)
		default:
			err = fmt.Errorf("target %v operation 0 has unknown type: %v", target, firstOp.Type)
			multiErr = multiErr.Add(err)
			continue
		}
		tagPairs = tagPairs[:0]
		applied, err := as.applyIDToPipeline(sortedTagPairBytes, toApply, tagPairs)
		if err != nil {
			err = fmt.Errorf("failed to apply id %s to pipeline %v: %v", id, toApply, err)
			multiErr = multiErr.Add(err)
			continue
		}
		newPipeline := metadata.PipelineMetadata{
			AggregationID:   aggregationID,
			StoragePolicies: target.StoragePolicies,
			Pipeline:        applied,
		}
		if rollupID == nil {
			// The applied pipeline applies to the incoming ID.
			pipelines = append(pipelines, newPipeline)
		} else {
			// The applied pipeline applies to a new rollup ID.
			matchResults := ruleMatchResults{
				cutoverNanos: cutoverNanos,
				pipelines:    []metadata.PipelineMetadata{newPipeline},
			}
			newRollupIDResult := idWithMatchResults{id: rollupID, matchResults: matchResults}
			newRollupIDResults = append(newRollupIDResults, newRollupIDResult)
		}
	}

	return rollupResults{
		forExistingID:   ruleMatchResults{cutoverNanos: cutoverNanos, pipelines: pipelines},
		forNewRollupIDs: newRollupIDResults,
	}, multiErr.FinalError()
}

// matchRollupTarget matches an incoming metric ID against a rollup target,
// returns the new rollup ID if the metric ID contains the full list of rollup
// tags, and nil otherwise.
func (as *activeRuleSet) matchRollupTarget(
	sortedTagPairBytes []byte,
	newName []byte,
	rollupTags [][]byte,
	tagPairs []metricID.TagPair, // buffer for reuse to generate rollup ID across calls
	opts matchRollupTargetOptions,
) ([]byte, bool) {
	var (
		sortedTagIter = as.tagsFilterOpts.SortedTagIteratorFn(sortedTagPairBytes)
		hasMoreTags   = sortedTagIter.Next()
		currTagIdx    = 0
	)
	for hasMoreTags && currTagIdx < len(rollupTags) {
		tagName, tagVal := sortedTagIter.Current()
		res := bytes.Compare(tagName, rollupTags[currTagIdx])
		if res == 0 {
			if opts.generateRollupID {
				tagPairs = append(tagPairs, metricID.TagPair{Name: tagName, Value: tagVal})
			}
			currTagIdx++
			hasMoreTags = sortedTagIter.Next()
			continue
		}
		// If one of the target tags is not found in the ID, this is considered
		// a non-match so bail immediately.
		if res > 0 {
			break
		}
		hasMoreTags = sortedTagIter.Next()
	}
	sortedTagIter.Close()

	// If not all the target tags are found, this is considered a no match.
	if currTagIdx < len(rollupTags) {
		return nil, false
	}
	if !opts.generateRollupID {
		return nil, true
	}
	return as.newRollupIDFn(newName, tagPairs), true
}

func (as *activeRuleSet) applyIDToPipeline(
	sortedTagPairBytes []byte,
	pipeline mpipeline.Pipeline,
	tagPairs []metricID.TagPair, // buffer for reuse across calls
) (applied.Pipeline, error) {
	operations := make([]applied.OpUnion, 0, pipeline.Len())
	for i := 0; i < pipeline.Len(); i++ {
		pipelineOp := pipeline.At(i)
		var opUnion applied.OpUnion
		switch pipelineOp.Type {
		case mpipeline.TransformationOpType:
			opUnion = applied.OpUnion{
				Type:           mpipeline.TransformationOpType,
				Transformation: pipelineOp.Transformation,
			}
		case mpipeline.RollupOpType:
			rollupOp := pipelineOp.Rollup
			var matched bool
			rollupID, matched := as.matchRollupTarget(
				sortedTagPairBytes,
				rollupOp.NewName,
				rollupOp.Tags,
				tagPairs,
				matchRollupTargetOptions{generateRollupID: true},
			)
			if !matched {
				err := fmt.Errorf("existing tag pairs %s do not contain all rollup tags %s", sortedTagPairBytes, rollupOp.Tags)
				return applied.Pipeline{}, err
			}
			opUnion = applied.OpUnion{
				Type:   mpipeline.RollupOpType,
				Rollup: applied.RollupOp{ID: rollupID, AggregationID: rollupOp.AggregationID},
			}
		default:
			return applied.Pipeline{}, fmt.Errorf("unexpected pipeline op type: %v", pipelineOp.Type)
		}
		operations = append(operations, opUnion)
	}
	return applied.NewPipeline(operations), nil
}

func (as *activeRuleSet) reverseMappingsFor(
	id, name, tags []byte,
	isRollupID bool,
	timeNanos int64,
	mt metric.Type,
	at aggregation.Type,
) (metadata.StagedMetadata, bool) {
	if !isRollupID {
		return as.reverseMappingsForNonRollupID(id, timeNanos, mt, at)
	}
	return as.reverseMappingsForRollupID(name, tags, timeNanos, mt, at)
}

// reverseMappingsForNonRollupID returns the staged metadata for the given non-rollup ID at
// the given time, and true if a non-empty list of pipelines are found, and false otherwise.
func (as *activeRuleSet) reverseMappingsForNonRollupID(
	id []byte,
	timeNanos int64,
	mt metric.Type,
	at aggregation.Type,
) (metadata.StagedMetadata, bool) {
	mappingRes := as.mappingsForNonRollupID(id, timeNanos).forExistingID
	filteredPipelines := filteredPipelinesWithAggregationType(mappingRes.pipelines, mt, at, as.aggTypeOpts)
	if len(filteredPipelines) == 0 {
		return metadata.DefaultStagedMetadata, false
	}
	return metadata.StagedMetadata{
		CutoverNanos: mappingRes.cutoverNanos,
		Tombstoned:   false,
		Metadata:     metadata.Metadata{Pipelines: filteredPipelines},
	}, true
}

// NB(xichen): in order to determine the applicable policies for a rollup metric, we need to
// match the id against rollup rules to determine which rollup rules are applicable, under the
// assumption that no two rollup targets in the same namespace may have the same rollup metric
// name and the list of rollup tags. Otherwise, a rollup metric could potentially match more
// than one rollup rule with different policies even though only one of the matched rules was
// used to produce the given rollup metric id due to its tag filters, thereby causing the wrong
// staged policies to be returned. This also implies at any given time, at most one rollup target
// may match the given rollup id.
// Since we may have rollup pipelines with different aggregation types defined for a roll up rule,
// and each aggregation type would generate a new id. So when doing reverse mapping, not only do
// we need to match the roll up tags, we also need to check the aggregation type against
// each rollup pipeline to see if the aggregation type was actually contained in the pipeline.
func (as *activeRuleSet) reverseMappingsForRollupID(
	name, sortedTagPairBytes []byte,
	timeNanos int64,
	mt metric.Type,
	at aggregation.Type,
) (metadata.StagedMetadata, bool) {
	for _, rollupRule := range as.rollupRules {
		snapshot := rollupRule.activeSnapshot(timeNanos)
		if snapshot == nil || snapshot.tombstoned {
			continue
		}
		for _, target := range snapshot.targets {
			for i := 0; i < target.Pipeline.Len(); i++ {
				pipelineOp := target.Pipeline.At(i)
				if pipelineOp.Type != mpipeline.RollupOpType {
					continue
				}
				rollupOp := pipelineOp.Rollup
				if !bytes.Equal(rollupOp.NewName, name) {
					continue
				}
				if _, matched := as.matchRollupTarget(
					sortedTagPairBytes,
					rollupOp.NewName,
					rollupOp.Tags,
					nil,
					matchRollupTargetOptions{generateRollupID: false},
				); !matched {
					continue
				}
				// NB: the list of pipeline steps is not important and thus not computed and returned.
				pipeline := metadata.PipelineMetadata{
					AggregationID:   rollupOp.AggregationID,
					StoragePolicies: target.StoragePolicies.Clone(),
				}
				filteredPipelines := filteredPipelinesWithAggregationType([]metadata.PipelineMetadata{pipeline}, mt, at, as.aggTypeOpts)
				if len(filteredPipelines) == 0 {
					return metadata.DefaultStagedMetadata, false
				}
				return metadata.StagedMetadata{
					CutoverNanos: snapshot.cutoverNanos,
					Tombstoned:   false,
					Metadata:     metadata.Metadata{Pipelines: filteredPipelines},
				}, true
			}
		}
	}
	return metadata.DefaultStagedMetadata, false
}

// nextCutoverIdx returns the next snapshot index whose cutover time is after t.
// NB(xichen): not using sort.Search to avoid a lambda capture.
func (as *activeRuleSet) nextCutoverIdx(t int64) int {
	i, j := 0, len(as.cutoverTimesAsc)
	for i < j {
		h := i + (j-i)/2
		if as.cutoverTimesAsc[h] <= t {
			i = h + 1
		} else {
			j = h
		}
	}
	return i
}

// cutoverNanosAt returns the cutover time at given index.
func (as *activeRuleSet) cutoverNanosAt(idx int) int64 {
	if idx < len(as.cutoverTimesAsc) {
		return as.cutoverTimesAsc[idx]
	}
	return timeNanosMax
}

// filterByAggregationType takes a list of pipelines as input and returns those
// containing the given aggregation type.
func filteredPipelinesWithAggregationType(
	pipelines []metadata.PipelineMetadata,
	mt metric.Type,
	at aggregation.Type,
	opts aggregation.TypesOptions,
) []metadata.PipelineMetadata {
	var cur int
	for i := 0; i < len(pipelines); i++ {
		var containsAggType bool
		if aggID := pipelines[i].AggregationID; aggID.IsDefault() {
			containsAggType = opts.IsContainedInDefaultAggregationTypes(at, mt)
		} else {
			containsAggType = aggID.Contains(at)
		}
		if !containsAggType {
			continue
		}
		if cur != i {
			pipelines[cur] = pipelines[i]
		}
		cur++
	}
	return pipelines[:cur]
}

// mergeResultsForExistingID merges the next staged metadata into the current list of staged
// metadatas while ensuring the cutover times of the staged metadatas are non-decreasing. This
// is needed because the cutover times of staged metadata results produced by mapping rule matching
// may not always be in ascending order. For example, if at time T0 a metric matches against a
// mapping rule, and the filter of such rule changed at T1 such that the metric no longer matches
// the rule, this would indicate the staged metadata at T0 would have a cutover time of T0,
// whereas the staged metadata at T1 would have a cutover time of 0 (due to no rule match),
// in which case we need to set the cutover time of the staged metadata at T1 to T1 to ensure
// the mononicity of cutover times.
func mergeResultsForExistingID(
	currMetadatas metadata.StagedMetadatas,
	nextMetadata metadata.StagedMetadata,
	nextCutoverNanos int64,
) metadata.StagedMetadatas {
	if len(currMetadatas) == 0 {
		return metadata.StagedMetadatas{nextMetadata}
	}
	currCutoverNanos := currMetadatas[len(currMetadatas)-1].CutoverNanos
	if currCutoverNanos > nextMetadata.CutoverNanos {
		nextMetadata.CutoverNanos = nextCutoverNanos
	}
	currMetadatas = append(currMetadatas, nextMetadata)
	return currMetadatas
}

// mergeResultsForNewRollupIDs merges the current list of staged metadatas for new rollup IDs
// with the list of staged metadatas for new rollup IDs at the next rule cutover time, assuming
// that both the current metadatas list and the next metadatas list are sorted by rollup IDs
// in ascending order.
// NB: each item in the `nextResults` array has a single staged metadata in the `metadatas` array
// as the staged metadata for the associated rollup ID at the next cutover time.
func mergeResultsForNewRollupIDs(
	currResults []IDWithMetadatas,
	nextResults []IDWithMetadatas,
	nextCutoverNanos int64,
) []IDWithMetadatas {
	var (
		currLen, nextLen = len(currResults), len(nextResults)
		currIdx, nextIdx int
	)
	for currIdx < currLen || nextIdx < nextLen {
		var compareResult int
		if currIdx >= currLen {
			compareResult = 1
		} else if nextIdx >= nextLen {
			compareResult = -1
		} else {
			compareResult = bytes.Compare(currResults[currIdx].ID, nextResults[nextIdx].ID)
		}

		// If the current result and the next result have the same ID, we append the next metadata
		// to the end of the metadata list.
		if compareResult == 0 {
			currResults[currIdx].Metadatas = append(currResults[currIdx].Metadatas, nextResults[nextIdx].Metadatas[0])
			currIdx++
			nextIdx++
			continue
		}

		// If the current ID is smaller, it means the current rollup ID is tombstoned at the next
		// cutover time.
		if compareResult < 0 {
			tombstonedMetadata := metadata.StagedMetadata{CutoverNanos: nextCutoverNanos, Tombstoned: true}
			currResults[currIdx].Metadatas = append(currResults[currIdx].Metadatas, tombstonedMetadata)
			currIdx++
			continue
		}

		// Otherwise the current ID is larger, meaning a new ID is added at the next cutover time.
		currResults = append(currResults, nextResults[nextIdx])
		nextIdx++
	}
	sort.Sort(IDWithMetadatasByIDAsc(currResults))
	return currResults
}

type int64Asc []int64

func (a int64Asc) Len() int           { return len(a) }
func (a int64Asc) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a int64Asc) Less(i, j int) bool { return a[i] < a[j] }

type matchRollupTargetOptions struct {
	generateRollupID bool
}

type ruleMatchResults struct {
	cutoverNanos int64
	pipelines    []metadata.PipelineMetadata
}

// merge merges in another rule match results in place.
func (res *ruleMatchResults) merge(other ruleMatchResults) *ruleMatchResults {
	if res.cutoverNanos < other.cutoverNanos {
		res.cutoverNanos = other.cutoverNanos
	}
	res.pipelines = append(res.pipelines, other.pipelines...)
	return res
}

// unique de-duplicates the pipelines.
func (res *ruleMatchResults) unique() *ruleMatchResults {
	if len(res.pipelines) == 0 {
		return res
	}
	curr := 0
	for i := 1; i < len(res.pipelines); i++ {
		foundDup := false
		for j := 0; j <= curr; j++ {
			if res.pipelines[j].Equal(res.pipelines[i]) {
				foundDup = true
				break
			}
		}
		if foundDup {
			continue
		}
		curr++
		res.pipelines[curr] = res.pipelines[i]
	}
	for i := curr + 1; i < len(res.pipelines); i++ {
		res.pipelines[i] = metadata.PipelineMetadata{}
	}
	res.pipelines = res.pipelines[:curr+1]
	return res
}

// toStagedMetadata converts the match results to a staged metadata.
func (res *ruleMatchResults) toStagedMetadata() metadata.StagedMetadata {
	return metadata.StagedMetadata{
		CutoverNanos: res.cutoverNanos,
		Tombstoned:   false,
		Metadata:     metadata.Metadata{Pipelines: res.resolvedPipelines()},
	}
}

func (res *ruleMatchResults) resolvedPipelines() []metadata.PipelineMetadata {
	if len(res.pipelines) > 0 {
		return res.pipelines
	}
	return metadata.DefaultPipelineMetadatas
}

type idWithMatchResults struct {
	id           []byte
	matchResults ruleMatchResults
}

type mappingResults struct {
	// This represent the match result that should be applied against the
	// incoming metric ID the mapping rules were matched against.
	forExistingID ruleMatchResults
}

type rollupResults struct {
	// This represent the match result that should be applied against the
	// incoming metric ID the rollup rules were matched against. This usually contains
	// the match result produced by rollup rules containing rollup pipelines whose first
	// pipeline operation is not a rollup operation.
	forExistingID ruleMatchResults

	// This represents the match result that should be applied against new rollup
	// IDs generated during the rule matching process. This usually contains
	// the match result produced by rollup rules containing rollup pipelines whose first
	// pipeline operation is a rollup operation.
	forNewRollupIDs []idWithMatchResults
}

type forwardMatchResult struct {
	forExistingID   metadata.StagedMetadata
	forNewRollupIDs []IDWithMetadatas
}
