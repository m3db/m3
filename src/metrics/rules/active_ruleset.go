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

package rules

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/filters"
	"github.com/m3db/m3/src/metrics/metadata"
	metricid "github.com/m3db/m3/src/metrics/metric/id"
	mpipeline "github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/pipeline/applied"
	"github.com/m3db/m3/src/metrics/rules/view"
	"github.com/m3db/m3/src/query/models"
	xerrors "github.com/m3db/m3/src/x/errors"
)

type activeRuleSet struct {
	version         int
	mappingRules    []*mappingRule
	rollupRules     []*rollupRule
	cutoverTimesAsc []int64
	tagsFilterOpts  filters.TagsFilterOptions
	newRollupIDFn   metricid.NewIDFn
}

func newActiveRuleSet(
	version int,
	mappingRules []*mappingRule,
	rollupRules []*rollupRule,
	tagsFilterOpts filters.TagsFilterOptions,
	newRollupIDFn metricid.NewIDFn) *activeRuleSet {
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
	id metricid.ID,
	fromNanos, toNanos int64,
	opts MatchOptions,
) (MatchResult, error) {
	currMatchRes, err := as.forwardMatchAt(id.Bytes(), fromNanos, opts)
	if err != nil {
		return MatchResult{}, err
	}
	var (
		forExistingID    = metadata.StagedMetadatas{currMatchRes.forExistingID}
		forNewRollupIDs  = currMatchRes.forNewRollupIDs
		nextIdx          = as.nextCutoverIdx(fromNanos)
		nextCutoverNanos = as.cutoverNanosAt(nextIdx)
		keepOriginal     = currMatchRes.keepOriginal
	)

	for nextIdx < len(as.cutoverTimesAsc) && nextCutoverNanos < toNanos {
		nextMatchRes, err := as.forwardMatchAt(id.Bytes(), nextCutoverNanos, opts)
		if err != nil {
			return MatchResult{}, err
		}
		forExistingID = mergeResultsForExistingID(forExistingID, nextMatchRes.forExistingID, nextCutoverNanos)
		forNewRollupIDs = mergeResultsForNewRollupIDs(forNewRollupIDs, nextMatchRes.forNewRollupIDs, nextCutoverNanos)
		nextIdx++
		nextCutoverNanos = as.cutoverNanosAt(nextIdx)
		keepOriginal = nextMatchRes.keepOriginal
	}

	// The result expires when the beginning of the match time range reaches the first cutover time
	// after `fromNanos`, or the end of the match time range reaches the first cutover time after
	// `toNanos` among all active rules because the metric may then be matched against a different
	// set of rules.
	return NewMatchResult(
		as.version,
		nextCutoverNanos,
		forExistingID,
		forNewRollupIDs,
		keepOriginal,
	), nil
}

// NB(xichen): can further consolidate pipelines with the same aggregation ID
// and same applied pipeline but different storage policies to reduce amount of
// data that needed to be stored in memory and sent across the wire.
func (as *activeRuleSet) forwardMatchAt(
	id []byte,
	timeNanos int64,
	matchOpts MatchOptions,
) (forwardMatchResult, error) {
	mappingResults, err := as.mappingsForNonRollupID(id, timeNanos, matchOpts)
	if err != nil {
		return forwardMatchResult{}, err
	}
	rollupResults, err := as.rollupResultsFor(id, timeNanos, matchOpts)
	if err != nil {
		return forwardMatchResult{}, err
	}
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
		keepOriginal:    rollupResults.keepOriginal,
	}, nil
}

func (as *activeRuleSet) mappingsForNonRollupID(
	id []byte,
	timeNanos int64,
	matchOpts MatchOptions,
) (mappingResults, error) {
	var (
		cutoverNanos int64
		pipelines    []metadata.PipelineMetadata
	)
	for _, mappingRule := range as.mappingRules {
		snapshot := mappingRule.activeSnapshot(timeNanos)
		if snapshot == nil {
			continue
		}
		matches, err := snapshot.filter.Matches(id, filters.TagMatchOptions{
			SortedTagIteratorFn: matchOpts.SortedTagIteratorFn,
			NameAndTagsFn:       matchOpts.NameAndTagsFn,
		})
		if err != nil {
			return mappingResults{}, err
		}
		if !matches {
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
			DropPolicy:      snapshot.dropPolicy,
			Tags:            snapshot.tags,
			GraphitePrefix:  snapshot.graphitePrefix,
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
	}, nil
}

func (as *activeRuleSet) LatestRollupRules(_ []byte, timeNanos int64) ([]view.RollupRule, error) {
	out := []view.RollupRule{}
	// Return the list of cloned rollup rule views that were active (and are still
	// active) as of timeNanos.
	for _, rollupRule := range as.rollupRules {
		rule := rollupRule.activeRule(timeNanos)
		// Skip missing or empty rules.
		// tombstoned() returns true if the length of rule.snapshots is zero.
		if rule == nil || rule.tombstoned() {
			continue
		}

		view, err := rule.rollupRuleView(len(rule.snapshots) - 1)
		if err != nil {
			return nil, err
		}
		out = append(out, view)
	}
	return out, nil
}

func (as *activeRuleSet) rollupResultsFor(id []byte, timeNanos int64, matchOpts MatchOptions) (rollupResults, error) {
	var (
		cutoverNanos  int64
		rollupTargets []rollupTarget
		keepOriginal  bool
		tags          [][]models.Tag
	)

	for _, rollupRule := range as.rollupRules {
		snapshot := rollupRule.activeSnapshot(timeNanos)
		if snapshot == nil {
			continue
		}
		match, err := snapshot.filter.Matches(id, filters.TagMatchOptions{
			NameAndTagsFn:       matchOpts.NameAndTagsFn,
			SortedTagIteratorFn: matchOpts.SortedTagIteratorFn,
		})
		if err != nil {
			return rollupResults{}, err
		}
		if !match {
			continue
		}

		// Make sure the cutover time tracks the latest cutover time among all matching
		// rollup rules to represent the correct time of rule change.
		if cutoverNanos < snapshot.cutoverNanos {
			cutoverNanos = snapshot.cutoverNanos
		}

		if snapshot.keepOriginal {
			keepOriginal = true
		}

		// If the rollup rule snapshot is a tombstoned snapshot, its cutover time is
		// recorded to indicate a rule change, but its rollup targets are no longer in effect.
		if snapshot.tombstoned {
			continue
		}

		for _, target := range snapshot.targets {
			rollupTargets = append(rollupTargets, target.clone())
			tags = append(tags, snapshot.tags)
		}
	}
	// NB: could log the matching error here if needed.
	res, _ := as.toRollupResults(id, cutoverNanos, rollupTargets, keepOriginal, tags, matchOpts)
	return res, nil
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
	keepOriginal bool,
	tags [][]models.Tag,
	matchOpts MatchOptions,
) (rollupResults, error) {
	if len(targets) == 0 {
		return rollupResults{}, nil
	}

	// If we cannot extract tags from the id, this is likely an invalid
	// metric and we bail early.
	_, sortedTagPairBytes, err := matchOpts.NameAndTagsFn(id)
	if err != nil {
		return rollupResults{}, err
	}

	var (
		multiErr           = xerrors.NewMultiError()
		pipelines          = make([]metadata.PipelineMetadata, 0, len(targets))
		newRollupIDResults = make([]idWithMatchResults, 0, len(targets))
		tagPairs           []metricid.TagPair
	)

	for idx, target := range targets {
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
			rollupID, matched, err = as.matchRollupTarget(
				sortedTagPairBytes,
				firstOp.Rollup,
				tagPairs,
				tags[idx],
				matchRollupTargetOptions{generateRollupID: true},
				matchOpts)
			if err != nil {
				multiErr = multiErr.Add(err)
				continue
			}
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
		applied, err := as.applyIDToPipeline(sortedTagPairBytes, toApply, tagPairs, tags[idx], matchOpts)
		if err != nil {
			err = fmt.Errorf("failed to apply id %s to pipeline %v: %v", id, toApply, err)
			multiErr = multiErr.Add(err)
			continue
		}
		newPipeline := metadata.PipelineMetadata{
			AggregationID:   aggregationID,
			StoragePolicies: target.StoragePolicies,
			Pipeline:        applied,
			ResendEnabled:   target.ResendEnabled,
		}
		if rollupID == nil {
			// The applied pipeline applies to the incoming ID.
			pipelines = append(pipelines, newPipeline)
		} else {
			if len(tags[idx]) > 0 {
				newPipeline.Tags = tags[idx]
			}
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
		keepOriginal:    keepOriginal,
	}, multiErr.FinalError()
}

// matchRollupTarget matches an incoming metric ID against a rollup target,
// returns the new rollup ID if the metric ID contains the full list of rollup
// tags, and nil otherwise.
func (as *activeRuleSet) matchRollupTarget(
	sortedTagPairBytes []byte,
	rollupOp mpipeline.RollupOp,
	tagPairs []metricid.TagPair, // buffer for reuse to generate rollup ID across calls
	tags []models.Tag,
	targetOpts matchRollupTargetOptions,
	matchOpts MatchOptions,
) ([]byte, bool, error) {
	if rollupOp.Type == mpipeline.ExcludeByRollupType && !targetOpts.generateRollupID {
		// Exclude by tag always matches, if not generating rollup ID
		// then immediately return.
		return nil, true, nil
	}

	var (
		rollupTags    = rollupOp.Tags
		sortedTagIter = matchOpts.SortedTagIteratorFn(sortedTagPairBytes)
		matchTagIdx   = 0
		nameTagName   = as.tagsFilterOpts.NameTagKey
		nameTagValue  []byte
	)

	switch rollupOp.Type {
	case mpipeline.GroupByRollupType:
		// Iterate through each tag, looking to match it with corresponding filter tags on the rule
		//
		// For include rules, every rule has to have a corresponding match. This means we return
		// early whenever there's a missing match and increment matchRuleIdx whenever there is a match.
		for hasMoreTags := sortedTagIter.Next(); hasMoreTags; hasMoreTags = sortedTagIter.Next() {
			tagName, tagVal := sortedTagIter.Current()
			// nolint:gosimple
			isNameTag := bytes.Compare(tagName, nameTagName) == 0
			if isNameTag {
				nameTagValue = tagVal
			}

			// If we've matched all tags, no need to process.
			// We don't break out of the for loop, because we may still need to find the name tag.
			if matchTagIdx >= len(rollupTags) {
				continue
			}

			res := bytes.Compare(tagName, rollupTags[matchTagIdx])
			if res == 0 {
				// Include grouped by tag.
				if targetOpts.generateRollupID {
					tagPairs = append(tagPairs, metricid.TagPair{Name: tagName, Value: tagVal})
				}
				matchTagIdx++
				continue
			}

			// If one of the target tags is not found in the ID, this is considered  a non-match so return immediately.
			if res > 0 {
				return nil, false, nil
			}
		}
	case mpipeline.ExcludeByRollupType:
		// Iterate through each tag, looking to match it with corresponding filter tags on the rule.
		//
		// For exclude rules, this means merging with the tag rule list and incrementing the
		// matchTagIdx whenever the current tag rule is lexigraphically greater than the rule tag,
		// since we need to be careful in the case where there is no matching input tag for some rule.
		for hasMoreTags := sortedTagIter.Next(); hasMoreTags; {
			tagName, tagVal := sortedTagIter.Current()
			// nolint:gosimple
			isNameTag := bytes.Compare(tagName, nameTagName) == 0
			if isNameTag {
				nameTagValue = tagVal

				// Don't copy name tag since we'll add that using the new rollup ID fn.
				hasMoreTags = sortedTagIter.Next()
				continue
			}

			if matchTagIdx >= len(rollupTags) {
				// Have matched all the tags to exclude, just blindly copy.
				if targetOpts.generateRollupID {
					tagPairs = append(tagPairs, metricid.TagPair{Name: tagName, Value: tagVal})
				}
				hasMoreTags = sortedTagIter.Next()
				continue
			}

			res := bytes.Compare(tagName, rollupTags[matchTagIdx])
			if res > 0 {
				// Current tag is greater than the current exclude rule,
				// so we know the current exclude rule has no match and
				// we should move on to the next one.
				matchTagIdx++
				continue
			}

			if res != 0 {
				// Only include tags that don't match the exclude tag
				if targetOpts.generateRollupID {
					tagPairs = append(tagPairs, metricid.TagPair{Name: tagName, Value: tagVal})
				}
			}

			hasMoreTags = sortedTagIter.Next()
		}
	}

	if sortedTagIter.Err() != nil {
		return nil, false, sortedTagIter.Err()
	}

	if !targetOpts.generateRollupID {
		return nil, true, nil
	}

	for _, tag := range tags {
		tagPairs = append(tagPairs, metricid.TagPair{
			Name:  tag.Name,
			Value: tag.Value,
		})
	}

	newName := rollupOp.NewName(nameTagValue)
	return as.newRollupIDFn(newName, tagPairs), true, nil
}

func (as *activeRuleSet) applyIDToPipeline(
	sortedTagPairBytes []byte,
	pipeline mpipeline.Pipeline,
	tagPairs []metricid.TagPair, // buffer for reuse across calls
	tags []models.Tag,
	matchOpts MatchOptions,
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
			rollupID, matched, err := as.matchRollupTarget(
				sortedTagPairBytes,
				rollupOp,
				tagPairs,
				tags,
				matchRollupTargetOptions{generateRollupID: true},
				matchOpts)
			if err != nil {
				return applied.Pipeline{}, err
			}
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

	// Otherwise merge as per usual
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

	// This represents whether or not the original (source) metric for the
	// matched rollup rule should be kept. If true, both metrics are written;
	// if false, only the new generated rollup metric is written.
	keepOriginal bool
}

type forwardMatchResult struct {
	forExistingID   metadata.StagedMetadata
	forNewRollupIDs []IDWithMetadatas
	keepOriginal    bool
}
