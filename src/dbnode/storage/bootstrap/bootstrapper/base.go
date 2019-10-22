// Copyright (c) 2016 Uber Technologies, Inc.
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

package bootstrapper

import (
	"fmt"

	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	baseBootstrapperName = "base"
)

// baseBootstrapper provides a skeleton for the interface methods.
type baseBootstrapper struct {
	opts result.Options
	log  *zap.Logger
	name string
	src  bootstrap.Source
	next bootstrap.Bootstrapper
}

// NewBaseBootstrapper creates a new base bootstrapper.
func NewBaseBootstrapper(
	name string,
	src bootstrap.Source,
	opts result.Options,
	next bootstrap.Bootstrapper,
) (bootstrap.Bootstrapper, error) {
	var (
		bs  = next
		err error
	)
	if next == nil {
		bs, err = NewNoOpNoneBootstrapperProvider().Provide()
		if err != nil {
			return nil, err
		}
	}
	return baseBootstrapper{
		opts: opts,
		log:  opts.InstrumentOptions().Logger(),
		name: name,
		src:  src,
		next: bs,
	}, nil
}

// String returns the name of the bootstrapper.
func (b baseBootstrapper) String() string {
	return baseBootstrapperName
}

func (b baseBootstrapper) Bootstrap(
	namespaces bootstrap.Namespaces,
) (bootstrap.NamespaceResults, error) {
	logFields := []zapcore.Field{
		zap.String("bootstrapper", b.name),
	}

	curr := bootstrap.Namespaces{
		Namespaces: bootstrap.NewNamespacesMap(bootstrap.NamespacesMapOptions{}),
	}
	for _, elem := range namespaces.Namespaces.Iter() {
		id := elem.Key()
		namespace := elem.Value()

		// Shallow copy the namespace, do not modify namespaces input to bootstrap call.
		currNamespace := namespace

		dataRange := currNamespace.DataRunOptions.ShardTimeRanges.MinMaxRange()
		requestedLogFields := append(logFieldsCopy(logFields), []zapcore.Field{
			zap.String("namespace", id.String()),
			zap.Int("numShards", len(currNamespace.Shards)),
			zap.Duration("dataRange", dataRange),
		}...)
		if dataRange > 0 {
			dataMin, dataMax := currNamespace.DataRunOptions.ShardTimeRanges.MinMax()
			requestedLogFields = append(requestedLogFields, []zapcore.Field{
				zap.Time("dataFrom", dataMin),
				zap.Time("dataTo", dataMax),
			}...)
		}
		if namespace.Metadata.Options().IndexOptions().Enabled() {
			indexRange := currNamespace.IndexRunOptions.ShardTimeRanges.MinMaxRange()
			requestedLogFields = append(requestedLogFields, []zapcore.Field{
				zap.Duration("indexRange", indexRange),
			}...)
			if indexRange > 0 {
				indexMin, indexMax := currNamespace.IndexRunOptions.ShardTimeRanges.MinMax()
				requestedLogFields = append(requestedLogFields, []zapcore.Field{
					zap.Time("indexFrom", indexMin),
					zap.Time("indexTo", indexMax),
				}...)
			}
		}

		b.log.Info("bootstrap from source for namespace requested", requestedLogFields...)

		dataAvailable, err := b.src.AvailableData(namespace.Metadata,
			namespace.DataRunOptions.ShardTimeRanges.Copy(),
			namespace.DataRunOptions.RunOptions)
		if err != nil {
			return bootstrap.NamespaceResults{}, err
		}

		currNamespace.DataRunOptions.ShardTimeRanges = dataAvailable

		// Prepare index if required.
		if namespace.Metadata.Options().IndexOptions().Enabled() {
			indexAvailable, err := b.src.AvailableIndex(namespace.Metadata,
				namespace.DataRunOptions.ShardTimeRanges.Copy(),
				namespace.DataRunOptions.RunOptions)
			if err != nil {
				return bootstrap.NamespaceResults{}, err
			}

			currNamespace.IndexRunOptions.ShardTimeRanges = indexAvailable
		}

		// Set the namespace options for the current bootstrapper source.
		curr.Namespaces.Set(id, currNamespace)

		// Log the metadata about bootstrapping this namespace.
		dataRange = currNamespace.DataRunOptions.ShardTimeRanges.MinMaxRange()
		prepareLogFields := append(logFieldsCopy(logFields), []zapcore.Field{
			zap.String("namespace", id.String()),
			zap.Int("numShards", len(currNamespace.DataRunOptions.ShardTimeRanges)),
			zap.Duration("dataRange", dataRange),
		}...)
		if dataRange > 0 {
			dataMin, dataMax := currNamespace.DataRunOptions.ShardTimeRanges.MinMax()
			prepareLogFields = append(prepareLogFields, []zapcore.Field{
				zap.Time("dataFrom", dataMin),
				zap.Time("dataTo", dataMax),
			}...)
		}

		if namespace.Metadata.Options().IndexOptions().Enabled() {
			indexRange := currNamespace.IndexRunOptions.ShardTimeRanges.MinMaxRange()
			prepareLogFields = append(prepareLogFields, []zapcore.Field{
				zap.Duration("indexRange", indexRange),
			}...)
			if indexRange > 0 {
				indexMin, indexMax := currNamespace.IndexRunOptions.ShardTimeRanges.MinMax()
				prepareLogFields = append(prepareLogFields, []zapcore.Field{
					zap.Time("indexFrom", indexMin),
					zap.Time("indexTo", indexMax),
				}...)
			}
		}

		b.log.Info("bootstrap from source for namespace ready", prepareLogFields...)
	}

	nowFn := b.opts.ClockOptions().NowFn()
	begin := nowFn()

	b.log.Info("bootstrap from source started", logFields...)
	currResults, err := b.src.Read(curr)

	logFields = append(logFields, zap.Duration("took", nowFn().Sub(begin)))
	if err != nil {
		errorLogFields := append(logFieldsCopy(logFields), zap.Error(err))
		b.log.Info("bootstrapping from source completed with error", errorLogFields...)
	}
	if err != nil {
		return bootstrap.NamespaceResults{}, err
	}

	// Determine the unfulfilled and the unattempted ranges to execute next.
	next := bootstrap.Namespaces{
		Namespaces: bootstrap.NewNamespacesMap(bootstrap.NamespacesMapOptions{}),
	}
	for _, elem := range namespaces.Namespaces.Iter() {
		id := elem.Key()
		namespace := elem.Value()

		currResult, ok := currResults.Results.Get(id)
		if !ok {
			return bootstrap.NamespaceResults{},
				fmt.Errorf("namespace result not returned by bootstrapper: %v", id.String())
		}

		currNamespace, ok := curr.Namespaces.Get(id)
		if !ok {
			return bootstrap.NamespaceResults{},
				fmt.Errorf("namespace prepared request not found: %v", id.String())
		}

		// Shallow copy the current namespace for the next namespace prepared request.
		nextNamespace := currNamespace

		// Calculate bootstrap time ranges.
		dataRequired := namespace.DataRunOptions.ShardTimeRanges.Copy()
		dataCurrRequested := currNamespace.DataRunOptions.ShardTimeRanges.Copy()
		dataCurrFulfilled := dataCurrRequested.Copy()
		dataCurrFulfilled.Subtract(currResult.DataResult.Unfulfilled())

		dataUnfulfilled := dataRequired.Copy()
		dataUnfulfilled.Subtract(dataCurrFulfilled)

		// Modify the unfulfilled result.
		currResult.DataResult.SetUnfulfilled(dataUnfulfilled.Copy())

		// Set the next bootstrapper required ranges.
		nextNamespace.DataRunOptions.ShardTimeRanges = dataUnfulfilled.Copy()

		var (
			indexRequired      = result.ShardTimeRanges{}
			indexCurrRequested = result.ShardTimeRanges{}
			indexCurrFulfilled = result.ShardTimeRanges{}
		)
		if namespace.Metadata.Options().IndexOptions().Enabled() {
			// Calculate bootstrap time ranges.
			indexRequired = namespace.IndexRunOptions.ShardTimeRanges.Copy()
			indexCurrRequested = currNamespace.IndexRunOptions.ShardTimeRanges.Copy()
			indexCurrFulfilled = indexCurrRequested.Copy()
			indexCurrFulfilled.Subtract(currResult.IndexResult.Unfulfilled())

			indexUnfulfilled := indexRequired.Copy()
			indexUnfulfilled.Subtract(indexCurrFulfilled)

			// Modify the unfulfilled result.
			currResult.IndexResult.SetUnfulfilled(indexUnfulfilled.Copy())

			// Set the next bootstrapper required ranges.
			nextNamespace.IndexRunOptions.ShardTimeRanges = indexUnfulfilled.Copy()
		} else {
			// NB(r): Make sure to always set an empty requested range so IsEmpty
			// does not cause nil ptr deref.
			nextNamespace.IndexRunOptions.ShardTimeRanges = result.ShardTimeRanges{}
		}

		// Set the modified result.
		currResults.Results.Set(id, currResult)

		// Set the next bootstrapper namespace run options if we need to bootstrap
		// further time ranges.
		if !nextNamespace.DataRunOptions.ShardTimeRanges.IsEmpty() ||
			!nextNamespace.IndexRunOptions.ShardTimeRanges.IsEmpty() {
			next.Namespaces.Set(id, nextNamespace)
		}

		// Log the result.
		successLogFields := append(logFieldsCopy(logFields), []zapcore.Field{
			zap.String("namespace", id.String()),
			zap.Int("numShards", len(currNamespace.Shards)),
		}...)
		successLogFields = append(successLogFields,
			zap.Duration("dataRangeRequested", dataCurrRequested.MinMaxRange()))
		successLogFields = append(successLogFields,
			zap.Duration("dataRangeFulfilled", dataCurrFulfilled.MinMaxRange()))
		successLogFields = append(successLogFields,
			zap.Duration("indexRangeRequested", indexCurrRequested.MinMaxRange()))
		successLogFields = append(successLogFields,
			zap.Duration("indexRangeFulfilled", indexCurrFulfilled.MinMaxRange()))

		successLogFields = append(successLogFields,
			zap.Int("dataNumSeries", currResult.DataMetadata.NumSeries))
		if namespace.Metadata.Options().IndexOptions().Enabled() {
			successLogFields = append(successLogFields,
				zap.Int("indexNumSeries", currResult.IndexMetadata.NumSeries))
		}

		b.log.Info("bootstrapping from source completed successfully",
			successLogFields...)

	}

	// Unless next bootstrapper is required, this is the final results.
	finalResults := currResults

	// If there are some time ranges the current bootstrapper could not fulfill,
	// that we can attempt then pass it along to the next bootstrapper.
	if next.Namespaces.Len() > 0 {
		nextResults, err := b.next.Bootstrap(next)
		if err != nil {
			return bootstrap.NamespaceResults{}, err
		}

		// Now merge the final results.
		for _, elem := range nextResults.Results.Iter() {
			id := elem.Key()
			namespace := elem.Value()

			finalResult, ok := finalResults.Results.Get(id)
			if !ok {
				return bootstrap.NamespaceResults{},
					fmt.Errorf("expected result for namespace: %s", id.String())
			}

			// NB(r): Since we originally passed all unfulfilled ranges to the
			// next bootstrapper, the final unfulfilled is simply what it could
			// not fulfill.
			finalResult.DataResult.SetUnfulfilled(namespace.DataResult.Unfulfilled().Copy())
			if namespace.Metadata.Options().IndexOptions().Enabled() {
				finalResult.IndexResult.SetUnfulfilled(namespace.IndexResult.Unfulfilled().Copy())
			}

			// Map is by value, set the result altered struct.
			finalResults.Results.Set(id, finalResult)
		}
	}

	return finalResults, nil
}

func logFieldsCopy(logFields []zapcore.Field) []zapcore.Field {
	return append([]zapcore.Field(nil), logFields...)
}
