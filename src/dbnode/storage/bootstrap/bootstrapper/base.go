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
	"github.com/m3db/m3/src/x/context"

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
	ctx context.Context,
	namespaces bootstrap.Namespaces,
	cache bootstrap.Cache,
) (bootstrap.NamespaceResults, error) {
	logFields := []zapcore.Field{
		zap.String("bootstrapper", b.name),
	}

	curr := bootstrap.Namespaces{
		Namespaces: bootstrap.NewNamespacesMap(bootstrap.NamespacesMapOptions{}),
	}
	for _, elem := range namespaces.Namespaces.Iter() {
		id := elem.Key()

		// Shallow copy the namespace, do not modify namespaces input to bootstrap call.
		currNamespace := elem.Value()

		b.logShardTimeRanges("bootstrap from source requested",
			logFields, currNamespace)

		dataAvailable, err := b.src.AvailableData(currNamespace.Metadata,
			currNamespace.DataRunOptions.ShardTimeRanges.Copy(), cache,
			currNamespace.DataRunOptions.RunOptions)
		if err != nil {
			return bootstrap.NamespaceResults{}, err
		}

		currNamespace.DataRunOptions.ShardTimeRanges = dataAvailable

		// Prepare index if required.
		if currNamespace.Metadata.Options().IndexOptions().Enabled() {
			indexAvailable, err := b.src.AvailableIndex(currNamespace.Metadata,
				currNamespace.IndexRunOptions.ShardTimeRanges.Copy(), cache,
				currNamespace.IndexRunOptions.RunOptions)
			if err != nil {
				return bootstrap.NamespaceResults{}, err
			}

			currNamespace.IndexRunOptions.ShardTimeRanges = indexAvailable
		}

		// Set the namespace options for the current bootstrapper source.
		curr.Namespaces.Set(id, currNamespace)

		// Log the metadata about bootstrapping this namespace based on
		// the availability returned.
		b.logShardTimeRanges("bootstrap from source ready after availability query",
			logFields, currNamespace)
	}

	nowFn := b.opts.ClockOptions().NowFn()
	begin := nowFn()

	// Run the bootstrap source begin hook.
	b.log.Info("bootstrap from source hook begin started", logFields...)
	if err := namespaces.Hooks().BootstrapSourceBegin(); err != nil {
		return bootstrap.NamespaceResults{}, err
	}

	b.log.Info("bootstrap from source started", logFields...)

	// Run the bootstrap source.
	currResults, err := b.src.Read(ctx, curr, cache)

	logFields = append(logFields, zap.Duration("took", nowFn().Sub(begin)))
	if err != nil {
		errorLogFields := append(logFieldsCopy(logFields), zap.Error(err))
		b.log.Error("error bootstrapping from source", errorLogFields...)
		return bootstrap.NamespaceResults{}, err
	}

	// Run the bootstrap source end hook.
	b.log.Info("bootstrap from source hook end started", logFields...)
	if err := namespaces.Hooks().BootstrapSourceEnd(); err != nil {
		return bootstrap.NamespaceResults{}, err
	}

	b.log.Info("bootstrap from source completed", logFields...)
	// Determine the unfulfilled and the unattempted ranges to execute next.
	next, err := b.logSuccessAndDetermineCurrResultsUnfulfilledAndNextBootstrapRanges(namespaces,
		curr, currResults, logFields)
	if err != nil {
		return bootstrap.NamespaceResults{}, err
	}

	// Unless next bootstrapper is required, this is the final results.
	finalResults := currResults

	// If there are some time ranges the current bootstrapper could not fulfill,
	// that we can attempt then pass it along to the next bootstrapper.
	if next.Namespaces.Len() > 0 {
		nextResults, err := b.next.Bootstrap(ctx, next, cache)
		if err != nil {
			return bootstrap.NamespaceResults{}, err
		}

		// Now merge the final results.
		for _, elem := range nextResults.Results.Iter() {
			id := elem.Key()
			currNamespace := elem.Value()

			finalResult, ok := finalResults.Results.Get(id)
			if !ok {
				return bootstrap.NamespaceResults{},
					fmt.Errorf("expected result for namespace: %s", id.String())
			}

			// NB(r): Since we originally passed all unfulfilled ranges to the
			// next bootstrapper, the final unfulfilled is simply what it could
			// not fulfill.
			finalResult.DataResult.SetUnfulfilled(currNamespace.DataResult.Unfulfilled().Copy())
			if currNamespace.Metadata.Options().IndexOptions().Enabled() {
				finalResult.IndexResult.SetUnfulfilled(currNamespace.IndexResult.Unfulfilled().Copy())
			}

			// Map is by value, set the result altered struct.
			finalResults.Results.Set(id, finalResult)
		}
	}

	return finalResults, nil
}

func (b baseBootstrapper) logSuccessAndDetermineCurrResultsUnfulfilledAndNextBootstrapRanges(
	requested bootstrap.Namespaces,
	curr bootstrap.Namespaces,
	currResults bootstrap.NamespaceResults,
	baseLogFields []zapcore.Field,
) (bootstrap.Namespaces, error) {
	next := bootstrap.Namespaces{
		Namespaces: bootstrap.NewNamespacesMap(bootstrap.NamespacesMapOptions{}),
	}
	for _, elem := range requested.Namespaces.Iter() {
		id := elem.Key()
		requestedNamespace := elem.Value()

		currResult, ok := currResults.Results.Get(id)
		if !ok {
			return bootstrap.Namespaces{},
				fmt.Errorf("namespace result not returned by bootstrapper: %v", id.String())
		}

		currNamespace, ok := curr.Namespaces.Get(id)
		if !ok {
			return bootstrap.Namespaces{},
				fmt.Errorf("namespace prepared request not found: %v", id.String())
		}

		// Shallow copy the current namespace for the next namespace prepared request.
		nextNamespace := currNamespace

		// Calculate bootstrap time ranges.
		dataRequired := requestedNamespace.DataRunOptions.ShardTimeRanges.Copy()
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
			indexCurrRequested = result.NewShardTimeRanges()
			indexCurrFulfilled = result.NewShardTimeRanges()
			indexUnfulfilled   = result.NewShardTimeRanges()
		)
		if currNamespace.Metadata.Options().IndexOptions().Enabled() {
			// Calculate bootstrap time ranges.
			indexRequired := requestedNamespace.IndexRunOptions.ShardTimeRanges.Copy()
			indexCurrRequested = currNamespace.IndexRunOptions.ShardTimeRanges.Copy()
			indexCurrFulfilled = indexCurrRequested.Copy()
			indexCurrFulfilled.Subtract(currResult.IndexResult.Unfulfilled())

			indexUnfulfilled = indexRequired.Copy()
			indexUnfulfilled.Subtract(indexCurrFulfilled)

			// Modify the unfulfilled result.
			currResult.IndexResult.SetUnfulfilled(indexUnfulfilled.Copy())
		}

		// Set the next bootstrapper required ranges.
		// NB(r): Make sure to always set an empty requested range so IsEmpty
		// does not cause nil ptr deref.
		nextNamespace.IndexRunOptions.ShardTimeRanges = indexUnfulfilled.Copy()

		// Set the modified result.
		currResults.Results.Set(id, currResult)

		// Always set the next bootstrapper namespace run options regardless of
		// whether there are unfulfilled index/data shard time ranges.
		// NB(bodu): We perform short circuiting directly in the peers bootstrapper and the
		// commitlog bootstrapper should always run for all time ranges.
		next.Namespaces.Set(id, nextNamespace)

		// Log the result.
		_, _, dataRangeRequested := dataCurrRequested.MinMaxRange()
		_, _, dataRangeFulfilled := dataCurrFulfilled.MinMaxRange()
		successLogFields := append(logFieldsCopy(baseLogFields),
			zap.String("namespace", id.String()),
			zap.Int("numShards", len(currNamespace.Shards)),
			zap.Duration("dataRangeRequested", dataRangeRequested),
			zap.Duration("dataRangeFulfilled", dataRangeFulfilled),
		)

		if currNamespace.Metadata.Options().IndexOptions().Enabled() {
			_, _, indexRangeRequested := indexCurrRequested.MinMaxRange()
			_, _, indexRangeFulfilled := indexCurrFulfilled.MinMaxRange()
			successLogFields = append(successLogFields,
				zap.Duration("indexRangeRequested", indexRangeRequested),
				zap.Duration("indexRangeFulfilled", indexRangeFulfilled),
				zap.Int("numIndexBlocks", len(currResult.IndexResult.IndexResults())),
			)
		}

		b.log.Info("bootstrapping from source completed successfully",
			successLogFields...)
	}

	return next, nil
}

func (b baseBootstrapper) logShardTimeRanges(
	msg string,
	baseLogFields []zapcore.Field,
	currNamespace bootstrap.Namespace,
) {
	dataShardTimeRanges := currNamespace.DataRunOptions.ShardTimeRanges
	dataMin, dataMax, dataRange := dataShardTimeRanges.MinMaxRange()
	logFields := append(logFieldsCopy(baseLogFields),
		zap.Stringer("namespace", currNamespace.Metadata.ID()),
		zap.Int("numShards", len(currNamespace.Shards)),
		zap.Duration("dataRange", dataRange),
	)
	if dataRange > 0 {
		logFields = append(logFields,
			zap.Time("dataFrom", dataMin.ToTime()),
			zap.Time("dataTo", dataMax.ToTime()),
		)
	}
	if currNamespace.Metadata.Options().IndexOptions().Enabled() {
		indexShardTimeRanges := currNamespace.IndexRunOptions.ShardTimeRanges
		indexMin, indexMax, indexRange := indexShardTimeRanges.MinMaxRange()
		logFields = append(logFields,
			zap.Duration("indexRange", indexRange),
		)
		if indexRange > 0 {
			logFields = append(logFields,
				zap.Time("indexFrom", indexMin.ToTime()),
				zap.Time("indexTo", indexMax.ToTime()),
			)
		}
	}

	b.log.Info(msg, logFields...)
}

func logFieldsCopy(logFields []zapcore.Field) []zapcore.Field {
	return append(make([]zapcore.Field, 0, 2*len(logFields)), logFields...)
}
