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

package bootstrap

import (
	"fmt"
	"io"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ReadersForID is a slice of readers that share a series ID.
type ReadersForID []ReaderAtTime

// ReaderMap is a map containing all gathered block segment readers.
type ReaderMap map[string]ReadersForID

// TestDataAccumulator is a NamespaceDataAccumulator that captures any
// series inserts for examination.
type TestDataAccumulator struct {
	t         *testing.T
	ctrl      *gomock.Controller
	ns        string
	pool      encoding.MultiReaderIteratorPool
	readerMap ReaderMap
}

// DecodedValues is a slice of series datapoints.
type DecodedValues []series.DecodedTestValue

// DecodedBlockMap is a map of decoded datapoints per series ID.
type DecodedBlockMap map[string]DecodedValues

// ReaderAtTime indicates the block start for incoming readers.
type ReaderAtTime struct {
	// Start is the block start time.
	Start time.Time
	// Reader is the block segment reader.
	Reader xio.SegmentReader
	// Tags is the list of tags in map format.
	Tags map[string]string
}

// DumpValues decodes any accumulated values and returns them as values.
func (a *TestDataAccumulator) DumpValues() DecodedBlockMap {
	if len(a.readerMap) == 0 {
		return nil
	}

	decodedMap := make(DecodedBlockMap, len(a.readerMap))
	iter := a.pool.Get()
	defer iter.Close()
	for k, v := range a.readerMap {
		readers := make([]xio.SegmentReader, 0, len(v))
		for _, r := range v {
			readers = append(readers, r.Reader)
		}

		value, err := series.DecodeSegmentValues(readers, iter, nil)
		if err != nil {
			if err != io.EOF {
				// fail test.
				require.NoError(a.t, err)
			}

			// NB: print out that we encountered EOF here to assist debugging tests.
			fmt.Println("EOF: segment had no values.")
		}

		sort.Sort(series.ValuesByTime(value))
		decodedMap[k] = value
	}

	return decodedMap
}

// CheckoutSeries will retrieve a series for writing to
// and when the accumulator is closed it will ensure that the
// series is released.
func (a *TestDataAccumulator) CheckoutSeries(
	id ident.ID,
	tags ident.TagIterator,
) (CheckoutSeriesResult, error) {
	decodedTags := make(map[string]string, tags.Len())
	for tags.Next() {
		tag := tags.Current()
		name := tag.Name.String()
		value := tag.Value.String()
		if len(name) > 0 && len(value) > 0 {
			decodedTags[name] = value
		}
	}

	require.NoError(a.t, tags.Err())
	stringID := id.String()
	var streamErr error
	mockSeries := series.NewMockDatabaseSeries(a.ctrl)
	mockSeries.EXPECT().
		LoadBlock(gomock.Any(), gomock.Any()).
		DoAndReturn(func(bl block.DatabaseBlock, _ series.WriteType) error {
			reader, err := bl.Stream(context.NewContext())
			if err != nil {
				streamErr = err
				return err
			}

			a.readerMap[stringID] = append(a.readerMap[stringID], ReaderAtTime{
				Start:  bl.StartTime(),
				Reader: reader,
				Tags:   decodedTags,
			})
			return nil
		}).AnyTimes()

	// NB: unique index doesn't matter here.
	return CheckoutSeriesResult{
		Series: mockSeries,
	}, streamErr
}

// Release will reset and release all checked out series from
// the accumulator so owners can return them and reset the
// keys lookup.
func (a *TestDataAccumulator) Release() {}

// Close will close the data accumulator and will return an error
// if any checked out series have not been released yet with reset.
func (a *TestDataAccumulator) Close() error { return nil }

// NamespacesTester is a utility to assist testing bootstrapping.
type NamespacesTester struct {
	t    *testing.T
	ctrl *gomock.Controller
	pool encoding.MultiReaderIteratorPool

	// Accumulators are the accumulators which incoming blocks get loaded into.
	// One per namespace.
	Accumulators []*TestDataAccumulator

	// Namespaces is the namespaces for this tester.
	Namespaces Namespaces
	// Results are the namespace results after bootstrapping.
	Results NamespaceResults
}

// BuildNamespacesTester builds a NamespacesTester.
func BuildNamespacesTester(
	t *testing.T,
	runOpts RunOptions,
	ranges result.ShardTimeRanges,
	mds ...namespace.Metadata,
) NamespacesTester {
	shards := make([]uint32, 0, len(ranges))
	for shard := range ranges {
		shards = append(shards, shard)
	}

	ctrl := gomock.NewController(t)
	iterPool := encoding.NewMultiReaderIteratorPool(pool.NewObjectPoolOptions())
	iterPool.Init(
		func(r io.Reader, _ namespace.SchemaDescr) encoding.ReaderIterator {
			return m3tsz.NewReaderIterator(r,
				m3tsz.DefaultIntOptimizationEnabled,
				encoding.NewOptions())
		})

	namespacesMap := NewNamespacesMap(NamespacesMapOptions{})
	accumulators := make([]*TestDataAccumulator, 0, len(mds))
	for _, md := range mds {
		acc := &TestDataAccumulator{
			t:         t,
			ctrl:      ctrl,
			pool:      iterPool,
			ns:        md.ID().String(),
			readerMap: make(ReaderMap),
		}

		accumulators = append(accumulators, acc)
		namespacesMap.Set(md.ID(), Namespace{
			Metadata:        md,
			Shards:          shards,
			DataAccumulator: acc,
			DataRunOptions: NamespaceRunOptions{
				ShardTimeRanges: ranges.Copy(),
				RunOptions:      runOpts,
			},
			IndexRunOptions: NamespaceRunOptions{
				ShardTimeRanges: ranges.Copy(),
				RunOptions:      runOpts,
			},
		})
	}

	return NamespacesTester{
		t:            t,
		ctrl:         ctrl,
		pool:         iterPool,
		Accumulators: accumulators,
		Namespaces: Namespaces{
			Namespaces: namespacesMap,
		},
	}
}

// DecodedNamespaceMap is a map of decoded blocks per namespace ID.
type DecodedNamespaceMap map[string]DecodedBlockMap

// DumpValues dumps any accumulated blocks as decoded series per namespace.
func (nt *NamespacesTester) DumpValues() DecodedNamespaceMap {
	nsMap := make(DecodedNamespaceMap, len(nt.Accumulators))
	for _, acc := range nt.Accumulators {
		block := acc.DumpValues()

		if block != nil {
			nsMap[acc.ns] = block
		}
	}

	return nsMap
}

// DumpReadersForNamespace dumps the readers and their start times for a given
// namespace.
func (nt *NamespacesTester) DumpReadersForNamespace(
	md namespace.Metadata,
) ReaderMap {
	id := md.ID().String()
	for _, acc := range nt.Accumulators {
		if acc.ns == id {
			return acc.readerMap
		}
	}

	assert.FailNow(nt.t, fmt.Sprintf("namespace with id %s not found "+
		"valid namespaces are %v", id, nt.Namespaces))
	return nil
}

// ResultForNamespace gives the result for the given namespace.
func (nt *NamespacesTester) ResultForNamespace(id ident.ID) NamespaceResult {
	result, found := nt.Results.Results.Get(id)
	require.True(nt.t, found)
	return result
}

// TestBootstrapWith bootstraps the current Namespaces with the
// provided bootstrapper.
func (nt *NamespacesTester) TestBootstrapWith(b Bootstrapper) {
	res, err := b.Bootstrap(nt.Namespaces)
	assert.NoError(nt.t, err)
	nt.Results = res
}

// TestReadWith reads the current Namespaces with the
// provided bootstrap source.
func (nt *NamespacesTester) TestReadWith(s Source) {
	res, err := s.Read(nt.Namespaces)
	require.NoError(nt.t, err)
	nt.Results = res
}

func (nt *NamespacesTester) validateRanges(
	name string,
	ac xtime.Ranges,
	ex xtime.Ranges,
) {
	// Make range eclipses expected
	removedRange := ex.RemoveRanges(ac)
	require.True(nt.t, removedRange.IsEmpty(),
		fmt.Sprintf("%s: actual range %v does not match expected range %v "+
			"diff: %v", name, ac, ex, removedRange))

	// Now make sure no ranges outside of expected
	expectedWithAddedRanges := ex.AddRanges(ac)

	require.Equal(nt.t, ex.Len(), expectedWithAddedRanges.Len())
	iter := ex.Iter()
	withAddedRangesIter := expectedWithAddedRanges.Iter()
	for iter.Next() && withAddedRangesIter.Next() {
		require.True(nt.t, iter.Value().Equal(withAddedRangesIter.Value()),
			fmt.Sprintf("%s: actual range %v does not match expected range %v",
				name, ac, ex))
	}
}

func (nt *NamespacesTester) validateShardTimeRanges(
	name string,
	r result.ShardTimeRanges,
	ex result.ShardTimeRanges,
) {
	assert.Equal(nt.t, len(ex), len(r),
		fmt.Sprintf("%s: expected %v and actual %v size mismatch", name, ex, r))
	for k, val := range r {
		expectedVal, found := ex[k]
		require.True(nt.t, found,
			fmt.Sprintf("%s: expected shard map %v does not have key %d; actual: %v",
				name, ex, k, r))
		nt.validateRanges(name, val, expectedVal)
	}
}

// TestUnfulfilledForNamespace ensures the given namespace has the expected
// range flagged as unfulfilled.
func (nt *NamespacesTester) TestUnfulfilledForNamespace(
	md namespace.Metadata,
	ex result.ShardTimeRanges,
	exIdx result.ShardTimeRanges,
) {
	ns := nt.ResultForNamespace(md.ID())
	actual := ns.DataResult.Unfulfilled()
	nt.validateShardTimeRanges("data", actual, ex)

	if md.Options().IndexOptions().Enabled() {
		actual := ns.IndexResult.Unfulfilled()
		nt.validateShardTimeRanges("index", actual, exIdx)
	}
}

// TestUnfulfilledForNamespaceIsEmpty ensures the given namespace has an empty
// unfulfilled range.
func (nt *NamespacesTester) TestUnfulfilledForNamespaceIsEmpty(
	md namespace.Metadata,
) {
	nt.TestUnfulfilledForIDIsEmpty(md.ID(), md.Options().IndexOptions().Enabled())
}

// TestUnfulfilledForIDIsEmpty ensures the given id has an empty
// unfulfilled range.
func (nt *NamespacesTester) TestUnfulfilledForIDIsEmpty(
	id ident.ID,
	useIndex bool,
) {
	ns := nt.ResultForNamespace(id)
	actual := ns.DataResult.Unfulfilled()
	assert.True(nt.t, actual.IsEmpty(), fmt.Sprintf("data: not empty %v", actual))

	if useIndex {
		actual := ns.DataResult.Unfulfilled()
		assert.True(nt.t, actual.IsEmpty(),
			fmt.Sprintf("index: not empty %v", actual))
	}
}

// Finish closes the namespaceTester and tests mocks for completion.
func (nt *NamespacesTester) Finish() {
	nt.ctrl.Finish()
}
