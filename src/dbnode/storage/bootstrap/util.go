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

package bootstrap

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
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

// Must implement NamespaceDataAccumulator.
var _ NamespaceDataAccumulator = (*TestDataAccumulator)(nil)

// TestDataAccumulator is a NamespaceDataAccumulator that captures any
// series inserts for examination.
type TestDataAccumulator struct {
	sync.Mutex

	t         *testing.T
	ctrl      *gomock.Controller
	ns        string
	pool      encoding.MultiReaderIteratorPool
	readerMap ReaderMap
	schema    namespace.SchemaDescr
	// writeMap is a map to which values are written directly.
	writeMap DecodedBlockMap
	results  map[string]CheckoutSeriesResult
}

// DecodedValues is a slice of series datapoints.
type DecodedValues []series.DecodedTestValue

// DecodedBlockMap is a map of decoded datapoints per series ID.
type DecodedBlockMap map[string]DecodedValues

func testValuesEqual(
	a series.DecodedTestValue,
	b series.DecodedTestValue,
) bool {
	return a.Timestamp.Equal(b.Timestamp) &&
		math.Abs(a.Value-b.Value) < 0.000001 &&
		a.Unit == b.Unit &&
		bytes.Equal(a.Annotation, b.Annotation)
}

// VerifyEquals verifies that two DecodedBlockMap are equal; errors otherwise.
func (m DecodedBlockMap) VerifyEquals(other DecodedBlockMap) error {
	if len(m) != len(other) {
		return fmt.Errorf("block maps of length %d and %d do not match",
			len(m), len(other))
	}

	seen := make(map[string]struct{})
	for k, v := range m {
		otherSeries, found := other[k]
		if !found {
			return fmt.Errorf("series %s: values not found", k)
		}

		if len(otherSeries) != len(v) {
			return fmt.Errorf("series %s: length of series %d does not match other %d",
				k, len(v), len(otherSeries))
		}

		// NB: make a clone here to avoid mutating base data
		// just in case any tests care about order.
		thisVal := append([]series.DecodedTestValue(nil), v...)
		otherVal := append([]series.DecodedTestValue(nil), otherSeries...)

		sort.Sort(series.ValuesByTime(thisVal))
		sort.Sort(series.ValuesByTime(otherVal))
		for i, t := range thisVal {
			o := otherVal[i]
			if !testValuesEqual(t, o) {
				return fmt.Errorf("series %s: value %+v does not match other value %+v",
					k, t, o)
			}
		}

		seen[k] = struct{}{}
	}

	for k := range other {
		if _, beenFound := seen[k]; !beenFound {
			return fmt.Errorf("series %s not found in this map", k)
		}
	}

	return nil
}

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

		value, err := series.DecodeSegmentValues(readers, iter, a.schema)
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

// CheckoutSeriesWithLock will retrieve a series for writing to
// and when the accumulator is closed it will ensure that the
// series is released (with lock).
func (a *TestDataAccumulator) CheckoutSeriesWithLock(
	id ident.ID,
	tags ident.TagIterator,
) (CheckoutSeriesResult, error) {
	a.Lock()
	defer a.Unlock()
	return a.checkoutSeriesWithLock(id, tags)
}

// CheckoutSeriesWithoutLock will retrieve a series for writing to
// and when the accumulator is closed it will ensure that the
// series is released (without lock).
func (a *TestDataAccumulator) CheckoutSeriesWithoutLock(
	id ident.ID,
	tags ident.TagIterator,
) (CheckoutSeriesResult, error) {
	return a.checkoutSeriesWithLock(id, tags)
}

func (a *TestDataAccumulator) checkoutSeriesWithLock(
	id ident.ID,
	tags ident.TagIterator,
) (CheckoutSeriesResult, error) {
	var decodedTags map[string]string
	if tags != nil {
		decodedTags = make(map[string]string, tags.Len())
		for tags.Next() {
			tag := tags.Current()
			name := tag.Name.String()
			value := tag.Value.String()
			if len(name) > 0 && len(value) > 0 {
				decodedTags[name] = value
			}
		}

		require.NoError(a.t, tags.Err())
	}

	stringID := id.String()
	if result, found := a.results[stringID]; found {
		return result, nil
	}

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

	mockSeries.EXPECT().Write(
		gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(
			func(
				_ context.Context,
				ts time.Time,
				val float64,
				unit xtime.Unit,
				annotation []byte,
				_ series.WriteOptions,
			) (bool, error) {
				a.Lock()
				a.writeMap[stringID] = append(
					a.writeMap[stringID], series.DecodedTestValue{
						Timestamp:  ts,
						Value:      val,
						Unit:       unit,
						Annotation: annotation,
					})
				a.Unlock()
				return true, nil
			}).AnyTimes()

	a.Lock()
	result := CheckoutSeriesResult{
		Series:      mockSeries,
		UniqueIndex: uint64(len(a.results) + 1),
	}

	a.results[stringID] = result
	a.Unlock()

	return result, streamErr
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

func buildDefaultIterPool() encoding.MultiReaderIteratorPool {
	iterPool := encoding.NewMultiReaderIteratorPool(pool.NewObjectPoolOptions())
	iterPool.Init(
		func(r io.Reader, _ namespace.SchemaDescr) encoding.ReaderIterator {
			return m3tsz.NewReaderIterator(r,
				m3tsz.DefaultIntOptimizationEnabled,
				encoding.NewOptions())
		})
	return iterPool
}

// BuildNamespacesTester builds a NamespacesTester.
func BuildNamespacesTester(
	t *testing.T,
	runOpts RunOptions,
	ranges result.ShardTimeRanges,
	mds ...namespace.Metadata,
) NamespacesTester {

	return BuildNamespacesTesterWithReaderIteratorPool(
		t,
		runOpts,
		ranges,
		nil,
		mds...,
	)
}

// BuildNamespacesTesterWithReaderIteratorPool builds a NamespacesTester.
func BuildNamespacesTesterWithReaderIteratorPool(
	t *testing.T,
	runOpts RunOptions,
	ranges result.ShardTimeRanges,
	iterPool encoding.MultiReaderIteratorPool,
	mds ...namespace.Metadata,
) NamespacesTester {
	shards := make([]uint32, 0, len(ranges))
	for shard := range ranges {
		shards = append(shards, shard)
	}

	if iterPool == nil {
		iterPool = buildDefaultIterPool()
	}

	ctrl := gomock.NewController(t)
	namespacesMap := NewNamespacesMap(NamespacesMapOptions{})
	accumulators := make([]*TestDataAccumulator, 0, len(mds))
	for _, md := range mds {
		nsCtx := namespace.NewContextFrom(md)
		acc := &TestDataAccumulator{
			t:         t,
			ctrl:      ctrl,
			pool:      iterPool,
			ns:        md.ID().String(),
			results:   make(map[string]CheckoutSeriesResult),
			readerMap: make(ReaderMap),
			writeMap:  make(DecodedBlockMap),
			schema:    nsCtx.Schema,
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

// DumpValuesForNamespace dumps any accumulated blocks as decoded series.
func (nt *NamespacesTester) DumpValuesForNamespace(
	md namespace.Metadata,
) DecodedBlockMap {
	id := md.ID().String()
	for _, acc := range nt.Accumulators {
		if acc.ns == id {
			return acc.DumpValues()
		}
	}

	assert.FailNow(nt.t, fmt.Sprintf("namespace with id %s not found "+
		"valid namespaces are %v", id, nt.Namespaces))
	return nil
}

// DumpWrites dumps the writes encountered for all namespaces.
func (nt *NamespacesTester) DumpWrites() DecodedNamespaceMap {
	nsMap := make(DecodedNamespaceMap, len(nt.Accumulators))
	for _, acc := range nt.Accumulators {
		nsMap[acc.ns] = acc.writeMap
	}

	return nsMap
}

// DumpWritesForNamespace dumps the writes encountered for the given namespace.
func (nt *NamespacesTester) DumpWritesForNamespace(
	md namespace.Metadata,
) DecodedBlockMap {
	id := md.ID().String()
	for _, acc := range nt.Accumulators {
		if acc.ns == id {
			return acc.writeMap
		}
	}

	assert.FailNow(nt.t, fmt.Sprintf("namespace with id %s not found "+
		"valid namespaces are %v", id, nt.Namespaces))
	return nil
}

// DumpAllForNamespace dumps all results for a single namespace. The results are
// unsorted, so if that's important, they should be sorted prior to comparison.
func (nt *NamespacesTester) DumpAllForNamespace(
	md namespace.Metadata,
) (DecodedBlockMap, error) {
	id := md.ID().String()
	for _, acc := range nt.Accumulators {
		if acc.ns != id {
			continue
		}

		writeMap := acc.writeMap
		dumpMap := acc.DumpValues()
		merged := make(DecodedBlockMap, len(writeMap)+len(dumpMap))
		for k, v := range writeMap {
			merged[k] = v
		}

		for k, v := range dumpMap {
			if vals, found := merged[k]; found {
				merged[k] = append(vals, v...)
			} else {
				merged[k] = v
			}
		}

		return merged, nil
	}

	return nil, fmt.Errorf("namespace with id %s not found "+
		"valid namespaces are %v", id, nt.Namespaces)
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

func validateRanges(ac xtime.Ranges, ex xtime.Ranges) error {
	// Make range eclipses expected.
	removedRange := ex.RemoveRanges(ac)
	if !removedRange.IsEmpty() {
		return fmt.Errorf("actual range %v does not match expected range %v "+
			"diff: %v", ac, ex, removedRange)
	}

	// Now make sure no ranges outside of expected.
	expectedWithAddedRanges := ex.AddRanges(ac)

	if ex.Len() != expectedWithAddedRanges.Len() {
		return fmt.Errorf("expected with re-added ranges not equal")
	}

	iter := ex.Iter()
	withAddedRangesIter := expectedWithAddedRanges.Iter()
	for iter.Next() && withAddedRangesIter.Next() {
		if !iter.Value().Equal(withAddedRangesIter.Value()) {
			return fmt.Errorf("actual range %v does not match expected range %v",
				ac, ex)
		}
	}

	return nil
}

func validateShardTimeRanges(
	r result.ShardTimeRanges,
	ex result.ShardTimeRanges,
) error {
	if len(ex) != len(r) {
		return fmt.Errorf("expected %v and actual %v size mismatch", ex, r)
	}

	seen := make(map[uint32]struct{}, len(r))
	for k, val := range r {
		expectedVal, found := ex[k]
		if !found {
			return fmt.Errorf("expected shard map %v does not have shard %d; "+
				"actual: %v", ex, k, r)
		}

		if err := validateRanges(val, expectedVal); err != nil {
			return err
		}

		seen[k] = struct{}{}
	}

	for k := range ex {
		if _, beenFound := seen[k]; !beenFound {
			return fmt.Errorf("shard %d in actual not found in expected %v", k, ex)
		}
	}

	return nil
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
	require.NoError(nt.t, validateShardTimeRanges(actual, ex), "data")

	if md.Options().IndexOptions().Enabled() {
		actual := ns.IndexResult.Unfulfilled()
		require.NoError(nt.t, validateShardTimeRanges(actual, exIdx), "index")
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

// NamespaceMatcher is a matcher for namespaces.
type NamespaceMatcher struct {
	// Namespaces are the expected namespaces.
	Namespaces Namespaces
}

// String describes what the matcher matches.
func (m NamespaceMatcher) String() string { return "namespace query" }

// Matches returns whether x is a match.
func (m NamespaceMatcher) Matches(x interface{}) bool {
	ns, ok := x.(Namespaces)
	if !ok {
		return false
	}

	equalRange := func(a, b TargetRange) bool {
		return a.Range.Start.Equal(b.Range.Start) &&
			a.Range.End.Equal(b.Range.End)
	}

	for _, v := range ns.Namespaces.Iter() {
		other, found := m.Namespaces.Namespaces.Get(v.Key())
		if !found {
			return false
		}

		val := v.Value()
		if !other.Metadata.Equal(val.Metadata) {
			return false
		}

		if !equalRange(val.DataTargetRange, other.DataTargetRange) {
			return false
		}

		if !equalRange(val.IndexTargetRange, other.IndexTargetRange) {
			return false
		}
	}

	return true
}

// NB: assert NamespaceMatcher is a gomock.Matcher
var _ gomock.Matcher = (*NamespaceMatcher)(nil)

// ShardTimeRangesMatcher is a matcher for ShardTimeRanges.
type ShardTimeRangesMatcher struct {
	// Ranges are the expected ranges.
	Ranges map[uint32]xtime.Ranges
}

// Matches returns whether x is a match.
func (m ShardTimeRangesMatcher) Matches(x interface{}) bool {
	actual, ok := x.(result.ShardTimeRanges)
	if !ok {
		return false
	}

	if err := validateShardTimeRanges(m.Ranges, actual); err != nil {
		fmt.Println("shard time ranges do not match:", err.Error())
		return false
	}

	return true
}

// String describes what the matcher matches.
func (m ShardTimeRangesMatcher) String() string {
	return "shardTimeRangesMatcher"
}

// NB: assert ShardTimeRangesMatcher is a gomock.Matcher
var _ gomock.Matcher = (*ShardTimeRangesMatcher)(nil)
