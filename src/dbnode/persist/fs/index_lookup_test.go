// Copyright (c) 2017 Uber Technologies, Inc.
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
package fs

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/mmap"

	"github.com/stretchr/testify/require"
)

func TestNewNearestIndexOffsetDetectsUnsortedFiles(t *testing.T) {
	// Create a slice of out-of-order index summary entries
	outOfOrderSummaries := []schema.IndexSummary{
		{
			Index:            0,
			ID:               []byte("1"),
			IndexEntryOffset: 0,
		},
		{
			Index:            1,
			ID:               []byte("0"),
			IndexEntryOffset: 10,
		},
	}

	// Create a temp file
	file, err := ioutil.TempFile("", "index-lookup-sort")
	require.NoError(t, err)
	defer os.Remove(file.Name())

	// Write out the out-of-order summaries into the temp file
	writeSummariesEntries(t, file, outOfOrderSummaries)

	// Prepare the digest reader
	summariesFdWithDigest := digest.NewFdWithDigestReader(4096)
	file.Seek(0, 0)
	summariesFdWithDigest.Reset(file)

	// Determine the expected digest
	expectedDigest := calculateExpectedDigest(t, summariesFdWithDigest)

	// Reset the digest reader
	file.Seek(0, 0)
	summariesFdWithDigest.Reset(file)

	// Try and create the index lookup and make sure it detects the file is out
	// of order
	_, err = newNearestIndexOffsetLookupFromSummariesFile(
		summariesFdWithDigest,
		expectedDigest,
		msgpack.NewDecoder(nil),
		msgpack.NewByteDecoderStream(nil),
		len(outOfOrderSummaries),
		false,
		mmap.ReporterOptions{},
	)
	expectedErr := fmt.Errorf("summaries file is not sorted: %s", file.Name())
	require.Equal(t, expectedErr, err)
}

func TestCloneCannotBeCloned(t *testing.T) {
	indexLookup := newNearestIndexOffsetLookup(nil, mmap.Descriptor{})
	clone, err := indexLookup.concurrentClone()
	require.NoError(t, err)

	_, err = clone.concurrentClone()
	require.Error(t, err)
	require.NoError(t, indexLookup.close())
	require.NoError(t, clone.close())
}

func TestClosingCloneDoesNotAffectParent(t *testing.T) {
	indexSummaries := []schema.IndexSummary{
		{
			Index:            0,
			ID:               []byte("0"),
			IndexEntryOffset: 0,
		},
		{
			Index:            1,
			ID:               []byte("1"),
			IndexEntryOffset: 10,
		},
	}

	indexLookup := newIndexLookupWithSummaries(t, indexSummaries, false)
	clone, err := indexLookup.concurrentClone()
	require.NoError(t, err)
	require.NoError(t, clone.close())
	for _, summary := range indexSummaries {
		id := ident.StringID(string(summary.ID))
		require.NoError(t, err)
		offset, err := clone.getNearestIndexFileOffset(id, newTestReusableSeekerResources())
		require.NoError(t, err)
		require.Equal(t, summary.IndexEntryOffset, offset)
		id.Finalize()
	}
	require.NoError(t, indexLookup.close())
}

func TestParentAndClonesSafeForConcurrentUse(t *testing.T) {
	testParentAndClonesSafeForConcurrentUse(t, false)
}

func TestParentAndClonesSafeForConcurrentUseForceMmapMemory(t *testing.T) {
	testParentAndClonesSafeForConcurrentUse(t, true)
}

func testParentAndClonesSafeForConcurrentUse(t *testing.T, forceMmapMemory bool) {
	numSummaries := 1000
	numClones := 10

	// Create test summary entries
	indexSummaries := []schema.IndexSummary{}
	for i := 0; i < numSummaries; i++ {
		indexSummaries = append(indexSummaries, schema.IndexSummary{
			Index:            int64(i),
			ID:               []byte(strconv.Itoa(i)),
			IndexEntryOffset: int64(10 * i),
		})
	}
	sort.Sort(sortableSummaries(indexSummaries))

	// Create indexLookup and associated clones
	indexLookup := newIndexLookupWithSummaries(t, indexSummaries, forceMmapMemory)
	clones := []*nearestIndexOffsetLookup{}
	for i := 0; i < numClones; i++ {
		clone, err := indexLookup.concurrentClone()
		require.NoError(t, err)
		clones = append(clones, clone)
	}

	// Spin up a goroutine for each clone that looks up every offset. Use one waitgroup
	// to make a best effort attempt to get all the goroutines active before they start
	// doing work, and then another waitgroup to wait for them to all finish their work.
	startWg := sync.WaitGroup{}
	doneWg := sync.WaitGroup{}
	startWg.Add(len(clones) + 1)
	doneWg.Add(len(clones) + 1)

	lookupOffsetsFunc := func(clone *nearestIndexOffsetLookup) {
		startWg.Done()
		startWg.Wait()
		for _, summary := range indexSummaries {
			id := ident.StringID(string(summary.ID))
			offset, err := clone.getNearestIndexFileOffset(id, newTestReusableSeekerResources())
			require.NoError(t, err)
			require.Equal(t, summary.IndexEntryOffset, offset)
			id.Finalize()
		}
		doneWg.Done()
	}
	go lookupOffsetsFunc(indexLookup)
	for _, clone := range clones {
		go lookupOffsetsFunc(clone)
	}

	// Wait for all workers to finish and then make sure everything can be cleaned
	// up properly
	doneWg.Wait()
	require.NoError(t, indexLookup.close())
	for _, clone := range clones {
		require.NoError(t, clone.close())
	}
}

// newIndexLookupWithSummaries will return a new index lookup that is backed by the provided
// indexSummaries (in the order that they are provided).
func newIndexLookupWithSummaries(
	t *testing.T, indexSummaries []schema.IndexSummary, forceMmapMemory bool) *nearestIndexOffsetLookup {
	// Create a temp file
	file, err := ioutil.TempFile("", "index-lookup-sort")
	require.NoError(t, err)
	defer os.Remove(file.Name())

	writeSummariesEntries(t, file, indexSummaries)

	// Prepare the digest reader
	summariesFdWithDigest := digest.NewFdWithDigestReader(4096)
	file.Seek(0, 0)
	summariesFdWithDigest.Reset(file)

	// Determine the expected digest
	expectedDigest := calculateExpectedDigest(t, summariesFdWithDigest)

	// Reset the digest reader
	file.Seek(0, 0)
	summariesFdWithDigest.Reset(file)

	// Try and create the index lookup and make sure it detects the file is out
	// of order
	indexLookup, err := newNearestIndexOffsetLookupFromSummariesFile(
		summariesFdWithDigest,
		expectedDigest,
		msgpack.NewDecoder(nil),
		msgpack.NewByteDecoderStream(nil),
		len(indexSummaries),
		forceMmapMemory,
		mmap.ReporterOptions{},
	)
	require.NoError(t, err)
	return indexLookup
}

func writeSummariesEntries(t *testing.T, fd *os.File, summaries []schema.IndexSummary) {
	encoder := msgpack.NewEncoder()
	for _, summary := range summaries {
		encoder.Reset()
		require.NoError(t, encoder.EncodeIndexSummary(summary))
		_, err := fd.Write(encoder.Bytes())
		require.NoError(t, err)
	}
}

func calculateExpectedDigest(t *testing.T, digestReader digest.FdWithDigestReader) uint32 {
	// Determine the size of the file
	file := digestReader.Fd()
	stat, err := file.Stat()
	require.NoError(t, err)
	fileSize := stat.Size()

	// Calculate the digest
	_, err = digestReader.Read(make([]byte, fileSize))
	require.NoError(t, err)
	return digestReader.Digest().Sum32()
}

type sortableSummaries []schema.IndexSummary

func (s sortableSummaries) Len() int {
	return len(s)
}

func (s sortableSummaries) Less(i, j int) bool {
	return bytes.Compare(s[i].ID, s[j].ID) < 0
}

func (s sortableSummaries) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
