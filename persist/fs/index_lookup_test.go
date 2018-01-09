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
	"fmt"
	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/persist/fs/msgpack"
	"github.com/m3db/m3db/persist/schema"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

func TestNewNearestIndexOffsetDetectsUnsortedFiles(t *testing.T) {
	// Create a slice of out-of-order index summary entries
	outOfOrderSummaries := []schema.IndexSummary{
		schema.IndexSummary{
			Index:            0,
			ID:               []byte("1"),
			IndexEntryOffset: 0,
		},
		schema.IndexSummary{
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
		len(outOfOrderSummaries),
	)
	expectedErr := fmt.Errorf("summaries file is not sorted: %s", file.Name())
	require.Equal(t, expectedErr, err)
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
