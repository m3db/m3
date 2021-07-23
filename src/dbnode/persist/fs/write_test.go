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

package fs

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
	"github.com/stretchr/testify/require"
)

// TestWriteReuseAfterError was added as a regression test after it was
// discovered that reusing a fileset writer after a called to Close() had
// returned an error could make the fileset writer end up in a near infinite
// loop when it was reused to write out a completely indepedent set of files.
//
// This test verifies that the fix works as expected and prevents regressions
// of the issue.
func TestWriteReuseAfterError(t *testing.T) {
	dir := createTempDir(t)
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w := newTestWriter(t, filePathPrefix)
	writerOpts := DataWriterOpenOptions{
		Identifier: FileSetFileIdentifier{
			Namespace:   testNs1ID,
			Shard:       0,
			BlockStart:  xtime.Now().Truncate(time.Hour),
			VolumeIndex: 0,
		},
		BlockSize:   time.Hour,
		FileSetType: persist.FileSetFlushType,
	}
	data := checkedBytes([]byte{1, 2, 3})

	require.NoError(t, w.Open(writerOpts))
	require.NoError(t, w.Write(
		persist.NewMetadataFromIDAndTags(
			ident.StringID("series1"),
			ident.Tags{},
			persist.MetadataOptions{}),
		data,
		0))
	require.NoError(t, w.Write(
		persist.NewMetadataFromIDAndTags(
			ident.StringID("series1"),
			ident.Tags{},
			persist.MetadataOptions{}),
		data,
		0))
	require.Error(t, w.Close())

	require.NoError(t, w.Open(writerOpts))
	require.NoError(t, w.Close())
}
