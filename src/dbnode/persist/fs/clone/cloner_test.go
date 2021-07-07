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

package clone

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

const (
	numTestSeries = 100
)

var (
	testBytes = checked.NewBytes([]byte("somelongstringofdata"), nil)
)

func TestCloner(t *testing.T) {
	dir, err := ioutil.TempDir("", "clone")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opts := NewOptions()

	// generate some fake source data
	srcBlockSize := time.Hour
	srcData := path.Join(dir, "src")
	require.NoError(t, os.Mkdir(srcData, opts.DirMode()))
	src := FileSetID{
		PathPrefix: srcData,
		Namespace:  "testns-src",
		Shard:      123,
		Blockstart: xtime.Now().Truncate(srcBlockSize),
	}
	testBytes.IncRef()
	defer testBytes.DecRef()
	writeTestData(t, srcBlockSize, src, opts)

	// clone it
	destBlockSize := 2 * time.Hour
	clonedData := path.Join(dir, "clone")
	require.NoError(t, os.Mkdir(clonedData, opts.DirMode()))
	dest := FileSetID{
		PathPrefix: clonedData,
		Namespace:  "testns-dest",
		Shard:      321,
		Blockstart: xtime.Now().Add(-1 * 24 * 30 * time.Hour).Truncate(destBlockSize),
	}
	cloner := New(opts)
	require.NoError(t, cloner.Clone(src, dest, destBlockSize))

	// verify the two are equal
	r1, err := fs.NewReader(opts.BytesPool(), fs.NewOptions().
		SetFilePathPrefix(src.PathPrefix).
		SetDataReaderBufferSize(opts.BufferSize()).
		SetInfoReaderBufferSize(opts.BufferSize()).
		SetDecodingOptions(opts.DecodingOptions()))
	require.NoError(t, err)
	r1OpenOpts := fs.DataReaderOpenOptions{
		Identifier: fs.FileSetFileIdentifier{
			Namespace:  ident.StringID(src.Namespace),
			Shard:      src.Shard,
			BlockStart: src.Blockstart,
		},
	}
	require.NoError(t, r1.Open(r1OpenOpts))
	r2, err := fs.NewReader(opts.BytesPool(), fs.NewOptions().
		SetFilePathPrefix(dest.PathPrefix).
		SetDataReaderBufferSize(opts.BufferSize()).
		SetInfoReaderBufferSize(opts.BufferSize()).
		SetDecodingOptions(opts.DecodingOptions()))
	require.NoError(t, err)
	r2OpenOpts := fs.DataReaderOpenOptions{
		Identifier: fs.FileSetFileIdentifier{
			Namespace:  ident.StringID(dest.Namespace),
			Shard:      dest.Shard,
			BlockStart: dest.Blockstart,
		},
	}
	require.NoError(t, r2.Open(r2OpenOpts))
	for {
		t1, a1, b1, c1, e1 := r1.Read()
		t2, a2, b2, c2, e2 := r2.Read()
		if e1 == e2 && e1 == io.EOF {
			break
		}

		require.NoError(t, e1)
		require.NoError(t, e2)

		b1.IncRef()
		b2.IncRef()
		require.Equal(t, t1.String(), t2.String())
		require.True(t, ident.NewTagIterMatcher(a1).Matches(a2))
		require.Equal(t, b1.Bytes(), b2.Bytes())
		require.Equal(t, c1, c2)
		b1.DecRef()
		b2.DecRef()
	}
	require.NoError(t, r1.Close())
	require.NoError(t, r2.Close())
}

func writeTestData(t *testing.T, bs time.Duration, src FileSetID, opts Options) {
	w, err := fs.NewWriter(fs.NewOptions().
		SetFilePathPrefix(src.PathPrefix).
		SetWriterBufferSize(opts.BufferSize()).
		SetNewFileMode(opts.FileMode()).
		SetNewDirectoryMode(opts.DirMode()))
	require.NoError(t, err)
	writerOpts := fs.DataWriterOpenOptions{
		BlockSize: bs,
		Identifier: fs.FileSetFileIdentifier{
			Namespace:  ident.StringID(src.Namespace),
			Shard:      src.Shard,
			BlockStart: src.Blockstart,
		},
	}
	require.NoError(t, w.Open(writerOpts))
	for i := 0; i < numTestSeries; i++ {
		id := ident.StringID(fmt.Sprintf("test-series.%d", i))
		var tags ident.Tags
		if i%2 == 0 {
			tags = ident.NewTags(
				ident.StringTag("foo", "bar"),
				ident.StringTag("qux", "qaz"),
			)
		}
		metadata := persist.NewMetadataFromIDAndTags(id, tags,
			persist.MetadataOptions{})
		require.NoError(t, w.Write(metadata, testBytes, 1234))
	}
	require.NoError(t, w.Close())
}
