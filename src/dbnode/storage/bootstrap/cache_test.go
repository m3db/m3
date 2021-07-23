// Copyright (c) 2020  Uber Technologies, Inc.
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
	"io/ioutil"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

var (
	testBlockSize             = 2 * time.Hour
	testStart                 = xtime.Now().Truncate(testBlockSize)
	testNamespaceIndexOptions = namespace.NewIndexOptions()
	testNamespaceOptions      = namespace.NewOptions()
	testRetentionOptions      = retention.NewOptions()
	testFilesystemOptions     = fs.NewOptions()
)

func TestCacheReadInfoFiles(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	md1 := testNamespaceMetadata(t, ident.StringID("ns1"))
	md2 := testNamespaceMetadata(t, ident.StringID("ns2"))

	fsOpts := testFilesystemOptions.SetFilePathPrefix(dir)

	writeFilesets(t, md1.ID(), 0, fsOpts)
	writeFilesets(t, md1.ID(), 1, fsOpts)
	writeFilesets(t, md2.ID(), 0, fsOpts)
	writeFilesets(t, md2.ID(), 1, fsOpts)

	opts := NewCacheOptions().
		SetFilesystemOptions(fsOpts).
		SetInstrumentOptions(fsOpts.InstrumentOptions()).
		SetNamespaceDetails([]NamespaceDetails{
			{
				Namespace: md1,
				Shards:    []uint32{0, 1},
			},
			{
				Namespace: md2,
				Shards:    []uint32{0, 1},
			},
		})
	cache, err := NewCache(opts)
	require.NoError(t, err)

	infoFilesByNamespace := cache.ReadInfoFiles()
	require.NotEmpty(t, infoFilesByNamespace)

	// Ensure we have two namespaces.
	require.Equal(t, 2, len(infoFilesByNamespace))

	// Ensure we have two shards.
	filesByShard, err := cache.InfoFilesForNamespace(md1)
	require.NoError(t, err)
	require.Equal(t, 2, len(filesByShard))

	filesByShard, err = cache.InfoFilesForNamespace(md2)
	require.NoError(t, err)
	require.Equal(t, 2, len(filesByShard))

	// Ensure each shard has three info files (one for each fileset written).
	for shard := uint32(0); shard < 2; shard++ {
		infoFiles, err := cache.InfoFilesForShard(md1, shard)
		require.NoError(t, err)
		require.Equal(t, 3, len(infoFiles))

		infoFiles, err = cache.InfoFilesForShard(md2, shard)
		require.NoError(t, err)
		require.Equal(t, 3, len(infoFiles))
	}
}

func TestCacheReadInfoFilesInvariantViolation(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	md1 := testNamespaceMetadata(t, ident.StringID("ns1"))
	md2 := testNamespaceMetadata(t, ident.StringID("ns2"))

	fsOpts := testFilesystemOptions.SetFilePathPrefix(dir)
	writeFilesets(t, md1.ID(), 0, fsOpts)

	opts := NewCacheOptions().
		SetFilesystemOptions(fsOpts).
		SetInstrumentOptions(fsOpts.InstrumentOptions()).
		SetNamespaceDetails([]NamespaceDetails{
			{
				Namespace: md1,
				Shards:    []uint32{0, 1},
			},
		})
	cache, err := NewCache(opts)
	require.NoError(t, err)

	_, err = cache.InfoFilesForNamespace(md2)
	require.Error(t, err)

	_, err = cache.InfoFilesForShard(md1, 12)
	require.Error(t, err)
}

func testNamespaceMetadata(t *testing.T, nsID ident.ID) namespace.Metadata {
	rOpts := testRetentionOptions.SetBlockSize(testBlockSize)
	md, err := namespace.NewMetadata(nsID, testNamespaceOptions.
		SetRetentionOptions(rOpts).
		SetIndexOptions(testNamespaceIndexOptions))
	require.NoError(t, err)
	return md
}

func createTempDir(t *testing.T) string {
	dir, err := ioutil.TempDir("", "foo")
	require.NoError(t, err)
	return dir
}

type testSeries struct {
	id   string
	tags map[string]string
	data []byte
}

func writeFilesets(t *testing.T, namespace ident.ID, shard uint32, fsOpts fs.Options) {
	inputs := []struct {
		start xtime.UnixNano
		id    string
		tags  map[string]string
		data  []byte
	}{
		{testStart, "foo", map[string]string{"n": "0"}, []byte{1, 2, 3}},
		{testStart.Add(10 * time.Hour), "bar", map[string]string{"n": "1"}, []byte{4, 5, 6}},
		{testStart.Add(20 * time.Hour), "baz", nil, []byte{7, 8, 9}},
	}

	for _, input := range inputs {
		writeTSDBFiles(t, namespace, shard, input.start,
			[]testSeries{{input.id, input.tags, input.data}}, fsOpts)
	}
}

func writeTSDBFiles(
	t require.TestingT,
	namespace ident.ID,
	shard uint32,
	start xtime.UnixNano,
	series []testSeries,
	opts fs.Options,
) {
	w, err := fs.NewWriter(opts)
	require.NoError(t, err)
	writerOpts := fs.DataWriterOpenOptions{
		Identifier: fs.FileSetFileIdentifier{
			Namespace:  namespace,
			Shard:      shard,
			BlockStart: start,
		},
		BlockSize: testBlockSize,
	}
	require.NoError(t, w.Open(writerOpts))

	for _, v := range series {
		bytes := checked.NewBytes(v.data, nil)
		bytes.IncRef()
		metadata := persist.NewMetadataFromIDAndTags(ident.StringID(v.id), sortedTagsFromTagsMap(v.tags),
			persist.MetadataOptions{})
		require.NoError(t, w.Write(metadata, bytes, digest.Checksum(bytes.Bytes())))
		bytes.DecRef()
	}

	require.NoError(t, w.Close())
}

func sortedTagsFromTagsMap(tags map[string]string) ident.Tags {
	var (
		seriesTags ident.Tags
		tagNames   []string
	)
	for name := range tags {
		tagNames = append(tagNames, name)
	}
	sort.Strings(tagNames)
	for _, name := range tagNames {
		seriesTags.Append(ident.StringTag(name, tags[name]))
	}
	return seriesTags
}
