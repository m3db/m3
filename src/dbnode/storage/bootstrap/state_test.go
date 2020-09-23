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
	"github.com/m3db/m3/src/x/instrument"

	"github.com/stretchr/testify/require"
)

var (
	testBlockSize             = 2 * time.Hour
	testStart                 = time.Now().Truncate(testBlockSize)
	testNamespaceIndexOptions = namespace.NewIndexOptions()
	testNamespaceOptions      = namespace.NewOptions()
	testRetentionOptions      = retention.NewOptions()
	testFilesystemOptions     = fs.NewOptions()
)

func TestStateReadInfoFiles(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	md1 := testNamespaceMetadata(t, ident.StringID("ns1"))
	md2 := testNamespaceMetadata(t, ident.StringID("ns2"))

	fsOpts := testFilesystemOptions.SetFilePathPrefix(dir)

	writeFilesets(t, md1.ID(), 0, fsOpts)
	writeFilesets(t, md1.ID(), 1, fsOpts)
	writeFilesets(t, md2.ID(), 0, fsOpts)
	writeFilesets(t, md2.ID(), 1, fsOpts)

	opts := NewStateOptions().
		SetFilesystemOptions(fsOpts).
		SetInstrumentOptions(fsOpts.InstrumentOptions()).
		SetInfoFilesFinders([]InfoFilesFinder{
			{
				Namespace: md1,
				Shards:    []uint32{0, 1},
			},
			{
				Namespace: md2,
				Shards:    []uint32{0, 1},
			},
		})
	state, err := NewState(opts)
	require.NoError(t, err)

	infoFilesByNamespace := state.ReadInfoFiles()
	require.NotEmpty(t, infoFilesByNamespace)

	// Ensure we have two namespaces.
	require.Equal(t, 2, len(infoFilesByNamespace))

	// Ensure we have two shards.
	require.Equal(t, 2, len(state.InfoFilesForNamespace(md1)))
	require.Equal(t, 2, len(state.InfoFilesForNamespace(md2)))

	// Ensure each shard has three info files (one for each fileset written).
	for shard := uint32(0); shard < 2; shard++ {
		require.Equal(t, 3, len(state.InfoFilesForShard(md1, shard)))
		require.Equal(t, 3, len(state.InfoFilesForShard(md2, shard)))
	}
}

func TestStateReadInfoFilesInvariantViolation(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	md1 := testNamespaceMetadata(t, ident.StringID("ns1"))
	md2 := testNamespaceMetadata(t, ident.StringID("ns2"))

	fsOpts := testFilesystemOptions.SetFilePathPrefix(dir)
	writeFilesets(t, md1.ID(), 0, fsOpts)

	opts := NewStateOptions().
		SetFilesystemOptions(fsOpts).
		SetInstrumentOptions(fsOpts.InstrumentOptions()).
		SetInfoFilesFinders([]InfoFilesFinder{
			{
				Namespace: md1,
				Shards:    []uint32{0, 1},
			},
		})
	state, err := NewState(opts)
	require.NoError(t, err)

	// Force invariant violations to panic for easier testability.
	os.Setenv(instrument.ShouldPanicEnvironmentVariableName, "true")
	defer os.Setenv(instrument.ShouldPanicEnvironmentVariableName, "false")

	require.Panics(t, func() {
		state.InfoFilesForNamespace(md2)
	})

	require.Panics(t, func() {
		state.InfoFilesForShard(md1, 12)
	})
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
		start time.Time
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
	start time.Time,
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
