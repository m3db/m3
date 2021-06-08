// Copyright (c) 2020 Uber Technologies, Inc.
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

package migration

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

func TestToVersion1_1Run(t *testing.T) {
	dir := createTempDir(t)
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	var shard uint32 = 1
	nsID := ident.StringID("foo")

	// Write unmigrated fileset to disk
	fsOpts := writeUnmigratedData(t, filePathPrefix, nsID, shard)

	// Read info file of just written fileset
	results := fs.ReadInfoFiles(filePathPrefix, nsID, shard,
		fsOpts.InfoReaderBufferSize(), fsOpts.DecodingOptions(), persist.FileSetFlushType)
	require.Equal(t, 1, len(results))
	infoFileResult := results[0]
	indexFd := openFile(t, fsOpts, nsID, shard, infoFileResult, "index")
	oldBytes, err := ioutil.ReadAll(indexFd)
	require.NoError(t, err)

	// Configure and run migration
	pm, err := fs.NewPersistManager(
		fsOpts.SetEncodingOptions(msgpack.DefaultLegacyEncodingOptions)) // Set encoder to most up-to-date version
	require.NoError(t, err)
	icm, err := fs.NewIndexClaimsManager(fsOpts)
	require.NoError(t, err)

	md, err := namespace.NewMetadata(nsID, namespace.NewOptions())
	require.NoError(t, err)

	plCache, err := index.NewPostingsListCache(1, index.PostingsListCacheOptions{
		InstrumentOptions: instrument.NewOptions(),
	})
	require.NoError(t, err)
	defer plCache.Start()()

	opts := NewTaskOptions().
		SetNewMergerFn(fs.NewMerger).
		SetPersistManager(pm).
		SetNamespaceMetadata(md).
		SetStorageOptions(storage.DefaultTestOptions().
			SetPersistManager(pm).
			SetIndexClaimsManager(icm).
			SetNamespaceInitializer(namespace.NewStaticInitializer([]namespace.Metadata{md})).
			SetRepairEnabled(false).
			SetIndexOptions(index.NewOptions().
				SetPostingsListCache(plCache)).
			SetBlockLeaseManager(block.NewLeaseManager(nil))).
		SetShard(shard).
		SetInfoFileResult(infoFileResult).
		SetFilesystemOptions(fsOpts)

	task, err := NewToVersion1_1Task(opts)
	require.NoError(t, err)

	updatedInfoFile, err := task.Run()
	require.NoError(t, err)

	// Read new info file and make sure it matches results returned by task
	newInfoFd := openFile(t, fsOpts, nsID, shard, updatedInfoFile, "info")

	newInfoBytes, err := ioutil.ReadAll(newInfoFd)
	require.NoError(t, err)

	decoder := msgpack.NewDecoder(nil)
	decoder.Reset(msgpack.NewByteDecoderStream(newInfoBytes))
	info, err := decoder.DecodeIndexInfo()

	require.Equal(t, updatedInfoFile.Info, info)

	// Read the index entries of new volume set
	indexFd = openFile(t, fsOpts, nsID, shard, updatedInfoFile, "index")
	newBytes, err := ioutil.ReadAll(indexFd)
	require.NoError(t, err)

	// Diff bytes of unmigrated vs migrated fileset
	require.NotEqual(t, oldBytes, newBytes)

	// Corrupt bytes to trip newly added checksum
	newBytes[len(newBytes)-1] = 1 + newBytes[len(newBytes)-1]
	decoder.Reset(msgpack.NewByteDecoderStream(newBytes))
	_, err = decoder.DecodeIndexEntry(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "checksum mismatch")
}

func openFile(
	t *testing.T,
	fsOpts fs.Options,
	nsID ident.ID,
	shard uint32,
	infoFileResult fs.ReadInfoFileResult,
	fileType string,
) *os.File {
	indexFd, err := os.Open(path.Join(fsOpts.FilePathPrefix(), fmt.Sprintf("data/%s/%d/fileset-%d-%d-%s.db",
		nsID.String(), shard, infoFileResult.Info.BlockStart, infoFileResult.Info.VolumeIndex, fileType)))
	require.NoError(t, err)
	return indexFd
}

func writeUnmigratedData(
	t *testing.T,
	filePathPrefix string,
	nsID ident.ID,
	shard uint32,
) fs.Options {
	// Use encoding options that will not generate entry level checksums
	eOpts := msgpack.LegacyEncodingOptions{EncodeLegacyIndexEntryVersion: msgpack.LegacyEncodingIndexEntryVersionV2}

	// Write data
	fsOpts := fs.NewOptions().
		SetFilePathPrefix(filePathPrefix).
		SetEncodingOptions(eOpts)
	w, err := fs.NewWriter(fsOpts)
	require.NoError(t, err)

	blockStart := xtime.Now().Truncate(time.Hour)
	writerOpts := fs.DataWriterOpenOptions{
		Identifier: fs.FileSetFileIdentifier{
			Namespace:   nsID,
			Shard:       shard,
			BlockStart:  blockStart,
			VolumeIndex: 0,
		},
		BlockSize: 2 * time.Hour,
	}
	err = w.Open(writerOpts)
	require.NoError(t, err)

	entry := []byte{1, 2, 3}

	chkdBytes := checked.NewBytes(entry, nil)
	chkdBytes.IncRef()
	metadata := persist.NewMetadataFromIDAndTags(ident.StringID("foo"),
		ident.Tags{}, persist.MetadataOptions{})
	err = w.Write(metadata, chkdBytes, digest.Checksum(entry))
	require.NoError(t, err)

	err = w.Close()
	require.NoError(t, err)

	return fsOpts
}

func createTempDir(t *testing.T) string {
	dir, err := ioutil.TempDir("", "testdir")
	require.NoError(t, err)

	return dir
}
