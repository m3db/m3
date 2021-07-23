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

package main

import (
	"encoding/hex"
	"io/ioutil"
	"os"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var (
	testBytesPool pool.CheckedBytesPool
)

func init() {
	testBytesPool = pool.NewCheckedBytesPool(nil, pool.NewObjectPoolOptions(),
		func(s []pool.Bucket) pool.BytesPool {
			return pool.NewBytesPool(s, pool.NewObjectPoolOptions())
		})
	testBytesPool.Init()
}

func newInvalidUTF8Bytes(t *testing.T) []byte {
	bytes, err := hex.DecodeString("bf")
	require.NoError(t, err)
	require.False(t, utf8.Valid(bytes))
	return bytes
}

type testWriter struct {
	t         *testing.T
	fileSetID fs.FileSetFileIdentifier
	writer    fs.DataFileSetWriter
	blockSize time.Duration
	testDir   string
}

func (w testWriter) Cleanup() {
	require.NoError(w.t, os.RemoveAll(w.testDir))
}

func newOpenWriter(t *testing.T) testWriter {
	testDir, err := ioutil.TempDir("", "m3db-data")
	require.NoError(t, err)

	fsOpts := fs.NewOptions().SetFilePathPrefix(testDir)
	writer, err := fs.NewWriter(fsOpts)
	require.NoError(t, err)

	blockSize := 2 * time.Hour
	start := xtime.Now().Truncate(blockSize).Add(-2 * blockSize)
	fileSetID := fs.FileSetFileIdentifier{
		FileSetContentType: persist.FileSetDataContentType,
		Namespace:          ident.StringID("foo"),
		BlockStart:         start,
		Shard:              42,
		VolumeIndex:        0,
	}
	err = writer.Open(fs.DataWriterOpenOptions{
		FileSetType:        persist.FileSetFlushType,
		FileSetContentType: fileSetID.FileSetContentType,
		Identifier:         fileSetID,
		BlockSize:          blockSize,
	})
	require.NoError(t, err)

	return testWriter{
		t:         t,
		fileSetID: fileSetID,
		writer:    writer,
		blockSize: blockSize,
		testDir:   testDir,
	}
}

func TestFixFileSetInvalidID(t *testing.T) {
	testWriter := newOpenWriter(t)
	defer testWriter.Cleanup()

	fixDir, err := ioutil.TempDir("", "m3db-fix-data")
	require.NoError(t, err)
	defer os.RemoveAll(fixDir)

	// Write invalid ID.
	invalidBytes := newInvalidUTF8Bytes(t)
	id := ident.BinaryID(checked.NewBytes(invalidBytes, nil))
	data := checked.NewBytes([]byte{1, 2, 3}, nil)
	data.IncRef()
	checksum := digest.Checksum(data.Bytes())

	writer := testWriter.writer
	metadata := persist.NewMetadataFromIDAndTags(id, ident.Tags{},
		persist.MetadataOptions{})
	err = writer.Write(metadata, data, checksum)
	require.NoError(t, err)

	// Write valid ID.
	id = ident.StringID("foo")
	metadata = persist.NewMetadataFromIDAndTags(id, ident.Tags{},
		persist.MetadataOptions{})
	err = writer.Write(metadata, data, checksum)
	require.NoError(t, err)

	// Close volume.
	err = writer.Close()
	require.NoError(t, err)

	// Fix the volume.
	run(runOptions{
		filePathPrefix: testWriter.testDir,
		fixDir:         fixDir,
		fixInvalidIDs:  true,
		bytesPool:      testBytesPool,
		log:            zap.NewExample(),
	})

	// Read back the volume from fix dir.
	reader, err := fs.NewReader(nil, fs.NewOptions().SetFilePathPrefix(fixDir))
	require.NoError(t, err)

	err = reader.Open(fs.DataReaderOpenOptions{
		Identifier:  testWriter.fileSetID,
		FileSetType: persist.FileSetFlushType,
	})
	require.NoError(t, err)

	require.Equal(t, 1, reader.Entries())

	readID, _, _, _, err := reader.Read()
	require.NoError(t, err)

	require.Equal(t, "foo", readID.String())

	err = reader.Close()
	require.NoError(t, err)
}

func TestFixFileSetInvalidTags(t *testing.T) {
	testWriter := newOpenWriter(t)
	defer testWriter.Cleanup()

	fixDir, err := ioutil.TempDir("", "m3db-fix-data")
	require.NoError(t, err)
	defer os.RemoveAll(fixDir)

	writer := testWriter.writer

	// Write invalid tags.
	invalidBytes := newInvalidUTF8Bytes(t)
	id := ident.StringID("foo")
	tags := ident.NewTags(ident.Tag{
		Name:  ident.BinaryID(checked.NewBytes(invalidBytes, nil)),
		Value: ident.StringID("bar"),
	}, ident.Tag{
		Name:  ident.StringID("baz"),
		Value: ident.StringID("qux"),
	}, ident.Tag{
		Name:  ident.StringID("qar"),
		Value: ident.StringID("qaz"),
	})
	data := checked.NewBytes([]byte{1, 2, 3}, nil)
	data.IncRef()
	checksum := digest.Checksum(data.Bytes())

	metadata := persist.NewMetadataFromIDAndTags(id, tags,
		persist.MetadataOptions{})
	err = writer.Write(metadata, data, checksum)
	require.NoError(t, err)

	// Write valid tags.
	id = ident.StringID("bar")
	tags = ident.NewTags(ident.Tag{
		Name:  ident.StringID("foo"),
		Value: ident.StringID("bar"),
	}, ident.Tag{
		Name:  ident.StringID("baz"),
		Value: ident.StringID("qux"),
	}, ident.Tag{
		Name:  ident.StringID("qar"),
		Value: ident.StringID("qaz"),
	})
	data = checked.NewBytes([]byte{1, 2, 3}, nil)
	data.IncRef()
	checksum = digest.Checksum(data.Bytes())

	metadata = persist.NewMetadataFromIDAndTags(id, tags,
		persist.MetadataOptions{})
	err = writer.Write(metadata, data, checksum)
	require.NoError(t, err)

	// Close volume.
	err = writer.Close()
	require.NoError(t, err)

	// Fix the volume.
	run(runOptions{
		filePathPrefix: testWriter.testDir,
		fixDir:         fixDir,
		fixInvalidTags: true,
		bytesPool:      testBytesPool,
		log:            zap.NewExample(),
	})

	// Read back the volume from fix dir.
	reader, err := fs.NewReader(nil, fs.NewOptions().SetFilePathPrefix(fixDir))
	require.NoError(t, err)

	err = reader.Open(fs.DataReaderOpenOptions{
		Identifier:  testWriter.fileSetID,
		FileSetType: persist.FileSetFlushType,
	})
	require.NoError(t, err)

	require.Equal(t, 1, reader.Entries())

	readID, readTags, _, _, err := reader.Read()
	require.NoError(t, err)

	require.Equal(t, "bar", readID.String())

	tagsMap := make(map[string]string)
	for readTags.Next() {
		tag := readTags.Current()
		tagsMap[tag.Name.String()] = tag.Value.String()
	}
	require.NoError(t, readTags.Err())
	readTags.Close()

	require.Equal(t, map[string]string{"foo": "bar", "baz": "qux", "qar": "qaz"}, tagsMap)

	err = reader.Close()
	require.NoError(t, err)
}

func TestFixFileSetInvalidChecksum(t *testing.T) {
	testWriter := newOpenWriter(t)
	defer testWriter.Cleanup()

	fixDir, err := ioutil.TempDir("", "m3db-fix-data")
	require.NoError(t, err)
	defer os.RemoveAll(fixDir)

	writer := testWriter.writer

	// Write invalid checksum.
	id := ident.StringID("foo")
	tags := ident.NewTags(ident.Tag{
		Name:  ident.StringID("foo"),
		Value: ident.StringID("bar"),
	}, ident.Tag{
		Name:  ident.StringID("baz"),
		Value: ident.StringID("qux"),
	}, ident.Tag{
		Name:  ident.StringID("qar"),
		Value: ident.StringID("qaz"),
	})
	data := checked.NewBytes([]byte{1, 2, 3}, nil)
	data.IncRef()
	checksum := digest.Checksum(data.Bytes()) + 1

	metadata := persist.NewMetadataFromIDAndTags(id, tags,
		persist.MetadataOptions{})
	err = writer.Write(metadata, data, checksum)
	require.NoError(t, err)

	// Write valid checksum.
	id = ident.StringID("bar")
	tags = ident.NewTags(ident.Tag{
		Name:  ident.StringID("foo"),
		Value: ident.StringID("bar"),
	}, ident.Tag{
		Name:  ident.StringID("baz"),
		Value: ident.StringID("qux"),
	}, ident.Tag{
		Name:  ident.StringID("qar"),
		Value: ident.StringID("qaz"),
	})
	data = checked.NewBytes([]byte{1, 2, 3}, nil)
	data.IncRef()
	checksum = digest.Checksum(data.Bytes())

	metadata = persist.NewMetadataFromIDAndTags(id, tags,
		persist.MetadataOptions{})
	err = writer.Write(metadata, data, checksum)
	require.NoError(t, err)

	// Close volume.
	err = writer.Close()
	require.NoError(t, err)

	// Fix the volume.
	run(runOptions{
		filePathPrefix:      testWriter.testDir,
		fixDir:              fixDir,
		fixInvalidChecksums: true,
		bytesPool:           testBytesPool,
		log:                 zap.NewExample(),
	})

	// Read back the volume from fix dir.
	reader, err := fs.NewReader(nil, fs.NewOptions().SetFilePathPrefix(fixDir))
	require.NoError(t, err)

	err = reader.Open(fs.DataReaderOpenOptions{
		Identifier:  testWriter.fileSetID,
		FileSetType: persist.FileSetFlushType,
	})
	require.NoError(t, err)

	require.Equal(t, 1, reader.Entries())

	readID, readTags, _, _, err := reader.Read()
	require.NoError(t, err)

	require.Equal(t, "bar", readID.String())

	tagsMap := make(map[string]string)
	for readTags.Next() {
		tag := readTags.Current()
		tagsMap[tag.Name.String()] = tag.Value.String()
	}
	require.NoError(t, readTags.Err())
	readTags.Close()

	require.Equal(t, map[string]string{"foo": "bar", "baz": "qux", "qar": "qaz"}, tagsMap)

	err = reader.Close()
	require.NoError(t, err)
}
