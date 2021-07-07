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

package storage

import (
	"errors"
	"testing"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestNamespaceReadersGet(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	metadata, err := namespace.NewMetadata(defaultTestNs1ID,
		defaultTestNs1Opts.SetColdWritesEnabled(true))
	require.NoError(t, err)
	shard := uint32(0)
	blockSize := metadata.Options().RetentionOptions().BlockSize()
	start := xtime.Now().Truncate(blockSize)

	mockBlockLeaseMgr := block.NewMockLeaseManager(ctrl)
	mockBlockLeaseMgr.EXPECT().RegisterLeaser(gomock.Any()).Return(nil)
	// Case 1: open new reader.
	mockBlockLeaseMgr.EXPECT().OpenLatestLease(gomock.Any(), gomock.Any()).
		Return(block.LeaseState{Volume: 0}, nil)
	// Case 2: open new reader and fast forward to the middle of the volume as
	// specified by the reader position.
	mockBlockLeaseMgr.EXPECT().OpenLatestLease(gomock.Any(), gomock.Any()).
		Return(block.LeaseState{Volume: 0}, nil)
	// Case 3: request a reader for the middle of volume 1, but the latest
	// volume is 3, so open a new reader for volume 3 from the beginning.
	mockBlockLeaseMgr.EXPECT().OpenLatestLease(gomock.Any(), gomock.Any()).
		Return(block.LeaseState{Volume: 3}, nil)
	// Case 4: request for a reader that we have cached, so make sure we use
	// the cached reader.
	mockBlockLeaseMgr.EXPECT().OpenLatestLease(gomock.Any(), gomock.Any()).
		Return(block.LeaseState{Volume: 2}, nil)
	// Case 5: request for a reader that we don't have cached. However, we do
	// have a closed reader so we should reuse that one.
	mockBlockLeaseMgr.EXPECT().OpenLatestLease(gomock.Any(), gomock.Any()).
		Return(block.LeaseState{Volume: 1}, nil)

	nsReaderMgr := newNamespaceReaderManager(metadata, tally.NoopScope,
		DefaultTestOptions().SetBlockLeaseManager(mockBlockLeaseMgr))
	nsReaderMgrImpl := nsReaderMgr.(*namespaceReaderManager)
	// Grabbing specific ID here so that the test can match on gomock arguments.
	nsID := nsReaderMgrImpl.namespace.ID()
	mockFSReader := fs.NewMockDataFileSetReader(ctrl)
	// Case 1.
	mockFSReader.EXPECT().Open(fs.DataReaderOpenOptions{
		Identifier: fs.FileSetFileIdentifier{
			Namespace: nsID, Shard: shard, BlockStart: start,
			VolumeIndex: 0,
		},
	}).Return(nil)
	mockFSReader.EXPECT().ValidateMetadata().Return(nil)
	// Case 2.
	mockFSReader.EXPECT().Open(fs.DataReaderOpenOptions{
		Identifier: fs.FileSetFileIdentifier{
			Namespace: nsID, Shard: shard, BlockStart: start,
			VolumeIndex: 0,
		},
	}).Return(nil)
	mockFSReader.EXPECT().ValidateMetadata().Return(nil)
	mockFSReader.EXPECT().Read().Return(ident.StringID("id"),
		ident.NewTagsIterator(ident.Tags{}), checked.NewBytes([]byte{}, nil),
		uint32(0), nil).Times(2)
	mockFSReader.EXPECT().ReadMetadata().Return(ident.StringID("id"),
		ident.NewTagsIterator(ident.Tags{}), 0, uint32(0), nil).Times(3)
	// Case 3.
	mockFSReader.EXPECT().Open(fs.DataReaderOpenOptions{
		Identifier: fs.FileSetFileIdentifier{
			Namespace: nsID, Shard: shard, BlockStart: start,
			VolumeIndex: 3,
		},
	}).Return(nil)
	mockFSReader.EXPECT().ValidateMetadata().Return(nil)

	nsReaderMgrImpl.newReaderFn = func(
		bytesPool pool.CheckedBytesPool,
		opts fs.Options,
	) (fs.DataFileSetReader, error) {
		return mockFSReader, nil
	}

	// Case 1.
	_, err = nsReaderMgr.get(shard, start, readerPosition{
		volume: 0, dataIdx: 0, metadataIdx: 0,
	})
	require.NoError(t, err)

	// Case 2.
	_, err = nsReaderMgr.get(shard, start, readerPosition{
		volume: 0, dataIdx: 2, metadataIdx: 3,
	})
	require.NoError(t, err)

	// Case 3.
	_, err = nsReaderMgr.get(shard, start, readerPosition{
		volume: 1, dataIdx: 1, metadataIdx: 1,
	})
	require.NoError(t, err)

	// Case 4.
	case4CachedReader := fs.NewMockDataFileSetReader(ctrl)
	pos := readerPosition{
		volume: 2, dataIdx: 4, metadataIdx: 5,
	}
	nsReaderMgrImpl.openReaders[cachedOpenReaderKey{
		shard:      shard,
		blockStart: start,
		position:   pos,
	}] = cachedReader{reader: case4CachedReader}
	case4Reader, err := nsReaderMgr.get(shard, start, pos)
	require.NoError(t, err)
	require.Equal(t, case4CachedReader, case4Reader)

	// Case 5.
	case5CachedReader := fs.NewMockDataFileSetReader(ctrl)
	case5CachedReader.EXPECT().Open(fs.DataReaderOpenOptions{
		Identifier: fs.FileSetFileIdentifier{
			Namespace: nsID, Shard: shard, BlockStart: start,
			VolumeIndex: 1,
		},
	}).Return(nil)
	case5CachedReader.EXPECT().ValidateMetadata().Return(nil)
	nsReaderMgrImpl.closedReaders = []cachedReader{
		{reader: case5CachedReader},
	}
	case5Reader, err := nsReaderMgr.get(shard, start, readerPosition{
		volume: 1, dataIdx: 0, metadataIdx: 0,
	})
	require.NoError(t, err)
	require.Equal(t, case5CachedReader, case5Reader)
	require.Len(t, nsReaderMgrImpl.closedReaders, 0)
}

func TestNamespaceReadersPutTickClose(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	metadata, err := namespace.NewMetadata(defaultTestNs1ID,
		defaultTestNs1Opts.SetColdWritesEnabled(true))
	require.NoError(t, err)
	shard := uint32(0)
	blockSize := metadata.Options().RetentionOptions().BlockSize()
	start := xtime.Now().Truncate(blockSize)

	mockFSReader := fs.NewMockDataFileSetReader(ctrl)
	mockBlockLeaseMgr := block.NewMockLeaseManager(ctrl)
	mockBlockLeaseMgr.EXPECT().RegisterLeaser(gomock.Any()).Return(nil)
	mockBlockLeaseMgr.EXPECT().UnregisterLeaser(gomock.Any()).Return(nil)
	// Case 2.
	mockBlockLeaseMgr.EXPECT().OpenLatestLease(gomock.Any(), gomock.Any()).
		Return(block.LeaseState{Volume: 1}, nil)
	// Case 3.
	mockBlockLeaseMgr.EXPECT().OpenLatestLease(gomock.Any(), gomock.Any()).
		Return(block.LeaseState{Volume: 2}, nil)
	// Case 4.
	mockBlockLeaseMgr.EXPECT().OpenLatestLease(gomock.Any(), gomock.Any()).
		Return(block.LeaseState{Volume: 3}, nil)

	nsReaderMgr := newNamespaceReaderManager(metadata, tally.NoopScope,
		DefaultTestOptions().SetBlockLeaseManager(mockBlockLeaseMgr))
	nsReaderMgrImpl := nsReaderMgr.(*namespaceReaderManager)
	// Grabbing specific ID here so that the test can match on gomock arguments.
	nsID := nsReaderMgrImpl.namespace.ID()

	// Case 1: putting a closed reader should add it to the slice of closed
	// readers.
	mockFSReader.EXPECT().Status().Return(fs.DataFileSetReaderStatus{
		Namespace: nsID, BlockStart: start, Shard: shard,
		Volume: 0,
		Open:   false,
	})
	require.Len(t, nsReaderMgrImpl.closedReaders, 0)
	require.NoError(t, nsReaderMgr.put(mockFSReader))
	require.Len(t, nsReaderMgrImpl.closedReaders, 1)

	// Case 2: a reader with a volume lower than the latest volume should be
	// closed and added in the slice of closed readers.
	mockFSReader.EXPECT().Status().Return(fs.DataFileSetReaderStatus{
		Namespace: nsID, BlockStart: start, Shard: shard,
		Volume: 0,
		Open:   true,
	})
	mockFSReader.EXPECT().Close().Return(nil)
	require.Len(t, nsReaderMgrImpl.closedReaders, 1)
	require.NoError(t, nsReaderMgr.put(mockFSReader))
	require.Len(t, nsReaderMgrImpl.closedReaders, 2)

	// Case 3: an open reader with the correct volume gets added to the open
	// readers.
	mockFSReader.EXPECT().Status().Return(fs.DataFileSetReaderStatus{
		Namespace: nsID, BlockStart: start, Shard: shard,
		Volume: 2,
		Open:   true,
	})
	mockFSReader.EXPECT().EntriesRead().Return(5)
	mockFSReader.EXPECT().MetadataRead().Return(6)
	require.Len(t, nsReaderMgrImpl.openReaders, 0)
	require.NoError(t, nsReaderMgr.put(mockFSReader))
	require.Len(t, nsReaderMgrImpl.openReaders, 1)

	// Case 4: if trying to put a reader that happens to already have an open
	// cached version for its exact key, close the reader and add to the slice
	// of closed readers.
	mockFSReader.EXPECT().Status().Return(fs.DataFileSetReaderStatus{
		Namespace: nsID, BlockStart: start, Shard: shard,
		Volume: 3,
		Open:   true,
	})
	mockFSReader.EXPECT().EntriesRead().Return(7)
	mockFSReader.EXPECT().MetadataRead().Return(8)
	mockFSReader.EXPECT().Close()
	mockExistingFSReader := fs.NewMockDataFileSetReader(ctrl)
	nsReaderMgrImpl.openReaders[cachedOpenReaderKey{
		shard:      shard,
		blockStart: start,
		position: readerPosition{
			volume:      3,
			dataIdx:     7,
			metadataIdx: 8,
		},
	}] = cachedReader{reader: mockExistingFSReader}
	require.Len(t, nsReaderMgrImpl.closedReaders, 2)
	require.NoError(t, nsReaderMgr.put(mockFSReader))
	require.Len(t, nsReaderMgrImpl.closedReaders, 3)

	// Closing the reader manager should close any open readers and remove all
	// readers.
	require.Len(t, nsReaderMgrImpl.openReaders, 2)
	require.Len(t, nsReaderMgrImpl.closedReaders, 3)
	mockFSReader.EXPECT().Close()
	mockExistingFSReader.EXPECT().Close()
	nsReaderMgr.close()
	require.Len(t, nsReaderMgrImpl.openReaders, 0)
	require.Len(t, nsReaderMgrImpl.closedReaders, 0)
}

func TestNamespaceReadersUpdateOpenLease(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	metadata, err := namespace.NewMetadata(defaultTestNs1ID,
		defaultTestNs1Opts.SetColdWritesEnabled(true))
	require.NoError(t, err)
	shard := uint32(0)
	blockSize := metadata.Options().RetentionOptions().BlockSize()
	start := xtime.Now().Truncate(blockSize)

	mockFSReader := fs.NewMockDataFileSetReader(ctrl)
	mockFSReader.EXPECT().Close().Return(nil).Times(2)
	mockBlockLeaseMgr := block.NewMockLeaseManager(ctrl)
	mockBlockLeaseMgr.EXPECT().RegisterLeaser(gomock.Any()).Return(nil)
	nsReaderMgr := newNamespaceReaderManager(metadata, tally.NoopScope,
		DefaultTestOptions().SetBlockLeaseManager(mockBlockLeaseMgr))
	nsReaderMgrImpl := nsReaderMgr.(*namespaceReaderManager)
	// Grabbing specific ID here so that the test can match on gomock arguments.
	nsID := nsReaderMgrImpl.namespace.ID()

	nsReaderMgrImpl.openReaders[cachedOpenReaderKey{
		shard:      shard,
		blockStart: start,
		position: readerPosition{
			volume:      0,
			dataIdx:     1,
			metadataIdx: 2,
		},
	}] = cachedReader{reader: mockFSReader}
	nsReaderMgrImpl.openReaders[cachedOpenReaderKey{
		shard:      shard,
		blockStart: start,
		position: readerPosition{
			volume:      1,
			dataIdx:     2,
			metadataIdx: 3,
		},
	}] = cachedReader{reader: mockFSReader}
	remainingKey := cachedOpenReaderKey{
		shard:      shard,
		blockStart: start,
		position: readerPosition{
			volume:      2,
			dataIdx:     4,
			metadataIdx: 5,
		},
	}
	nsReaderMgrImpl.openReaders[remainingKey] = cachedReader{reader: mockFSReader}

	require.Len(t, nsReaderMgrImpl.openReaders, 3)
	require.Len(t, nsReaderMgrImpl.closedReaders, 0)
	// Of the three existing open readers, only two of them have volumes lower
	// than the update lease volume, so those are expected to be closed, and
	// put in the slice of closed readers.
	res, err := nsReaderMgrImpl.UpdateOpenLease(block.LeaseDescriptor{
		Namespace: nsID, Shard: shard, BlockStart: start,
	}, block.LeaseState{Volume: 2})
	require.NoError(t, err)
	require.Equal(t, block.UpdateOpenLease, res)
	require.Len(t, nsReaderMgrImpl.openReaders, 1)
	require.Len(t, nsReaderMgrImpl.closedReaders, 2)
	_, exists := nsReaderMgrImpl.openReaders[remainingKey]
	require.True(t, exists)

	// Test attempting update lease on wrong namespace.
	res, err = nsReaderMgrImpl.UpdateOpenLease(block.LeaseDescriptor{
		Namespace: ident.StringID("wrong-ns"), Shard: shard, BlockStart: start,
	}, block.LeaseState{Volume: 2})
	require.NoError(t, err)
	require.Equal(t, block.NoOpenLease, res)
}

func TestNamespaceReadersFilesetExistsAt(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	metadata, err := namespace.NewMetadata(defaultTestNs1ID,
		defaultTestNs1Opts.SetColdWritesEnabled(true))
	require.NoError(t, err)

	mockBlockLeaseMgr := block.NewMockLeaseManager(ctrl)
	mockBlockLeaseMgr.EXPECT().RegisterLeaser(gomock.Any()).Return(nil)
	// First open lease is good.
	open1 := mockBlockLeaseMgr.EXPECT().OpenLatestLease(gomock.Any(), gomock.Any()).
		Return(block.LeaseState{}, nil)
	// Second open lease returns error.
	mockBlockLeaseMgr.EXPECT().OpenLatestLease(gomock.Any(), gomock.Any()).
		Return(block.LeaseState{}, errors.New("bad open")).After(open1)
	nsReaderMgr := newNamespaceReaderManager(metadata, tally.NoopScope,
		DefaultTestOptions().SetBlockLeaseManager(mockBlockLeaseMgr))

	exists, err := nsReaderMgr.filesetExistsAt(0, 0)
	require.NoError(t, err)
	require.False(t, exists)

	_, err = nsReaderMgr.filesetExistsAt(0, 0)
	require.Error(t, err)
}
