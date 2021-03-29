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

package fs

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

func newTestIndexClaimsManager(t *testing.T, opts Options) IndexClaimsManager {
	// Reset the count of index claim managers.
	ResetIndexClaimsManagersUnsafe()
	mgr, err := NewIndexClaimsManager(opts)
	require.NoError(t, err)
	return mgr
}

func TestIndexClaimsManagerSingleGlobalManager(t *testing.T) {
	// Reset the count of index claim managers.
	ResetIndexClaimsManagersUnsafe()

	// First should be able to be created easily.
	_, err := NewIndexClaimsManager(testDefaultOpts)
	require.NoError(t, err)

	// Second should cause an error.
	_, err = NewIndexClaimsManager(testDefaultOpts)
	require.Error(t, err)
	require.Equal(t, errMustUseSingleClaimsManager, err)
}

func TestIndexClaimsManagerConcurrentClaims(t *testing.T) {
	mgr, ok := newTestIndexClaimsManager(t, testDefaultOpts).(*indexClaimsManager)
	require.True(t, ok)

	// Always return 0 for starting volume index for testing purposes.
	mgr.nextIndexFileSetVolumeIndexFn = func(
		filePathPrefix string,
		namespace ident.ID,
		blockStart time.Time,
	) (int, error) {
		return 0, nil
	}

	md, err := namespace.NewMetadata(ident.StringID("foo"), namespace.NewOptions())
	require.NoError(t, err)

	var (
		m          sync.Map
		wg         sync.WaitGroup
		blockSize  = md.Options().IndexOptions().BlockSize()
		blockStart = time.Now().Truncate(blockSize)
	)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				volumeIndex, err := mgr.ClaimNextIndexFileSetVolumeIndex(
					md,
					blockStart,
				)
				require.NoError(t, err)
				_, loaded := m.LoadOrStore(volumeIndex, true)
				// volume index should not have been previously stored or
				// there are conflicting volume indices.
				require.False(t, loaded)
			}
		}()
	}
	wg.Wait()
}

// TestIndexClaimsManagerOutOfRetention ensure that we both reject and delete out of
// retention index claims.
func TestIndexClaimsManagerOutOfRetention(t *testing.T) {
	mgr, ok := newTestIndexClaimsManager(t, testDefaultOpts).(*indexClaimsManager)
	require.True(t, ok)

	// Always return 0 for starting volume index for testing purposes.
	mgr.nextIndexFileSetVolumeIndexFn = func(
		filePathPrefix string,
		namespace ident.ID,
		blockStart time.Time,
	) (int, error) {
		return 0, nil
	}

	md, err := namespace.NewMetadata(ident.StringID("foo"), namespace.NewOptions())
	blockSize := md.Options().IndexOptions().BlockSize()
	blockStart := time.Now().Truncate(blockSize)
	require.NoError(t, err)

	_, err = mgr.ClaimNextIndexFileSetVolumeIndex(
		md,
		blockStart,
	)
	require.NoError(t, err)

	now := mgr.nowFn().Add(md.Options().RetentionOptions().RetentionPeriod()).
		Add(blockSize)
	mgr.nowFn = func() time.Time { return now }
	_, err = mgr.ClaimNextIndexFileSetVolumeIndex(
		md,
		blockStart,
	)
	require.Equal(t, ErrOutOfRetentionClaim, err)

	// Verify that the out of retention entry has been deleted as well.
	_, ok = mgr.volumeIndexClaims[md.ID().String()][xtime.ToUnixNano(blockStart)]
	require.False(t, ok)
}
