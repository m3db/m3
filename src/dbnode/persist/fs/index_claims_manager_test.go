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

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/x/ident"

	"github.com/stretchr/testify/require"
)

func TestIndexClaimsManagerConcurrentClaims(t *testing.T) {
	mgr, ok := NewIndexClaimsManager(NewOptions()).(*indexClaimsManager)
	require.True(t, ok)

	// Always return 0 for starting volume index for testing purposes.
	mgr.nextIndexFileSetVolumeIndexFn = func(
		filePathPrefix string,
		namespace ident.ID,
		blockStart time.Time,
	) (int, error) {
		return 0, nil
	}

	blockSize := time.Hour
	blockStart := time.Now().Truncate(blockSize)
	md, err := namespace.NewMetadata(ident.StringID("foo"), namespace.NewOptions())
	require.NoError(t, err)
	var (
		m  sync.Map
		wg sync.WaitGroup
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
