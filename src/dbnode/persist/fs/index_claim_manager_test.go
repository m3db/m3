package fs

import (
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/x/ident"
	"github.com/stretchr/testify/require"
)

func TestIndexClaimManagerConcurrentClaims(t *testing.T) {
	mgr, ok := NewIndexClaimManager(NewOptions()).(*indexClaimManager)
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
