package fs

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/x/ident"
	"github.com/stretchr/testify/require"
)

func TestWriteReuseAfterError(t *testing.T) {
	dir := createTempDir(t)
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	seriesID := ident.StringID("series1")
	w := newTestWriter(t, filePathPrefix)
	writerOpts := DataWriterOpenOptions{
		Identifier: FileSetFileIdentifier{
			Namespace:   testNs1ID,
			Shard:       0,
			BlockStart:  time.Now().Truncate(time.Hour),
			VolumeIndex: 0,
		},
		BlockSize:   time.Hour,
		FileSetType: persist.FileSetFlushType,
	}
	data := checkedBytes([]byte{1, 2, 3})

	require.NoError(t, w.Open(writerOpts))
	require.NoError(t, w.Write(seriesID, ident.Tags{}, data, 0))
	require.NoError(t, w.Write(seriesID, ident.Tags{}, data, 0))
	require.Error(t, w.Close())

	require.NoError(t, w.Open(writerOpts))
	require.NoError(t, w.Close())
}
