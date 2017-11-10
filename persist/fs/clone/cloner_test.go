package clone

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/checked"

	"github.com/stretchr/testify/require"
)

const (
	numTestPoints = 100
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
	src := FilesetID{
		PathPrefix: srcData,
		Namespace:  "testns-src",
		Shard:      123,
		Blockstart: time.Now().Truncate(srcBlockSize),
	}
	testBytes.IncRef()
	defer testBytes.DecRef()
	writeTestData(t, srcBlockSize, src, opts)

	// clone it
	destBlockSize := 2 * time.Hour
	clonedData := path.Join(dir, "clone")
	require.NoError(t, os.Mkdir(clonedData, opts.DirMode()))
	dest := FilesetID{
		PathPrefix: clonedData,
		Namespace:  "testns-dest",
		Shard:      321,
		Blockstart: time.Now().Add(-1 * 24 * 30 * time.Hour).Truncate(destBlockSize),
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
	require.NoError(t, r1.Open(ts.StringID(src.Namespace), src.Shard, src.Blockstart))
	r2, err := fs.NewReader(opts.BytesPool(), fs.NewOptions().
		SetFilePathPrefix(dest.PathPrefix).
		SetDataReaderBufferSize(opts.BufferSize()).
		SetInfoReaderBufferSize(opts.BufferSize()).
		SetDecodingOptions(opts.DecodingOptions()))
	require.NoError(t, err)
	require.NoError(t, r2.Open(ts.StringID(dest.Namespace), dest.Shard, dest.Blockstart))
	for {
		t1, b1, c1, e1 := r1.Read()
		t2, b2, c2, e2 := r2.Read()
		if e1 == e2 && e1 == io.EOF {
			break
		}
		b1.IncRef()
		b2.IncRef()
		require.Equal(t, t1.String(), t2.String())
		require.Equal(t, b1.Get(), b2.Get())
		require.Equal(t, c1, c2)
		b1.DecRef()
		b2.DecRef()
	}
	require.NoError(t, r1.Close())
	require.NoError(t, r2.Close())
}

func writeTestData(t *testing.T, bs time.Duration, src FilesetID, opts Options) {
	w, err := fs.NewWriter(fs.NewOptions().
		SetFilePathPrefix(src.PathPrefix).
		SetWriterBufferSize(opts.BufferSize()).
		SetNewFileMode(opts.FileMode()).
		SetNewDirectoryMode(opts.DirMode()))
	require.NoError(t, err)
	require.NoError(t, w.Open(ts.StringID(src.Namespace), bs, src.Shard, src.Blockstart))
	for i := 0; i < numTestSeries; i++ {
		id := ts.StringID(fmt.Sprintf("testSeries.%d", i))
		for j := 0; j < numTestPoints; j++ {
			require.NoError(t, w.Write(id, testBytes, 1234))
		}
	}
	require.NoError(t, w.Close())
}
