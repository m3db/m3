package clone

import (
	"os"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/x/pool"
)

func TestOptions_DefaultsAndSetters(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockBytesPool := pool.NewMockCheckedBytesPool(ctrl)

	opts := NewOptions()
	require.Nil(t, opts.BytesPool())
	require.Equal(t, msgpack.NewDecodingOptions(), opts.DecodingOptions())
	require.Equal(t, 65536, opts.BufferSize())
	require.Equal(t, os.FileMode(0666), opts.FileMode())
	require.Equal(t, os.ModeDir|os.FileMode(0755), opts.DirMode())

	customOpts := msgpack.NewDecodingOptions()
	bufferSize := 12345
	fileMode := os.FileMode(0644)
	dirMode := os.FileMode(0700)

	opts = opts.
		SetBytesPool(mockBytesPool).
		SetDecodingOptions(customOpts).
		SetBufferSize(bufferSize).
		SetFileMode(fileMode).
		SetDirMode(dirMode)

	require.Equal(t, mockBytesPool, opts.BytesPool())
	require.Equal(t, customOpts, opts.DecodingOptions())
	require.Equal(t, bufferSize, opts.BufferSize())
	require.Equal(t, fileMode, opts.FileMode())
	require.Equal(t, dirMode, opts.DirMode())
}
