package clone

import (
	"os"

	"github.com/m3db/m3db/persist/msgpack"
	"github.com/m3db/m3x/pool"
)

const (
	defaultBufferSize = 65536
	defaultFileMode   = os.FileMode(0666)
	defaultDirMode    = os.ModeDir | os.FileMode(0755)
)

type opts struct {
	pool       pool.CheckedBytesPool
	dOpts      msgpack.DecodingOptions
	bufferSize int
	fileMode   os.FileMode
	dirMode    os.FileMode
}

// NewOptions returns the new options
func NewOptions() Options {
	return &opts{
		pool:       nil,
		dOpts:      msgpack.NewDecodingOptions(),
		bufferSize: defaultBufferSize,
		fileMode:   defaultFileMode,
		dirMode:    defaultDirMode,
	}
}

func (o *opts) SetBytesPool(bytesPool pool.CheckedBytesPool) Options {
	o.pool = bytesPool
	return o
}

func (o *opts) BytesPool() pool.CheckedBytesPool {
	return o.pool
}

func (o *opts) SetDecodingOptions(decodingOpts msgpack.DecodingOptions) Options {
	o.dOpts = decodingOpts
	return o
}

func (o *opts) DecodingOptions() msgpack.DecodingOptions {
	return o.dOpts
}

func (o *opts) SetBufferSize(b int) Options {
	o.bufferSize = b
	return o
}

func (o *opts) BufferSize() int {
	return o.bufferSize
}

func (o *opts) SetFileMode(f os.FileMode) Options {
	o.fileMode = f
	return o
}

func (o *opts) FileMode() os.FileMode {
	return o.fileMode
}

func (o *opts) SetDirMode(d os.FileMode) Options {
	o.dirMode = d
	return o
}

func (o *opts) DirMode() os.FileMode {
	return o.dirMode
}
