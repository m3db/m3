// Copyright (c) 2018 Uber Technologies, Inc.
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

package clone

import (
	"os"
	"time"

	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"
)

// FileSetID is the collection of identifiers required to
// uniquely identify a fileset
type FileSetID struct {
	PathPrefix string
	Namespace  string
	Shard      uint32
	Blockstart xtime.UnixNano
}

// FileSetCloner clones a given fileset
type FileSetCloner interface {
	// Clone clones the given fileset
	Clone(src FileSetID, dest FileSetID, destBlocksize time.Duration) error
}

// Options represents the knobs available while cloning
type Options interface {
	// SetBytesPool sets the bytesPool
	SetBytesPool(bytesPool pool.CheckedBytesPool) Options

	// BytesPool returns the bytesPool
	BytesPool() pool.CheckedBytesPool

	// SetDecodingOptions sets the decoding options
	SetDecodingOptions(decodingOpts msgpack.DecodingOptions) Options

	// DecodingOptions returns the decoding options
	DecodingOptions() msgpack.DecodingOptions

	// SetBufferSize sets the buffer size
	SetBufferSize(int) Options

	// BufferSize returns the buffer size
	BufferSize() int

	// SetFileMode sets the fileMode used for file creation
	SetFileMode(os.FileMode) Options

	// FileMode returns the fileMode used for file creation
	FileMode() os.FileMode

	// SetDirMode sets the file mode used for dir creation
	SetDirMode(os.FileMode) Options

	// DirMode returns the file mode used for dir creation
	DirMode() os.FileMode
}
