package clone

import (
	"os"
	"time"

	"github.com/m3db/m3db/persist/encoding/msgpack"
	"github.com/m3db/m3x/pool"
)

// FilesetID is the collection of identifiers required to
// uniquely identify a fileset
type FilesetID struct {
	PathPrefix string
	Namespace  string
	Shard      uint32
	Blockstart time.Time
}

// FilesetCloner clones a given fileset
type FilesetCloner interface {
	// Clone clones the given fileset
	Clone(src FilesetID, dest FilesetID, destBlocksize time.Duration) error
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
