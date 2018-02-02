package generate

import (
	"os"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"
)

// BlockConfig represents the configuration to generate a SeriesBlock
type BlockConfig struct {
	IDs       []string
	NumPoints int
	Start     time.Time
}

// Series represents a generated series of data
type Series struct {
	ID   ident.ID
	Data []ts.Datapoint
}

// SeriesDataPoint represents a single data point of a generated series of data
type SeriesDataPoint struct {
	ts.Datapoint
	ID ident.ID
}

// SeriesDataPointsByTime are a sorted list of SeriesDataPoints
type SeriesDataPointsByTime []SeriesDataPoint

// SeriesBlock is a collection of Series'
type SeriesBlock []Series

// SeriesBlocksByStart is a map of time -> SeriesBlock
type SeriesBlocksByStart map[xtime.UnixNano]SeriesBlock

// Writer writes generated data to disk
type Writer interface {
	// Write writes the data
	Write(ns ident.ID, shards sharding.ShardSet, data SeriesBlocksByStart) error
}

// Options represent the parameters needed for the Writer
type Options interface {
	// SetClockOptions sets the clock options
	SetClockOptions(value clock.Options) Options

	// ClockOptions returns the clock options
	ClockOptions() clock.Options

	// SetRetentionPeriod sets how long we intend to keep data in memory
	SetRetentionPeriod(value time.Duration) Options

	// RetentionPeriod returns how long we intend to keep data in memory
	RetentionPeriod() time.Duration

	// SetBlockSize sets the blockSize
	SetBlockSize(value time.Duration) Options

	// BlockSize returns the blockSize
	BlockSize() time.Duration

	// SetFilePathPrefix sets the file path prefix for sharded TSDB files
	SetFilePathPrefix(value string) Options

	// FilePathPrefix returns the file path prefix for sharded TSDB files
	FilePathPrefix() string

	// SetNewFileMode sets the new file mode
	SetNewFileMode(value os.FileMode) Options

	// NewFileMode returns the new file mode
	NewFileMode() os.FileMode

	// SetNewDirectoryMode sets the new directory mode
	SetNewDirectoryMode(value os.FileMode) Options

	// NewDirectoryMode returns the new directory mode
	NewDirectoryMode() os.FileMode

	// SetWriterBufferSize sets the buffer size for writing TSDB files
	SetWriterBufferSize(value int) Options

	// WriterBufferSize returns the buffer size for writing TSDB files
	WriterBufferSize() int

	// SetWriteEmptyShards sets whether writes are done even for empty start periods
	SetWriteEmptyShards(bool) Options

	// WriteEmptyShards returns whether writes are done even for empty start periods
	WriteEmptyShards() bool

	// SetEncoderPool sets the contextPool
	SetEncoderPool(value encoding.EncoderPool) Options

	// EncoderPool returns the contextPool
	EncoderPool() encoding.EncoderPool
}
