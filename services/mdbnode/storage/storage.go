package storage

import (
	"time"

	"code.uber.internal/infra/memtsdb/encoding"
	"code.uber.internal/infra/memtsdb/encoding/tsz"
)

const (
	// DefaultBlockSize is the default block size
	DefaultBlockSize = 2 * time.Hour

	// DefaultBufferResolution is the default buffer resolution
	DefaultBufferResolution = 1 * time.Second

	// DefaultBufferFuture is the default buffer future limit
	DefaultBufferFuture = 10 * time.Minute

	// DefaultBufferPast is the default buffer past limit
	DefaultBufferPast = 10 * time.Minute

	// DefaultBufferFlush is the default buffer flush
	DefaultBufferFlush = 1 * time.Minute
)

// Database is a time series database
type Database interface {

	// GetOptions returns the database options
	GetOptions() DatabaseOptions

	// Open will open the database for writing and reading
	Open() error

	// Close will close the database for writing and reading
	Close() error

	// Write value to the database for an ID
	Write(
		id string,
		timestamp time.Time,
		value float64,
		unit time.Duration,
		annotation []byte,
	) error

	// Fetch retrieves encoded segments for an ID
	FetchEncodedSegments(id string, start, end time.Time) ([][]byte, error)
}

// NowFn is the function supplied to determine "now"
type NowFn func() time.Time

// DatabaseOptions is a set of database options
type DatabaseOptions interface {
	// BlockSize sets the blockSize and returns a new DatabaseOptions
	BlockSize(value time.Duration) DatabaseOptions

	// GetBlockSize returns the blockSize
	GetBlockSize() time.Duration

	// BufferResolution sets the bufferResolution and returns a new DatabaseOptions
	BufferResolution(value time.Duration) DatabaseOptions

	// GetBufferResolution returns the bufferResolution
	GetBufferResolution() time.Duration

	// NewEncoderFn sets the newEncoderFn and returns a new DatabaseOptions
	NewEncoderFn(value encoding.NewEncoderFn) DatabaseOptions

	// GetNewEncoderFn returns the newEncoderFn
	GetNewEncoderFn() encoding.NewEncoderFn

	// NewDecoderFn sets the newDecoderFn and returns a new DatabaseOptions
	NewDecoderFn(value encoding.NewDecoderFn) DatabaseOptions

	// GetNewDecoderFn returns the newDecoderFn
	GetNewDecoderFn() encoding.NewDecoderFn

	// NowFn sets the nowFn and returns a new DatabaseOptions
	NowFn(value NowFn) DatabaseOptions

	// GetNowFn returns the nowFn
	GetNowFn() NowFn

	// BufferFuture sets the bufferFuture and returns a new DatabaseOptions
	BufferFuture(value time.Duration) DatabaseOptions

	// GetBufferFuture returns the bufferFuture
	GetBufferFuture() time.Duration

	// BufferPast sets the bufferPast and returns a new DatabaseOptions
	BufferPast(value time.Duration) DatabaseOptions

	// GetBufferPast returns the bufferPast
	GetBufferPast() time.Duration

	// BufferFlush sets the bufferFlush and returns a new DatabaseOptions
	BufferFlush(value time.Duration) DatabaseOptions

	// GetBufferFlush returns the bufferFlush
	GetBufferFlush() time.Duration
}

type dbOptions struct {
	blockSize        time.Duration
	bufferResolution time.Duration
	newEncoderFn     encoding.NewEncoderFn
	newDecoderFn     encoding.NewDecoderFn
	nowFn            NowFn
	bufferFuture     time.Duration
	bufferPast       time.Duration
	bufferFlush      time.Duration
}

// NewDatabaseOptions creates a new set of database options with defaults
func NewDatabaseOptions() DatabaseOptions {
	return &dbOptions{
		blockSize:        DefaultBlockSize,
		bufferResolution: DefaultBufferResolution,
		newEncoderFn: func(start time.Time) encoding.Encoder {
			// TODO(r): encoder will not require unit
			return tsz.NewEncoder(start, time.Second)
		},
		newDecoderFn: func() encoding.Decoder {
			// TODO(r): decoder will not require unit
			return tsz.NewDecoder(time.Second)
		},
		// NB(r): potentially temporarily memoize calls to time.Now
		nowFn:        time.Now,
		bufferFuture: DefaultBufferFuture,
		bufferPast:   DefaultBufferPast,
		bufferFlush:  DefaultBufferFlush,
	}
}

func (o *dbOptions) BlockSize(value time.Duration) DatabaseOptions {
	opts := *o
	opts.blockSize = value
	return &opts
}

func (o *dbOptions) GetBlockSize() time.Duration {
	return o.blockSize
}

func (o *dbOptions) BufferResolution(value time.Duration) DatabaseOptions {
	opts := *o
	opts.bufferResolution = value
	return &opts
}

func (o *dbOptions) GetBufferResolution() time.Duration {
	return o.bufferResolution
}

func (o *dbOptions) NewEncoderFn(value encoding.NewEncoderFn) DatabaseOptions {
	opts := *o
	opts.newEncoderFn = value
	return &opts
}

func (o *dbOptions) GetNewEncoderFn() encoding.NewEncoderFn {
	return o.newEncoderFn
}

func (o *dbOptions) NewDecoderFn(value encoding.NewDecoderFn) DatabaseOptions {
	opts := *o
	opts.newDecoderFn = value
	return &opts
}

func (o *dbOptions) GetNewDecoderFn() encoding.NewDecoderFn {
	return o.newDecoderFn
}

func (o *dbOptions) NowFn(value NowFn) DatabaseOptions {
	opts := *o
	opts.nowFn = value
	return &opts
}

func (o *dbOptions) GetNowFn() NowFn {
	return o.nowFn
}

func (o *dbOptions) BufferFuture(value time.Duration) DatabaseOptions {
	opts := *o
	opts.bufferFuture = value
	return &opts
}

func (o *dbOptions) GetBufferFuture() time.Duration {
	return o.bufferFuture
}

func (o *dbOptions) BufferPast(value time.Duration) DatabaseOptions {
	opts := *o
	opts.bufferPast = value
	return &opts
}

func (o *dbOptions) GetBufferPast() time.Duration {
	return o.bufferPast
}

func (o *dbOptions) BufferFlush(value time.Duration) DatabaseOptions {
	opts := *o
	opts.bufferFlush = value
	return &opts
}

func (o *dbOptions) GetBufferFlush() time.Duration {
	return o.bufferFlush
}
