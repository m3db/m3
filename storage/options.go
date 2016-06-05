package storage

import (
	"time"

	"code.uber.internal/infra/memtsdb"
	"code.uber.internal/infra/memtsdb/encoding"
	"code.uber.internal/infra/memtsdb/encoding/tsz"
)

const (
	// defaultBlockSize is the default block size
	defaultBlockSize = 2 * time.Hour

	// defaultBufferResolution is the default buffer resolution
	defaultBufferResolution = 1 * time.Second

	// defaultBufferFuture is the default buffer future limit
	defaultBufferFuture = 10 * time.Minute

	// defaultBufferPast is the default buffer past limit
	defaultBufferPast = 10 * time.Minute

	// defaultBufferFlush is the default buffer flush
	defaultBufferFlush = 1 * time.Minute

	// defaultRetentionPeriod is how long we keep data in memory by default.
	defaultRetentionPeriod = 2 * 24 * time.Hour
)

type dbOptions struct {
	blockSize        time.Duration
	bufferResolution time.Duration
	newEncoderFn     encoding.NewEncoderFn
	newDecoderFn     encoding.NewDecoderFn
	nowFn            memtsdb.NowFn
	bufferFuture     time.Duration
	bufferPast       time.Duration
	bufferFlush      time.Duration
	retentionPeriod  time.Duration
	newBootstrapFn   memtsdb.NewBootstrapFn
}

// NewDatabaseOptions creates a new set of database options with defaults
func NewDatabaseOptions() memtsdb.DatabaseOptions {
	opts := tsz.NewOptions()
	return &dbOptions{
		blockSize:        defaultBlockSize,
		bufferResolution: defaultBufferResolution,
		newEncoderFn: func(start time.Time, bytes []byte) encoding.Encoder {
			return tsz.NewEncoder(start, bytes, opts)
		},
		newDecoderFn: func() encoding.Decoder {
			return tsz.NewDecoder(opts)
		},
		// NB(r): potentially temporarily memoize calls to time.Now
		nowFn:           time.Now,
		bufferFuture:    defaultBufferFuture,
		bufferPast:      defaultBufferPast,
		bufferFlush:     defaultBufferFlush,
		retentionPeriod: defaultRetentionPeriod,
	}
}

func (o *dbOptions) BlockSize(value time.Duration) memtsdb.DatabaseOptions {
	opts := *o
	opts.blockSize = value
	return &opts
}

func (o *dbOptions) GetBlockSize() time.Duration {
	return o.blockSize
}

func (o *dbOptions) BufferResolution(value time.Duration) memtsdb.DatabaseOptions {
	opts := *o
	opts.bufferResolution = value
	return &opts
}

func (o *dbOptions) GetBufferResolution() time.Duration {
	return o.bufferResolution
}

func (o *dbOptions) NewEncoderFn(value encoding.NewEncoderFn) memtsdb.DatabaseOptions {
	opts := *o
	opts.newEncoderFn = value
	return &opts
}

func (o *dbOptions) GetNewEncoderFn() encoding.NewEncoderFn {
	return o.newEncoderFn
}

func (o *dbOptions) NewDecoderFn(value encoding.NewDecoderFn) memtsdb.DatabaseOptions {
	opts := *o
	opts.newDecoderFn = value
	return &opts
}

func (o *dbOptions) GetNewDecoderFn() encoding.NewDecoderFn {
	return o.newDecoderFn
}

func (o *dbOptions) NowFn(value memtsdb.NowFn) memtsdb.DatabaseOptions {
	opts := *o
	opts.nowFn = value
	return &opts
}

func (o *dbOptions) GetNowFn() memtsdb.NowFn {
	return o.nowFn
}

func (o *dbOptions) BufferFuture(value time.Duration) memtsdb.DatabaseOptions {
	opts := *o
	opts.bufferFuture = value
	return &opts
}

func (o *dbOptions) GetBufferFuture() time.Duration {
	return o.bufferFuture
}

func (o *dbOptions) BufferPast(value time.Duration) memtsdb.DatabaseOptions {
	opts := *o
	opts.bufferPast = value
	return &opts
}

func (o *dbOptions) GetBufferPast() time.Duration {
	return o.bufferPast
}

func (o *dbOptions) BufferFlush(value time.Duration) memtsdb.DatabaseOptions {
	opts := *o
	opts.bufferFlush = value
	return &opts
}

func (o *dbOptions) GetBufferFlush() time.Duration {
	return o.bufferFlush
}

// RetentionPeriod sets how long we intend to keep data in memory.
func (o *dbOptions) RetentionPeriod(value time.Duration) memtsdb.DatabaseOptions {
	opts := *o
	opts.retentionPeriod = value
	return &opts
}

// GetRetentionPeriod returns how long we intend to keep raw metrics in memory.
func (o *dbOptions) GetRetentionPeriod() time.Duration {
	return o.retentionPeriod
}

func (o *dbOptions) NewBootstrapFn(value memtsdb.NewBootstrapFn) memtsdb.DatabaseOptions {
	opts := *o
	opts.newBootstrapFn = value
	return &opts
}

func (o *dbOptions) GetBootstrapFn() memtsdb.NewBootstrapFn {
	return o.newBootstrapFn
}
