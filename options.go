package memtsdb

import (
	"time"

	"code.uber.internal/infra/memtsdb/encoding"
)

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

	// RetentionPeriod sets how long we intend to keep data in memory.
	RetentionPeriod(value time.Duration) DatabaseOptions

	// RetentionPeriod is how long we intend to keep data in memory.
	GetRetentionPeriod() time.Duration

	// NewBootstrapFn sets the newBootstrapFn and returns a new DatabaseOptions
	NewBootstrapFn(value NewBootstrapFn) DatabaseOptions

	// GetBootstrapFn returns the newBootstrapFn
	GetBootstrapFn() NewBootstrapFn
}
