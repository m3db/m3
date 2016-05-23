package storage

import (
	"sync"
	"time"

	"code.uber.internal/infra/memtsdb/encoding"
)

type databaseBlock interface {
	startTime() time.Time
	write(timestamp time.Time, value float64, unit time.Duration, annotation []byte)
	bytes() []byte
}

type dbBlock struct {
	sync.RWMutex
	opts    DatabaseOptions
	start   time.Time
	encoder encoding.Encoder
}

func newDatabaseBlock(start time.Time, opts DatabaseOptions) databaseBlock {
	newEncoderFn := opts.GetNewEncoderFn()
	return &dbBlock{
		opts:    opts,
		start:   start,
		encoder: newEncoderFn(start),
	}
}

func (b *dbBlock) startTime() time.Time {
	return b.start
}

func (b *dbBlock) write(timestamp time.Time, value float64, unit time.Duration, annotation []byte) {
	// TODO(r): encoder will take unit
	b.encoder.Encode(encoding.Datapoint{Timestamp: timestamp, Value: value}, annotation)
}

func (b *dbBlock) bytes() []byte {
	return b.encoder.Bytes()
}
