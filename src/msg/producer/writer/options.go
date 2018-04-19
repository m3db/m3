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

package writer

import (
	"time"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3msg/protocol/proto"
	"github.com/m3db/m3msg/topic"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/retry"
)

const (
	defaultDialTimeout               = 10 * time.Second
	defaultMessageRetryBackoff       = 5 * time.Second
	defaultPlacementWatchInitTimeout = 5 * time.Second
	defaultTopicWatchInitTimeout     = 5 * time.Second
	defaultCloseCheckInterval        = 2 * time.Second
	defaultConnectionResetDelay      = 2 * time.Second
	defaultMessageRetryBatchSize     = 16 * 1024
	// Using 16K which provides much better performance comparing
	// to lower values like 1k ~ 8k.
	defaultConnectionBufferSize = 16384
)

// ConnectionOptions configs the connections.
type ConnectionOptions interface {
	// DialTimeout returns the dial timeout.
	DialTimeout() time.Duration

	// SetDialTimeout sets the dial timeout.
	SetDialTimeout(value time.Duration) ConnectionOptions

	// ResetDelay returns the delay before resetting connection.
	ResetDelay() time.Duration

	// SetResetDelay sets the delay before resetting connection.
	SetResetDelay(value time.Duration) ConnectionOptions

	// RetryOptions returns the options for connection retrier.
	RetryOptions() retry.Options

	// SetRetryOptions sets the options for connection retrier.
	SetRetryOptions(value retry.Options) ConnectionOptions

	// WriteBufferSize returns the buffer size for write.
	WriteBufferSize() int

	// SetWriteBufferSize sets the buffer size for write.
	SetWriteBufferSize(value int) ConnectionOptions

	// ReadBufferSize returns the buffer size for read.
	ReadBufferSize() int

	// SetReadBufferSize sets the buffer size for read.
	SetReadBufferSize(value int) ConnectionOptions

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) ConnectionOptions
}

type connectionOptions struct {
	dialTimeout     time.Duration
	resetDelay      time.Duration
	rOpts           retry.Options
	writeBufferSize int
	readBufferSize  int
	iOpts           instrument.Options
}

// NewConnectionOptions creates ConnectionOptions.
func NewConnectionOptions() ConnectionOptions {
	return &connectionOptions{
		dialTimeout:     defaultDialTimeout,
		resetDelay:      defaultConnectionResetDelay,
		rOpts:           retry.NewOptions(),
		writeBufferSize: defaultConnectionBufferSize,
		readBufferSize:  defaultConnectionBufferSize,
		iOpts:           instrument.NewOptions(),
	}
}

func (opts *connectionOptions) DialTimeout() time.Duration {
	return opts.dialTimeout
}

func (opts *connectionOptions) SetDialTimeout(value time.Duration) ConnectionOptions {
	o := *opts
	o.dialTimeout = value
	return &o
}

func (opts *connectionOptions) RetryOptions() retry.Options {
	return opts.rOpts
}

func (opts *connectionOptions) SetRetryOptions(value retry.Options) ConnectionOptions {
	o := *opts
	o.rOpts = value
	return &o
}

func (opts *connectionOptions) ResetDelay() time.Duration {
	return opts.resetDelay
}

func (opts *connectionOptions) SetResetDelay(value time.Duration) ConnectionOptions {
	o := *opts
	o.resetDelay = value
	return &o
}

func (opts *connectionOptions) WriteBufferSize() int {
	return opts.writeBufferSize
}

func (opts *connectionOptions) SetWriteBufferSize(value int) ConnectionOptions {
	o := *opts
	o.writeBufferSize = value
	return &o
}

func (opts *connectionOptions) ReadBufferSize() int {
	return opts.readBufferSize
}

func (opts *connectionOptions) SetReadBufferSize(value int) ConnectionOptions {
	o := *opts
	o.readBufferSize = value
	return &o
}

func (opts *connectionOptions) InstrumentOptions() instrument.Options {
	return opts.iOpts
}

func (opts *connectionOptions) SetInstrumentOptions(value instrument.Options) ConnectionOptions {
	o := *opts
	o.iOpts = value
	return &o
}

// Options configs the writer.
type Options interface {
	// TopicName returns the topic name.
	TopicName() string

	// SetTopicName sets the topic name.
	SetTopicName(value string) Options

	// TopicService returns the topic service.
	TopicService() topic.Service

	// SetTopicService sets the topic service.
	SetTopicService(value topic.Service) Options

	// TopicWatchInitTimeout returns the timeout for topic watch initialization.
	TopicWatchInitTimeout() time.Duration

	// SetTopicWatchInitTimeout sets the timeout for topic watch initialization.
	SetTopicWatchInitTimeout(value time.Duration) Options

	// ServiceDiscovery returns the client to service discovery service.
	ServiceDiscovery() services.Services

	// SetServiceDiscovery sets the client to service discovery services.
	SetServiceDiscovery(value services.Services) Options

	// PlacementWatchInitTimeout returns the timeout for placement watch initialization.
	PlacementWatchInitTimeout() time.Duration

	// SetPlacementWatchInitTimeout sets the timeout for placement watch initialization.
	SetPlacementWatchInitTimeout(value time.Duration) Options

	// MessagePoolOptions returns the options of pool for messages.
	MessagePoolOptions() pool.ObjectPoolOptions

	// SetMessagePoolOptions sets the options of pool for messages.
	SetMessagePoolOptions(value pool.ObjectPoolOptions) Options

	// MessageRetryBackoff returns the backoff before retrying messages.
	MessageRetryBackoff() time.Duration

	// SetMessageRetryBackoff sets the backoff before retrying messages.
	SetMessageRetryBackoff(value time.Duration) Options

	// MessageRetryBatchSize returns the batch size for retry.
	MessageRetryBatchSize() int

	// SetMessageRetryBatchSize sets the batch size for retry.
	SetMessageRetryBatchSize(value int) Options

	// CloseCheckInterval returns the close check interval.
	CloseCheckInterval() time.Duration

	// SetCloseCheckInterval sets the close check interval.
	SetCloseCheckInterval(value time.Duration) Options

	// AckErrorRetryOptions returns the retrier for ack errors.
	AckErrorRetryOptions() retry.Options

	// SetAckErrorRetryOptions sets the retrier for ack errors.
	SetAckErrorRetryOptions(value retry.Options) Options

	// EncodeDecoderOptions returns the options for EncodeDecoder.
	EncodeDecoderOptions() proto.EncodeDecoderOptions

	// SetEncodeDecoderOptions sets the options for EncodeDecoder.
	SetEncodeDecoderOptions(value proto.EncodeDecoderOptions) Options

	// ConnectionOptions returns the options for connections.
	ConnectionOptions() ConnectionOptions

	// SetConnectionOptions sets the options for connections.
	SetConnectionOptions(value ConnectionOptions) Options

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) Options
}

type writerOptions struct {
	topicName                 string
	topicService              topic.Service
	topicWatchInitTimeout     time.Duration
	services                  services.Services
	placementWatchInitTimeout time.Duration
	messageRetryBackoff       time.Duration
	messagePoolOptions        pool.ObjectPoolOptions
	messageRetryBatchSize     int
	closeCheckInterval        time.Duration
	ackErrRetryOpts           retry.Options
	encdecOpts                proto.EncodeDecoderOptions
	cOpts                     ConnectionOptions
	iOpts                     instrument.Options
}

// NewOptions creates Options.
func NewOptions() Options {
	return &writerOptions{
		topicWatchInitTimeout:     defaultTopicWatchInitTimeout,
		placementWatchInitTimeout: defaultPlacementWatchInitTimeout,
		messageRetryBackoff:       defaultMessageRetryBackoff,
		messagePoolOptions:        pool.NewObjectPoolOptions(),
		messageRetryBatchSize:     defaultMessageRetryBatchSize,
		closeCheckInterval:        defaultCloseCheckInterval,
		ackErrRetryOpts:           retry.NewOptions(),
		encdecOpts:                proto.NewEncodeDecoderOptions(),
		cOpts:                     NewConnectionOptions(),
		iOpts:                     instrument.NewOptions(),
	}
}

func (opts *writerOptions) TopicName() string {
	return opts.topicName
}

func (opts *writerOptions) SetTopicName(value string) Options {
	o := *opts
	o.topicName = value
	return &o
}

func (opts *writerOptions) TopicService() topic.Service {
	return opts.topicService
}

func (opts *writerOptions) SetTopicService(value topic.Service) Options {
	o := *opts
	o.topicService = value
	return &o
}

func (opts *writerOptions) TopicWatchInitTimeout() time.Duration {
	return opts.topicWatchInitTimeout
}

func (opts *writerOptions) SetTopicWatchInitTimeout(value time.Duration) Options {
	o := *opts
	o.topicWatchInitTimeout = value
	return &o
}

func (opts *writerOptions) ServiceDiscovery() services.Services {
	return opts.services
}

func (opts *writerOptions) SetServiceDiscovery(value services.Services) Options {
	o := *opts
	o.services = value
	return &o
}

func (opts *writerOptions) PlacementWatchInitTimeout() time.Duration {
	return opts.placementWatchInitTimeout
}

func (opts *writerOptions) SetPlacementWatchInitTimeout(value time.Duration) Options {
	o := *opts
	o.placementWatchInitTimeout = value
	return &o
}

func (opts *writerOptions) MessagePoolOptions() pool.ObjectPoolOptions {
	return opts.messagePoolOptions
}

func (opts *writerOptions) SetMessagePoolOptions(value pool.ObjectPoolOptions) Options {
	o := *opts
	o.messagePoolOptions = value
	return &o
}

func (opts *writerOptions) MessageRetryBackoff() time.Duration {
	return opts.messageRetryBackoff
}

func (opts *writerOptions) SetMessageRetryBackoff(value time.Duration) Options {
	o := *opts
	o.messageRetryBackoff = value
	return &o
}

func (opts *writerOptions) MessageRetryBatchSize() int {
	return opts.messageRetryBatchSize
}

func (opts *writerOptions) SetMessageRetryBatchSize(value int) Options {
	o := *opts
	o.messageRetryBatchSize = value
	return &o
}

func (opts *writerOptions) CloseCheckInterval() time.Duration {
	return opts.closeCheckInterval
}

func (opts *writerOptions) SetCloseCheckInterval(value time.Duration) Options {
	o := *opts
	o.closeCheckInterval = value
	return &o
}

func (opts *writerOptions) AckErrorRetryOptions() retry.Options {
	return opts.ackErrRetryOpts
}

func (opts *writerOptions) SetAckErrorRetryOptions(value retry.Options) Options {
	o := *opts
	o.ackErrRetryOpts = value
	return &o
}

func (opts *writerOptions) EncodeDecoderOptions() proto.EncodeDecoderOptions {
	return opts.encdecOpts
}

func (opts *writerOptions) SetEncodeDecoderOptions(value proto.EncodeDecoderOptions) Options {
	o := *opts
	o.encdecOpts = value
	return &o
}

func (opts *writerOptions) ConnectionOptions() ConnectionOptions {
	return opts.cOpts
}

func (opts *writerOptions) SetConnectionOptions(value ConnectionOptions) Options {
	o := *opts
	o.cOpts = value
	return &o
}

func (opts *writerOptions) InstrumentOptions() instrument.Options {
	return opts.iOpts
}

func (opts *writerOptions) SetInstrumentOptions(value instrument.Options) Options {
	o := *opts
	o.iOpts = value
	return &o
}
