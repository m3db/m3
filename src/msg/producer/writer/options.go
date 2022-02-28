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
	"context"
	"net"
	"time"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/msg/protocol/proto"
	"github.com/m3db/m3/src/msg/topic"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/retry"
)

const (
	defaultPlacementWatchInitTimeout         = 2 * time.Second
	defaultTopicWatchInitTimeout             = 2 * time.Second
	defaultCloseCheckInterval                = time.Second
	defaultMessageQueueNewWritesScanInterval = 200 * time.Millisecond
	defaultMessageQueueFullScanInterval      = 5 * time.Second
	defaultMessageQueueScanBatchSize         = 16
	defaultInitialAckMapSize                 = 1024

	defaultNumConnections            = 4
	defaultConnectionDialTimeout     = 5 * time.Second
	defaultConnectionWriteTimeout    = 5 * time.Second
	defaultConnectionKeepAlivePeriod = 5 * time.Second
	defaultConnectionResetDelay      = 2 * time.Second
	defaultConnectionFlushInterval   = time.Second
	// Using 65k which provides much better performance comparing
	// to lower values like 1k ~ 8k.
	defaultConnectionBufferSize = 2 << 15 // ~65kb

	defaultWriterRetryInitialBackoff = time.Second * 5
)

// ContextDialerFn allows customization of how a m3msg Writer connects to producer endpoints.
// See ConnectionOptions#ContextDialer
type ContextDialerFn func(ctx context.Context, network string, address string) (net.Conn, error)

// ConnectionOptions configs the connections.
type ConnectionOptions interface {
	// NumConnections returns the number of connections.
	NumConnections() int

	// SetNumConnections sets the number of connections.
	SetNumConnections(value int) ConnectionOptions

	// ContextDialer allows customizing the way a m3msg Writer connects to producer endpoints. By default, this is:
	// (&net.ContextDialer{}).DialContext. This can be used to do a variety of things, such as forwarding a connection
	// over a proxy.
	// NOTE: if your ContextDialerFn returns anything other a *net.TCPConn, TCP options such as KeepAlivePeriod
	// will *not* be applied automatically. It is your responsibility to make sure these get applied as needed in
	// your custom ContextDialerFn.
	ContextDialer() ContextDialerFn

	// SetContextDialer see ContextDialer.
	SetContextDialer(fn ContextDialerFn) ConnectionOptions

	// DialTimeout returns the dial timeout.
	DialTimeout() time.Duration

	// SetDialTimeout sets the dial timeout.
	SetDialTimeout(value time.Duration) ConnectionOptions

	// WriteTimeout returns the write timeout.
	WriteTimeout() time.Duration

	// SetWriteTimeout sets the write timeout.
	SetWriteTimeout(value time.Duration) ConnectionOptions

	// KeepAlivePeriod returns the keepAlivePeriod.
	KeepAlivePeriod() time.Duration

	// SetKeepAlivePeriod sets the keepAlivePeriod.
	SetKeepAlivePeriod(value time.Duration) ConnectionOptions

	// ResetDelay returns the delay before resetting connection.
	ResetDelay() time.Duration

	// SetResetDelay sets the delay before resetting connection.
	SetResetDelay(value time.Duration) ConnectionOptions

	// RetryOptions returns the options for connection retrier.
	RetryOptions() retry.Options

	// SetRetryOptions sets the options for connection retrier.
	SetRetryOptions(value retry.Options) ConnectionOptions

	// FlushInterval returns the interval for flushing the buffered bytes.
	FlushInterval() time.Duration

	// SetFlushInterval sets the interval for flushing the buffered bytes.
	SetFlushInterval(value time.Duration) ConnectionOptions

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
	numConnections  int
	dialTimeout     time.Duration
	writeTimeout    time.Duration
	keepAlivePeriod time.Duration
	resetDelay      time.Duration
	rOpts           retry.Options
	flushInterval   time.Duration
	writeBufferSize int
	readBufferSize  int
	iOpts           instrument.Options
	dialer          ContextDialerFn
}

// NewConnectionOptions creates ConnectionOptions.
func NewConnectionOptions() ConnectionOptions {
	return &connectionOptions{
		numConnections:  defaultNumConnections,
		dialTimeout:     defaultConnectionDialTimeout,
		writeTimeout:    defaultConnectionWriteTimeout,
		keepAlivePeriod: defaultConnectionKeepAlivePeriod,
		resetDelay:      defaultConnectionResetDelay,
		rOpts:           retry.NewOptions(),
		flushInterval:   defaultConnectionFlushInterval,
		writeBufferSize: defaultConnectionBufferSize,
		readBufferSize:  defaultConnectionBufferSize,
		iOpts:           instrument.NewOptions(),
		dialer:          nil, // Will default to net.Dialer{}.DialContext
	}
}

func (opts *connectionOptions) NumConnections() int {
	return opts.numConnections
}

func (opts *connectionOptions) SetNumConnections(value int) ConnectionOptions {
	o := *opts
	o.numConnections = value
	return &o
}

func (opts *connectionOptions) DialTimeout() time.Duration {
	return opts.dialTimeout
}

func (opts *connectionOptions) SetDialTimeout(value time.Duration) ConnectionOptions {
	o := *opts
	o.dialTimeout = value
	return &o
}

func (opts *connectionOptions) ContextDialer() ContextDialerFn {
	return opts.dialer
}

func (opts *connectionOptions) SetContextDialer(fn ContextDialerFn) ConnectionOptions {
	o := *opts
	o.dialer = fn
	return &o
}

func (opts *connectionOptions) WriteTimeout() time.Duration {
	return opts.writeTimeout
}

func (opts *connectionOptions) SetWriteTimeout(value time.Duration) ConnectionOptions {
	o := *opts
	o.writeTimeout = value
	return &o
}

func (opts *connectionOptions) KeepAlivePeriod() time.Duration {
	return opts.keepAlivePeriod
}

func (opts *connectionOptions) SetKeepAlivePeriod(value time.Duration) ConnectionOptions {
	o := *opts
	o.keepAlivePeriod = value
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

func (opts *connectionOptions) FlushInterval() time.Duration {
	return opts.flushInterval
}

func (opts *connectionOptions) SetFlushInterval(value time.Duration) ConnectionOptions {
	o := *opts
	o.flushInterval = value
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

	// PlacementOptions returns the placement options.
	PlacementOptions() placement.Options

	// SetPlacementOptions sets the placement options.
	SetPlacementOptions(value placement.Options) Options

	// PlacementWatchInitTimeout returns the timeout for placement watch initialization.
	PlacementWatchInitTimeout() time.Duration

	// SetPlacementWatchInitTimeout sets the timeout for placement watch initialization.
	SetPlacementWatchInitTimeout(value time.Duration) Options

	// MessagePoolOptions returns the options of pool for messages.
	MessagePoolOptions() pool.ObjectPoolOptions

	// SetMessagePoolOptions sets the options of pool for messages.
	SetMessagePoolOptions(value pool.ObjectPoolOptions) Options

	// MessageRetryNanosFn returns the MessageRetryNanosFn.
	MessageRetryNanosFn() MessageRetryNanosFn

	// SetMessageRetryNanosFn sets the MessageRetryNanosFn.
	SetMessageRetryNanosFn(value MessageRetryNanosFn) Options

	// MessageQueueNewWritesScanInterval returns the interval between scanning
	// message queue for new writes.
	MessageQueueNewWritesScanInterval() time.Duration

	// SetMessageQueueNewWritesScanInterval sets the interval between scanning
	// message queue for new writes.
	SetMessageQueueNewWritesScanInterval(value time.Duration) Options

	// MessageQueueFullScanInterval returns the interval between scanning
	// message queue for retriable writes and cleanups.
	MessageQueueFullScanInterval() time.Duration

	// SetMessageQueueFullScanInterval sets the interval between scanning
	// message queue for retriable writes and cleanups.
	SetMessageQueueFullScanInterval(value time.Duration) Options

	// MessageQueueScanBatchSize returns the batch size for queue scan.
	MessageQueueScanBatchSize() int

	// SetMessageQueueScanBatchSize sets the batch size for queue scan.
	SetMessageQueueScanBatchSize(value int) Options

	// InitialAckMapSize returns the initial size of the ack map.
	InitialAckMapSize() int

	// SetInitialAckMapSize sets the initial size of the ack map.
	SetInitialAckMapSize(value int) Options

	// CloseCheckInterval returns the close check interval.
	CloseCheckInterval() time.Duration

	// SetCloseCheckInterval sets the close check interval.
	SetCloseCheckInterval(value time.Duration) Options

	// AckErrorRetryOptions returns the retrier for ack errors.
	AckErrorRetryOptions() retry.Options

	// SetAckErrorRetryOptions sets the retrier for ack errors.
	SetAckErrorRetryOptions(value retry.Options) Options

	// EncoderOptions returns the encoder's options.
	EncoderOptions() proto.Options

	// SetEncoderOptions sets the encoder's options.
	SetEncoderOptions(value proto.Options) Options

	// DecoderOptions returns the decoder's options.
	DecoderOptions() proto.Options

	// SetDecoderOptions sets the decoder's options.
	SetDecoderOptions(value proto.Options) Options

	// ConnectionOptions returns the options for connections.
	ConnectionOptions() ConnectionOptions

	// SetConnectionOptions sets the options for connections.
	SetConnectionOptions(value ConnectionOptions) Options

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) Options

	// IgnoreCutoffCutover returns a flag indicating whether cutoff/cutover timestamps are ignored.
	IgnoreCutoffCutover() bool

	// SetIgnoreCutoffCutover sets a flag controlling whether cutoff/cutover timestamps are ignored.
	SetIgnoreCutoffCutover(value bool) Options

	// WithoutConsumerScope disables the consumer scope for metrics. For large m3msg deployments the consumer
	// scope can add a lot of cardinality to the metrics.
	WithoutConsumerScope() bool

	// SetWithoutConsumerScope sets the value for WithoutConsumerScope.
	SetWithoutConsumerScope(value bool) Options
}

type writerOptions struct {
	topicName                         string
	topicService                      topic.Service
	topicWatchInitTimeout             time.Duration
	services                          services.Services
	placementOpts                     placement.Options
	placementWatchInitTimeout         time.Duration
	messageRetryNanosFn               MessageRetryNanosFn
	messagePoolOptions                pool.ObjectPoolOptions
	messageQueueNewWritesScanInterval time.Duration
	messageQueueFullScanInterval      time.Duration
	messageQueueScanBatchSize         int
	initialAckMapSize                 int
	closeCheckInterval                time.Duration
	ackErrRetryOpts                   retry.Options
	encOpts                           proto.Options
	decOpts                           proto.Options
	cOpts                             ConnectionOptions
	iOpts                             instrument.Options
	ignoreCutoffCutover               bool
	withoutConsumerScope              bool
}

// NewOptions creates Options.
func NewOptions() Options {
	messageRetryOpts := retry.NewOptions().
		SetInitialBackoff(defaultWriterRetryInitialBackoff)
	return &writerOptions{
		topicWatchInitTimeout:             defaultTopicWatchInitTimeout,
		placementOpts:                     placement.NewOptions(),
		placementWatchInitTimeout:         defaultPlacementWatchInitTimeout,
		messageRetryNanosFn:               NextRetryNanosFn(messageRetryOpts),
		messageQueueNewWritesScanInterval: defaultMessageQueueNewWritesScanInterval,
		messageQueueFullScanInterval:      defaultMessageQueueFullScanInterval,
		messageQueueScanBatchSize:         defaultMessageQueueScanBatchSize,
		initialAckMapSize:                 defaultInitialAckMapSize,
		closeCheckInterval:                defaultCloseCheckInterval,
		ackErrRetryOpts:                   retry.NewOptions(),
		encOpts:                           proto.NewOptions(),
		decOpts:                           proto.NewOptions(),
		cOpts:                             NewConnectionOptions(),
		iOpts:                             instrument.NewOptions(),
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

func (opts *writerOptions) PlacementOptions() placement.Options {
	return opts.placementOpts
}

func (opts *writerOptions) SetPlacementOptions(value placement.Options) Options {
	o := *opts
	o.placementOpts = value
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

func (opts *writerOptions) MessageRetryNanosFn() MessageRetryNanosFn {
	return opts.messageRetryNanosFn
}

func (opts *writerOptions) SetMessageRetryNanosFn(value MessageRetryNanosFn) Options {
	o := *opts
	o.messageRetryNanosFn = value
	return &o
}

func (opts *writerOptions) MessageQueueNewWritesScanInterval() time.Duration {
	return opts.messageQueueNewWritesScanInterval
}

func (opts *writerOptions) SetMessageQueueNewWritesScanInterval(value time.Duration) Options {
	o := *opts
	o.messageQueueNewWritesScanInterval = value
	return &o
}

func (opts *writerOptions) MessageQueueFullScanInterval() time.Duration {
	return opts.messageQueueFullScanInterval
}

func (opts *writerOptions) SetMessageQueueFullScanInterval(value time.Duration) Options {
	o := *opts
	o.messageQueueFullScanInterval = value
	return &o
}

func (opts *writerOptions) MessageQueueScanBatchSize() int {
	return opts.messageQueueScanBatchSize
}

func (opts *writerOptions) SetMessageQueueScanBatchSize(value int) Options {
	o := *opts
	o.messageQueueScanBatchSize = value
	return &o
}

func (opts *writerOptions) InitialAckMapSize() int {
	return opts.initialAckMapSize
}

func (opts *writerOptions) SetInitialAckMapSize(value int) Options {
	o := *opts
	o.initialAckMapSize = value
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

func (opts *writerOptions) EncoderOptions() proto.Options {
	return opts.encOpts
}

func (opts *writerOptions) SetEncoderOptions(value proto.Options) Options {
	o := *opts
	o.encOpts = value
	return &o
}

func (opts *writerOptions) DecoderOptions() proto.Options {
	return opts.decOpts
}

func (opts *writerOptions) SetDecoderOptions(value proto.Options) Options {
	o := *opts
	o.decOpts = value
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

func (opts *writerOptions) IgnoreCutoffCutover() bool {
	return opts.ignoreCutoffCutover
}

func (opts *writerOptions) SetIgnoreCutoffCutover(value bool) Options {
	o := *opts
	o.ignoreCutoffCutover = value
	return &o
}

func (opts *writerOptions) WithoutConsumerScope() bool {
	return opts.withoutConsumerScope
}

func (opts *writerOptions) SetWithoutConsumerScope(value bool) Options {
	o := *opts
	o.withoutConsumerScope = value
	return &o
}
