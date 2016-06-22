// Copyright (c) 2016 Uber Technologies, Inc.
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

package client

import (
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/x/logging"
	"github.com/m3db/m3db/x/metrics"

	"github.com/uber/tchannel-go"
)

const (
	// defaultMaxConnectionCount is the default max connection count
	defaultMaxConnectionCount = 32

	// defaultMinConnectionCount is the default min connection count
	defaultMinConnectionCount = 2

	// defaultHostConnectTimeout is the default host connection timeout
	defaultHostConnectTimeout = 5 * time.Second

	// defaultClusterConnectTimeout is the default cluster connect timeout
	defaultClusterConnectTimeout = 30 * time.Second

	// defaultWriteRequestTimeout is the default write request timeout
	defaultWriteRequestTimeout = 5 * time.Second

	// defaultWriteOpPoolSize is the default write op pool size
	defaultWriteOpPoolSize = 1000000

	// defaultWriteBatchSize is the default write batch size
	defaultWriteBatchSize = 128

	// defaultHostQueueOpsFlushSize is the default host queue ops flush size
	defaultHostQueueOpsFlushSize = 128

	// defaultHostQueueOpsFlushInterval is the default host queue flush interval
	defaultHostQueueOpsFlushInterval = time.Millisecond

	// defaultHostQueueOpsArrayPoolSize is the default host queue ops array pool size
	defaultHostQueueOpsArrayPoolSize = 8

	// defaultBackgroundConnectInterval is the default background connect interval
	defaultBackgroundConnectInterval = 4 * time.Second

	// defaultBackgroundConnectStutter is the default background connect stutter
	defaultBackgroundConnectStutter = 1 * time.Second

	// defaultBackgroundHealthCheckInterval is the default background health check interval
	defaultBackgroundHealthCheckInterval = 1 * time.Second

	// defaultBackgroundHealthCheckStutter is the default background health check stutter
	defaultBackgroundHealthCheckStutter = 1 * time.Second
)

type options struct {
	logger                        logging.Logger
	scope                         metrics.Scope
	topologyType                  m3db.TopologyType
	consistencyLevel              m3db.ConsistencyLevel
	channelOptions                *tchannel.ChannelOptions
	nowFn                         m3db.NowFn
	maxConnectionCount            int
	minConnectionCount            int
	hostConnectTimeout            time.Duration
	clusterConnectTimeout         time.Duration
	writeRequestTimeout           time.Duration
	backgroundConnectInterval     time.Duration
	backgroundConnectStutter      time.Duration
	backgroundHealthCheckInterval time.Duration
	backgroundHealthCheckStutter  time.Duration
	writeOpPoolSize               int
	writeBatchSize                int
	hostQueueOpsFlushSize         int
	hostQueueOpsFlushInterval     time.Duration
	hostQueueOpsArrayPoolSize     int
}

// NewOptions creates a new set of client options with defaults
// TODO(r): add an "IsValid()" method and ensure topology type is set
func NewOptions() m3db.ClientOptions {
	return &options{
		logger:                        logging.SimpleLogger,
		scope:                         metrics.NoopScope,
		consistencyLevel:              m3db.ConsistencyLevelQuorum,
		nowFn:                         time.Now,
		maxConnectionCount:            defaultMaxConnectionCount,
		minConnectionCount:            defaultMinConnectionCount,
		hostConnectTimeout:            defaultHostConnectTimeout,
		clusterConnectTimeout:         defaultClusterConnectTimeout,
		writeRequestTimeout:           defaultWriteRequestTimeout,
		backgroundConnectInterval:     defaultBackgroundConnectInterval,
		backgroundConnectStutter:      defaultBackgroundConnectStutter,
		backgroundHealthCheckInterval: defaultBackgroundHealthCheckInterval,
		backgroundHealthCheckStutter:  defaultBackgroundHealthCheckStutter,
		writeOpPoolSize:               defaultWriteOpPoolSize,
		writeBatchSize:                defaultWriteBatchSize,
		hostQueueOpsFlushSize:         defaultHostQueueOpsFlushSize,
		hostQueueOpsFlushInterval:     defaultHostQueueOpsFlushInterval,
		hostQueueOpsArrayPoolSize:     defaultHostQueueOpsArrayPoolSize,
	}
}

func (o *options) Logger(value logging.Logger) m3db.ClientOptions {
	opts := *o
	opts.logger = value
	return &opts
}

func (o *options) GetLogger() logging.Logger {
	return o.logger
}

func (o *options) MetricsScope(value metrics.Scope) m3db.ClientOptions {
	opts := *o
	opts.scope = value
	return &opts
}

func (o *options) GetMetricsScope() metrics.Scope {
	return o.scope
}

func (o *options) TopologyType(value m3db.TopologyType) m3db.ClientOptions {
	opts := *o
	opts.topologyType = value
	return &opts
}

func (o *options) GetTopologyType() m3db.TopologyType {
	return o.topologyType
}

func (o *options) ConsistencyLevel(value m3db.ConsistencyLevel) m3db.ClientOptions {
	opts := *o
	opts.consistencyLevel = value
	return &opts
}

func (o *options) GetConsistencyLevel() m3db.ConsistencyLevel {
	return o.consistencyLevel
}

func (o *options) ChannelOptions(value *tchannel.ChannelOptions) m3db.ClientOptions {
	opts := *o
	opts.channelOptions = value
	return &opts
}

func (o *options) GetChannelOptions() *tchannel.ChannelOptions {
	return o.channelOptions
}

func (o *options) NowFn(value m3db.NowFn) m3db.ClientOptions {
	opts := *o
	opts.nowFn = value
	return &opts
}

func (o *options) GetNowFn() m3db.NowFn {
	return o.nowFn
}

func (o *options) MaxConnectionCount(value int) m3db.ClientOptions {
	opts := *o
	opts.maxConnectionCount = value
	return &opts
}

func (o *options) GetMaxConnectionCount() int {
	return o.maxConnectionCount
}

func (o *options) MinConnectionCount(value int) m3db.ClientOptions {
	opts := *o
	opts.minConnectionCount = value
	return &opts
}

func (o *options) GetMinConnectionCount() int {
	return o.minConnectionCount
}

func (o *options) HostConnectTimeout(value time.Duration) m3db.ClientOptions {
	opts := *o
	opts.hostConnectTimeout = value
	return &opts
}

func (o *options) GetHostConnectTimeout() time.Duration {
	return o.hostConnectTimeout
}

func (o *options) ClusterConnectTimeout(value time.Duration) m3db.ClientOptions {
	opts := *o
	opts.clusterConnectTimeout = value
	return &opts
}

func (o *options) GetClusterConnectTimeout() time.Duration {
	return o.clusterConnectTimeout
}

func (o *options) WriteRequestTimeout(value time.Duration) m3db.ClientOptions {
	opts := *o
	opts.writeRequestTimeout = value
	return &opts
}

func (o *options) GetWriteRequestTimeout() time.Duration {
	return o.writeRequestTimeout
}

func (o *options) BackgroundConnectInterval(value time.Duration) m3db.ClientOptions {
	opts := *o
	opts.backgroundConnectInterval = value
	return &opts
}

func (o *options) GetBackgroundConnectInterval() time.Duration {
	return o.writeRequestTimeout
}

func (o *options) BackgroundConnectStutter(value time.Duration) m3db.ClientOptions {
	opts := *o
	opts.backgroundConnectStutter = value
	return &opts
}

func (o *options) GetBackgroundConnectStutter() time.Duration {
	return o.backgroundConnectStutter
}

func (o *options) BackgroundHealthCheckInterval(value time.Duration) m3db.ClientOptions {
	opts := *o
	opts.backgroundHealthCheckInterval = value
	return &opts
}

func (o *options) GetBackgroundHealthCheckInterval() time.Duration {
	return o.backgroundHealthCheckInterval
}

func (o *options) BackgroundHealthCheckStutter(value time.Duration) m3db.ClientOptions {
	opts := *o
	opts.backgroundHealthCheckStutter = value
	return &opts
}

func (o *options) GetBackgroundHealthCheckStutter() time.Duration {
	return o.backgroundHealthCheckStutter
}

func (o *options) WriteOpPoolSize(value int) m3db.ClientOptions {
	opts := *o
	opts.writeOpPoolSize = value
	return &opts
}

func (o *options) GetWriteOpPoolSize() int {
	return o.writeOpPoolSize
}

func (o *options) WriteBatchSize(value int) m3db.ClientOptions {
	opts := *o
	opts.writeBatchSize = value
	return &opts
}

func (o *options) GetWriteBatchSize() int {
	return o.writeBatchSize
}

func (o *options) HostQueueOpsFlushSize(value int) m3db.ClientOptions {
	opts := *o
	opts.hostQueueOpsFlushSize = value
	return &opts
}

func (o *options) GetHostQueueOpsFlushSize() int {
	return o.hostQueueOpsFlushSize
}

func (o *options) HostQueueOpsFlushInterval(value time.Duration) m3db.ClientOptions {
	opts := *o
	opts.hostQueueOpsFlushInterval = value
	return &opts
}

func (o *options) GetHostQueueOpsFlushInterval() time.Duration {
	return o.hostQueueOpsFlushInterval
}

func (o *options) HostQueueOpsArrayPoolSize(value int) m3db.ClientOptions {
	opts := *o
	opts.hostQueueOpsArrayPoolSize = value
	return &opts
}

func (o *options) GetHostQueueOpsArrayPoolSize() int {
	return o.hostQueueOpsArrayPoolSize
}
