// Copyright (c) 2017 Uber Technologies, Inc.
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

package m3

import (
	"bytes"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber-go/tally/m3/customtransports"
	m3thrift "github.com/uber-go/tally/m3/thrift"
	"github.com/uber-go/tally/m3/thriftudp"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	numReaders    = 10
	queueSize     = 1000
	includeHost   = true
	maxPacketSize = int32(1440)
	shortInterval = 10 * time.Millisecond
)

var localListenAddr = &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}
var defaultCommonTags = map[string]string{"env": "test", "host": "test"}

var protocols = []Protocol{Compact, Binary}

// TestReporter tests the reporter works as expected with both compact and binary protocols
func TestReporter(t *testing.T) {
	for _, protocol := range protocols {
		var wg sync.WaitGroup
		server := newFakeM3Server(t, &wg, true, protocol)
		go server.Serve()
		defer server.Close()

		commonTags = map[string]string{
			"env":        "development",
			"host":       hostname(),
			"commonTag":  "common",
			"commonTag2": "tag",
			"commonTag3": "val",
		}
		r, err := NewReporter(Options{
			HostPorts:          []string{server.Addr},
			Service:            "testService",
			CommonTags:         commonTags,
			IncludeHost:        includeHost,
			Protocol:           protocol,
			MaxQueueSize:       queueSize,
			MaxPacketSizeBytes: maxPacketSize,
		})
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, r.Close())
		}()

		tags := map[string]string{"testTag": "TestValue", "testTag2": "TestValue2"}

		wg.Add(2)

		r.AllocateCounter("my-counter", tags).ReportCount(10)
		r.Flush()

		r.AllocateTimer("my-timer", tags).ReportTimer(5 * time.Millisecond)
		r.Flush()

		wg.Wait()

		batches := server.Service.getBatches()
		require.Equal(t, 2, len(batches))

		// Validate common tags
		for _, batch := range batches {
			require.NotNil(t, batch)
			require.True(t, batch.IsSetCommonTags())
			require.Equal(t, len(commonTags)+1, len(batch.GetCommonTags()))
			for tag := range batch.GetCommonTags() {
				if tag.GetTagName() == ServiceTag {
					require.Equal(t, "testService", tag.GetTagValue())
				} else {
					require.Equal(t, commonTags[tag.GetTagName()], tag.GetTagValue())
				}
			}
		}

		//Validate metrics
		emittedCounters := batches[0].GetMetrics()
		require.Equal(t, 1, len(emittedCounters))
		emittedTimers := batches[1].GetMetrics()
		require.Equal(t, 1, len(emittedTimers))

		emittedCounter, emittedTimer := emittedCounters[0], emittedTimers[0]
		if emittedCounter.GetName() == "my-timer" {
			emittedCounter, emittedTimer = emittedTimer, emittedCounter
		}

		require.Equal(t, "my-counter", emittedCounter.GetName())
		require.True(t, emittedCounter.IsSetTags())
		require.Equal(t, len(tags), len(emittedCounter.GetTags()))
		for tag := range emittedCounter.GetTags() {
			require.Equal(t, tags[tag.GetTagName()], tag.GetTagValue())
		}
		require.True(t, emittedCounter.IsSetMetricValue())
		emittedVal := emittedCounter.GetMetricValue()
		require.True(t, emittedVal.IsSetCount())
		require.False(t, emittedVal.IsSetGauge())
		require.False(t, emittedVal.IsSetTimer())
		emittedCount := emittedVal.GetCount()
		require.True(t, emittedCount.IsSetI64Value())
		require.EqualValues(t, int64(10), emittedCount.GetI64Value())

		require.True(t, emittedTimer.IsSetMetricValue())
		emittedVal = emittedTimer.GetMetricValue()
		require.False(t, emittedVal.IsSetCount())
		require.False(t, emittedVal.IsSetGauge())
		require.True(t, emittedVal.IsSetTimer())
		emittedTimerVal := emittedVal.GetTimer()
		require.True(t, emittedTimerVal.IsSetI64Value())
		require.EqualValues(t, int64(5*1000*1000), emittedTimerVal.GetI64Value())
	}
}

// TestMultiReporter tests the multi Reporter works as expected
func TestMultiReporter(t *testing.T) {
	dests := []string{"127.0.0.1:9052", "127.0.0.1:9053"}
	commonTags := map[string]string{
		"env":        "test",
		"host":       "test",
		"commonTag":  "common",
		"commonTag2": "tag",
		"commonTag3": "val",
	}
	r, err := NewReporter(Options{
		HostPorts:  dests,
		Service:    "testService",
		CommonTags: commonTags,
	})
	require.NoError(t, err)
	defer r.Close()

	reporter, ok := r.(*reporter)
	require.True(t, ok)
	multitransport, ok := reporter.client.Transport.(*thriftudp.TMultiUDPTransport)
	require.NotNil(t, multitransport)
	require.True(t, ok)
}

// TestNewReporterErrors tests for Reporter creation errors
func TestNewReporterErrors(t *testing.T) {
	var err error
	// Test freeBytes (maxPacketSizeBytes - numOverheadBytes) is negative
	_, err = NewReporter(Options{
		HostPorts:          []string{"127.0.0.1"},
		Service:            "testService",
		MaxQueueSize:       10,
		MaxPacketSizeBytes: 2 << 5,
	})
	assert.Error(t, err)
	// Test invalid addr
	_, err = NewReporter(Options{
		HostPorts: []string{"fakeAddress"},
		Service:   "testService",
	})
	assert.Error(t, err)
}

// TestReporterFinalFlush ensures the Reporter emits the last batch of metrics
// after close
func TestReporterFinalFlush(t *testing.T) {
	var wg sync.WaitGroup
	server := newFakeM3Server(t, &wg, true, Compact)
	go server.Serve()
	defer server.Close()

	r, err := NewReporter(Options{
		HostPorts:          []string{server.Addr},
		Service:            "testService",
		CommonTags:         defaultCommonTags,
		MaxQueueSize:       queueSize,
		MaxPacketSizeBytes: maxPacketSize,
	})
	require.NoError(t, err)

	wg.Add(1)

	r.AllocateTimer("my-timer", nil).ReportTimer(10 * time.Millisecond)
	r.Close()

	wg.Wait()

	require.Equal(t, 1, len(server.Service.getBatches()))
	require.NotNil(t, server.Service.getBatches()[0])
	require.Equal(t, 1, len(server.Service.getBatches()[0].GetMetrics()))
}

func TestBatchSizes(t *testing.T) {
	server := newSimpleServer(t)
	go server.serve()
	defer server.close()

	commonTags := map[string]string{
		"env":    "test",
		"domain": "pod" + strconv.Itoa(rand.Intn(100)),
	}
	maxPacketSize := int32(1440)
	r, err := NewReporter(Options{
		HostPorts:          []string{server.addr()},
		Service:            "testService",
		CommonTags:         commonTags,
		MaxQueueSize:       10000,
		MaxPacketSizeBytes: maxPacketSize,
	})

	require.NoError(t, err)
	rand.Seed(time.Now().UnixNano())

	var stop uint32
	go func() {
		var (
			counters = make(map[string]tally.CachedCount)
			gauges   = make(map[string]tally.CachedGauge)
			timers   = make(map[string]tally.CachedTimer)
			randTags = func() map[string]string {
				return map[string]string{
					"t1": "val" + strconv.Itoa(rand.Intn(10000)),
				}
			}
		)
		for atomic.LoadUint32(&stop) == 0 {
			metTypeRand := rand.Intn(9)
			name := "size.test.metric.name" + strconv.Itoa(rand.Intn(50))

			if metTypeRand <= 2 {
				_, ok := counters[name]
				if !ok {
					counters[name] = r.AllocateCounter(name, randTags())
				}
				counters[name].ReportCount(rand.Int63n(10000))
			} else if metTypeRand <= 5 {
				_, ok := gauges[name]
				if !ok {
					gauges[name] = r.AllocateGauge(name, randTags())
				}
				gauges[name].ReportGauge(rand.Float64() * 10000)
			} else {
				_, ok := timers[name]
				if !ok {
					timers[name] = r.AllocateTimer(name, randTags())
				}
				timers[name].ReportTimer(time.Duration(rand.Int63n(10000)))
			}
		}
		r.Close()
	}()

	for len(server.getPackets()) < 100 {
		time.Sleep(shortInterval)
	}

	atomic.StoreUint32(&stop, 1)
	for _, packet := range server.getPackets() {
		require.True(t, len(packet) < int(maxPacketSize))
	}
}

func TestReporterSpecifyService(t *testing.T) {
	commonTags := map[string]string{
		ServiceTag: "overrideService",
		EnvTag:     "test",
		HostTag:    "overrideHost",
	}
	r, err := NewReporter(Options{
		HostPorts:    []string{"127.0.0.1:1000"},
		Service:      "testService",
		CommonTags:   commonTags,
		IncludeHost:  includeHost,
		MaxQueueSize: 10, MaxPacketSizeBytes: 100,
	})
	require.NoError(t, err)
	defer r.Close()

	reporter, ok := r.(*reporter)
	require.True(t, ok)
	assert.Equal(t, 3, len(reporter.commonTags))
	for tag := range reporter.commonTags {
		switch tag.GetTagName() {
		case ServiceTag:
			assert.Equal(t, "overrideService", tag.GetTagValue())
		case EnvTag:
			assert.Equal(t, "test", tag.GetTagValue())
		case HostTag:
			assert.Equal(t, "overrideHost", tag.GetTagValue())
		}
	}
}

func TestIncludeHost(t *testing.T) {
	var wg sync.WaitGroup
	server := newFakeM3Server(t, &wg, true, Compact)
	go server.Serve()
	defer server.Close()

	commonTags := map[string]string{"env": "test"}
	r, err := NewReporter(Options{
		HostPorts:   []string{server.Addr},
		Service:     "testService",
		CommonTags:  commonTags,
		IncludeHost: false,
	})
	require.NoError(t, err)
	defer r.Close()
	withoutHost, ok := r.(*reporter)
	require.True(t, ok)
	assert.False(t, tagIncluded(withoutHost.commonTags, "host"))

	r, err = NewReporter(Options{
		HostPorts:   []string{server.Addr},
		Service:     "testService",
		CommonTags:  commonTags,
		IncludeHost: true,
	})
	require.NoError(t, err)
	defer r.Close()
	withHost, ok := r.(*reporter)
	require.True(t, ok)
	assert.True(t, tagIncluded(withHost.commonTags, "host"))
}

func TestReporterHasReportingAndTaggingCapability(t *testing.T) {
	r, err := NewReporter(Options{
		HostPorts:  []string{"127.0.0.1:9052"},
		Service:    "testService",
		CommonTags: defaultCommonTags,
	})
	require.Nil(t, err)

	assert.True(t, r.Capabilities().Reporting())
	assert.True(t, r.Capabilities().Tagging())
}

type simpleServer struct {
	conn    *net.UDPConn
	t       *testing.T
	packets [][]byte
	sync.Mutex
	closed int32
}

func newSimpleServer(t *testing.T) *simpleServer {
	addr, err := net.ResolveUDPAddr("udp", ":0")
	require.NoError(t, err)

	conn, err := net.ListenUDP(addr.Network(), addr)
	require.NoError(t, err)

	return &simpleServer{conn: conn, t: t}
}

func (s *simpleServer) serve() {
	readBuf := make([]byte, 64000)
	for atomic.LoadInt32(&s.closed) == 0 {
		n, err := s.conn.Read(readBuf)
		if err != nil {
			if atomic.LoadInt32(&s.closed) == 0 {
				s.t.Errorf("FakeM3Server failed to Read: %v", err)
			}
			return
		}
		s.Lock()
		s.packets = append(s.packets, readBuf[0:n])
		s.Unlock()
		readBuf = make([]byte, 64000)
	}
}

func (s *simpleServer) getPackets() [][]byte {
	s.Lock()
	defer s.Unlock()
	copy := make([][]byte, len(s.packets))
	for i, packet := range s.packets {
		copy[i] = packet
	}

	return copy
}

func (s *simpleServer) close() error {
	atomic.AddInt32(&s.closed, 1)
	return s.conn.Close()
}

func (s *simpleServer) addr() string {
	return s.conn.LocalAddr().String()
}

type fakeM3Server struct {
	t         *testing.T
	Service   *fakeM3Service
	Addr      string
	protocol  Protocol
	processor thrift.TProcessor
	conn      *net.UDPConn
	closed    int32
}

func newFakeM3Server(t *testing.T, wg *sync.WaitGroup, countBatches bool, protocol Protocol) *fakeM3Server {
	service := newFakeM3Service(wg, countBatches)
	processor := m3thrift.NewM3Processor(service)
	conn, err := net.ListenUDP(localListenAddr.Network(), localListenAddr)
	require.NoError(t, err, "ListenUDP failed")

	return &fakeM3Server{
		t:         t,
		Service:   service,
		Addr:      conn.LocalAddr().String(),
		conn:      conn,
		protocol:  protocol,
		processor: processor,
	}
}

func (f *fakeM3Server) Serve() {
	readBuf := make([]byte, 64000)
	for f.conn != nil {
		n, err := f.conn.Read(readBuf)
		if err != nil {
			if atomic.LoadInt32(&f.closed) == 0 {
				f.t.Errorf("FakeM3Server failed to Read: %v", err)
			}
			return
		}
		trans, _ := customtransport.NewTBufferedReadTransport(bytes.NewBuffer(readBuf[0:n]))
		var proto thrift.TProtocol
		if f.protocol == Compact {
			proto = thrift.NewTCompactProtocol(trans)
		} else {
			proto = thrift.NewTBinaryProtocolTransport(trans)
		}
		f.processor.Process(proto, proto)
	}
}

func (f *fakeM3Server) Close() error {
	atomic.AddInt32(&f.closed, 1)
	return f.conn.Close()
}

func newFakeM3Service(wg *sync.WaitGroup, countBatches bool) *fakeM3Service {
	return &fakeM3Service{wg: wg, countBatches: countBatches}
}

type fakeM3Service struct {
	lock         sync.RWMutex
	batches      []*m3thrift.MetricBatch
	metrics      []*m3thrift.Metric
	wg           *sync.WaitGroup
	countBatches bool
}

func (m *fakeM3Service) getBatches() []*m3thrift.MetricBatch {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.batches
}

func (m *fakeM3Service) getMetrics() []*m3thrift.Metric {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.metrics
}

func (m *fakeM3Service) EmitMetricBatch(batch *m3thrift.MetricBatch) (err error) {
	m.lock.Lock()
	m.batches = append(m.batches, batch)
	if m.countBatches {
		m.wg.Done()
	}

	for _, metric := range batch.Metrics {
		m.metrics = append(m.metrics, metric)
		if !m.countBatches {
			m.wg.Done()
		}
	}

	m.lock.Unlock()
	return thrift.NewTTransportException(thrift.END_OF_FILE, "complete")
}

func hostname() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	return host
}

func tagIncluded(tags map[*m3thrift.MetricTag]bool, tagName string) bool {
	for k, v := range tags {
		if v && k.TagName == tagName {
			return true
		}
	}
	return false
}

func tagEquals(tags map[*m3thrift.MetricTag]bool, tagName, tagValue string) bool {
	for k, v := range tags {
		if v && k.GetTagName() == tagName {
			return k.GetTagValue() == tagValue
		}
	}
	return false
}
