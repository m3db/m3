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

package integration

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/placement/service"
	"github.com/m3db/m3/src/cluster/placement/storage"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/msg/consumer"
	"github.com/m3db/m3/src/msg/producer"
	"github.com/m3db/m3/src/msg/producer/config"
	"github.com/m3db/m3/src/msg/topic"
	"github.com/m3db/m3/src/x/instrument"
	xio "github.com/m3db/m3/src/x/io"
	xsync "github.com/m3db/m3/src/x/sync"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	yaml "gopkg.in/yaml.v2"
)

const (
	numConcurrentMessages = 10
	numberOfShards        = 10
	msgPerShard           = 200
	closeTimeout          = 30 * time.Second
	topicName             = "topicName"
)

type consumerServiceConfig struct {
	ct        topic.ConsumptionType
	instances int
	replicas  int
	isSharded bool
	lateJoin  bool
}

type op struct {
	progressPct int
	fn          func()
}

type setup struct {
	ts               topic.Service
	sd               *services.MockServices
	producers        []producer.Producer
	consumerServices []*testConsumerService
	totalConsumed    *atomic.Int64
	extraOps         []op
}

func newTestSetup(
	t *testing.T,
	ctrl *gomock.Controller,
	numProducers int,
	configs []consumerServiceConfig,
) *setup {
	zap.L().Sugar().Debugf("setting up a test with %d producers", numProducers)

	configService := client.NewMockClient(ctrl)
	configService.EXPECT().Store(gomock.Any()).Return(mem.NewStore(), nil).AnyTimes()

	sd := services.NewMockServices(ctrl)
	configService.EXPECT().Services(gomock.Any()).Return(sd, nil).AnyTimes()

	var (
		testConsumerServices  []*testConsumerService
		topicConsumerServices []topic.ConsumerService
		totalConsumed         = atomic.NewInt64(0)
	)
	for i, config := range configs {
		zap.L().Sugar().Debugf("setting up a consumer service in %s mode with %d replicas", config.ct.String(), config.replicas)
		cs := newTestConsumerService(t, i, config, sd, numProducers, totalConsumed)
		topicConsumerServices = append(topicConsumerServices, cs.consumerService)
		testConsumerServices = append(testConsumerServices, cs)
	}

	ts, err := topic.NewService(topic.NewServiceOptions().SetConfigService(configService))
	require.NoError(t, err)

	testTopic := topic.NewTopic().
		SetName(topicName).
		SetNumberOfShards(uint32(numberOfShards)).
		SetConsumerServices(topicConsumerServices)
	_, err = ts.CheckAndSet(testTopic, kv.UninitializedVersion)
	require.NoError(t, err)

	var producers []producer.Producer
	for i := 0; i < numProducers; i++ {
		p := testProducer(t, configService)
		require.NoError(t, p.Init())
		producers = append(producers, p)
	}

	return &setup{
		ts:               ts,
		sd:               sd,
		producers:        producers,
		consumerServices: testConsumerServices,
		totalConsumed:    totalConsumed,
	}
}

func newTestConsumerService(
	t *testing.T,
	i int,
	config consumerServiceConfig,
	sd *services.MockServices,
	numProducers int,
	totalConsumed *atomic.Int64,
) *testConsumerService {
	sid := serviceID(i)
	consumerService := topic.NewConsumerService().SetServiceID(sid).SetConsumptionType(config.ct)

	ps := testPlacementService(mem.NewStore(), sid, config.isSharded)
	sd.EXPECT().PlacementService(sid, gomock.Any()).Return(ps, nil).Times(numProducers)

	cs := testConsumerService{
		consumed:         make(map[string]struct{}),
		sid:              sid,
		placementService: ps,
		consumerService:  consumerService,
		config:           config,
	}
	var (
		instances []placement.Instance
		p         placement.Placement
		err       error
	)
	for i := 0; i < config.instances; i++ {
		c := newTestConsumer(t, &cs)
		c.consumeAndAck(totalConsumed)
		cs.testConsumers = append(cs.testConsumers, c)
		instances = append(instances, c.instance)
	}
	if config.isSharded {
		p, err = ps.BuildInitialPlacement(instances, numberOfShards, config.replicas)
	} else {
		p, err = ps.BuildInitialPlacement(instances, 0, config.replicas)
	}
	require.NoError(t, err)
	require.Equal(t, len(instances), p.NumInstances())
	return &cs
}

func (s *setup) TotalMessages() int {
	return msgPerShard * numberOfShards * len(s.producers)
}

func (s *setup) ExpectedNumMessages() int {
	return msgPerShard * numberOfShards
}

func (s *setup) Run(
	t *testing.T,
	ctrl *gomock.Controller,
) {
	numWritesPerProducer := s.ExpectedNumMessages()
	mockData := make([]producer.Message, 0, numWritesPerProducer)
	for i := 0; i < numberOfShards; i++ {
		for j := 0; j < msgPerShard; j++ {
			b := fmt.Sprintf("foo%d-%d", i, j)
			mm := producer.NewMockMessage(ctrl)
			mm.EXPECT().Size().Return(len(b)).AnyTimes()
			mm.EXPECT().Bytes().Return([]byte(b)).AnyTimes()
			mm.EXPECT().Shard().Return(uint32(i)).AnyTimes()
			mm.EXPECT().Finalize(producer.Consumed).Times(len(s.producers))
			mockData = append(mockData, mm)
		}
	}

	ops := make(map[int]func(), len(s.extraOps))
	for _, op := range s.extraOps {
		num := op.progressPct * numWritesPerProducer / 100
		ops[num] = op.fn
	}
	zap.L().Sugar().Debug("producing messages")
	for i := 0; i < numWritesPerProducer; i++ {
		if fn, ok := ops[i]; ok {
			fn()
		}
		m := mockData[i]
		for _, p := range s.producers {
			require.NoError(t, p.Produce(m))
		}
	}
	zap.L().Sugar().Debug("produced all the messages")
	s.CloseProducers(closeTimeout)
	s.CloseConsumers()

	expectedConsumeReplica := 0
	for _, cs := range s.consumerServices {
		if cs.config.lateJoin {
			continue
		}
		if cs.config.ct == topic.Shared {
			expectedConsumeReplica++
			continue
		}
		expectedConsumeReplica += cs.config.replicas
	}
	expectedConsumed := expectedConsumeReplica * numWritesPerProducer * len(s.producers)
	require.True(t, int(s.totalConsumed.Load()) >= expectedConsumed, fmt.Sprintf("expect %d, consumed %d", expectedConsumed, s.totalConsumed.Load()))
	zap.L().Sugar().Debug("done")
}

func (s *setup) VerifyConsumers(t *testing.T) {
	numWritesPerProducer := s.ExpectedNumMessages()
	for _, cs := range s.consumerServices {
		require.Equal(t, numWritesPerProducer, cs.numConsumed())
	}
}

func (s *setup) CloseProducers(dur time.Duration) {
	doneCh := make(chan struct{})

	go func() {
		for _, p := range s.producers {
			zap.L().Sugar().Debug("closing producer")
			p.Close(producer.WaitForConsumption)
			zap.L().Sugar().Debug("closed producer")
		}
		close(doneCh)
	}()

	select {
	case <-time.After(dur):
		panic(fmt.Sprintf("taking more than %v to close producers %v", dur, time.Now()))
	case <-doneCh:
		zap.L().Sugar().Debugf("producer closed in %v", dur)
		return
	}
}

func (s setup) CloseConsumers() {
	for _, cs := range s.consumerServices {
		cs.Close()
	}
}

func (s *setup) ScheduleOperations(pct int, fn func()) {
	if pct < 0 || pct > 100 {
		return
	}
	s.extraOps = append(s.extraOps, op{progressPct: pct, fn: fn})
}

func (s *setup) KillConnection(t *testing.T, idx int) {
	require.True(t, idx < len(s.consumerServices))
	cs := s.consumerServices[idx]

	testConsumers := cs.testConsumers
	require.NotEmpty(t, testConsumers)
	c := testConsumers[len(testConsumers)-1]
	c.closeOneConsumer()

	zap.L().Sugar().Debugf("killed a consumer on instance: %s", c.instance.ID())
	p, err := cs.placementService.Placement()
	require.NoError(t, err)
	zap.L().Sugar().Debugf("placement: %s", p.String())
}

func (s *setup) KillInstance(t *testing.T, idx int) {
	require.True(t, idx < len(s.consumerServices))
	cs := s.consumerServices[idx]

	testConsumers := cs.testConsumers
	require.NotEmpty(t, testConsumers)
	c := testConsumers[len(testConsumers)-1]
	c.Close()

	zap.L().Sugar().Debugf("killed instance: %s", c.instance.ID())
	p, err := cs.placementService.Placement()
	require.NoError(t, err)
	zap.L().Sugar().Debugf("placement: %s", p.String())
}

func (s *setup) AddInstance(t *testing.T, idx int) {
	require.True(t, idx < len(s.consumerServices))
	cs := s.consumerServices[idx]

	newConsumer := newTestConsumer(t, cs)
	newConsumer.consumeAndAck(s.totalConsumed)

	p, err := cs.placementService.Placement()
	require.NoError(t, err)
	zap.L().Sugar().Debugf("old placement: %s", p.String())

	p, _, err = cs.placementService.AddInstances([]placement.Instance{newConsumer.instance})
	require.NoError(t, err)
	zap.L().Sugar().Debugf("new placement: %s", p.String())
	cs.testConsumers = append(cs.testConsumers, newConsumer)
}

func (s *setup) RemoveInstance(t *testing.T, idx int) {
	require.True(t, idx < len(s.consumerServices))
	cs := s.consumerServices[idx]

	testConsumers := cs.testConsumers
	require.NotEmpty(t, testConsumers)
	l := len(testConsumers)
	oldConsumer := testConsumers[l-1]
	defer oldConsumer.Close()

	p, err := cs.placementService.Placement()
	require.NoError(t, err)
	zap.L().Sugar().Debugf("old placement: %s", p.String())

	p, err = cs.placementService.RemoveInstances([]string{oldConsumer.instance.ID()})
	require.NoError(t, err)
	zap.L().Sugar().Debugf("new placement: %s", p.String())
	cs.testConsumers = testConsumers[:l-1]
}

func (s *setup) ReplaceInstance(t *testing.T, idx int) {
	require.True(t, idx < len(s.consumerServices))
	cs := s.consumerServices[idx]

	newConsumer := newTestConsumer(t, cs)
	newConsumer.consumeAndAck(s.totalConsumed)

	testConsumers := cs.testConsumers
	require.NotEmpty(t, testConsumers)
	l := len(testConsumers)
	oldConsumer := testConsumers[l-1]
	defer oldConsumer.Close()

	p, err := cs.placementService.Placement()
	require.NoError(t, err)
	zap.L().Sugar().Debugf("old placement: %s", p.String())

	p, _, err = cs.placementService.ReplaceInstances(
		[]string{oldConsumer.instance.ID()},
		[]placement.Instance{newConsumer.instance},
	)
	require.NoError(t, err)
	zap.L().Sugar().Debugf("new placement: %s", p.String())
	cs.testConsumers[l-1] = newConsumer
}

func (s *setup) RemoveConsumerService(t *testing.T, idx int) {
	require.True(t, idx < len(s.consumerServices))
	topic, err := s.ts.Get(topicName)
	require.NoError(t, err)
	css := topic.ConsumerServices()
	topic = topic.SetConsumerServices(append(css[:idx], css[idx+1:]...))
	s.ts.CheckAndSet(topic, topic.Version())
	tcss := s.consumerServices
	tcss[idx].Close()
	s.consumerServices = append(tcss[:idx], tcss[idx+1:]...)
}

func (s *setup) AddConsumerService(t *testing.T, config consumerServiceConfig) {
	cs := newTestConsumerService(t, len(s.consumerServices), config, s.sd, len(s.producers), s.totalConsumed)
	s.consumerServices = append(s.consumerServices, cs)
	topic, err := s.ts.Get(topicName)
	require.NoError(t, err)
	topic = topic.SetConsumerServices(append(topic.ConsumerServices(), cs.consumerService))
	s.ts.CheckAndSet(topic, topic.Version())
}

type testConsumerService struct {
	sync.Mutex

	consumed         map[string]struct{}
	sid              services.ServiceID
	placementService placement.Service
	consumerService  topic.ConsumerService
	testConsumers    []*testConsumer
	config           consumerServiceConfig
}

func (cs *testConsumerService) markConsumed(b []byte) {
	cs.Lock()
	defer cs.Unlock()

	cs.consumed[string(b)] = struct{}{}
}

func (cs *testConsumerService) numConsumed() int {
	cs.Lock()
	defer cs.Unlock()

	return len(cs.consumed)
}

func (cs *testConsumerService) Close() {
	for _, c := range cs.testConsumers {
		c.Close()
	}
}

type testConsumer struct {
	sync.RWMutex

	cs        *testConsumerService
	listener  consumer.Listener
	consumers []consumer.Consumer
	instance  placement.Instance
	consumed  int
	closed    bool
	doneCh    chan struct{}
}

func (c *testConsumer) Close() {
	c.Lock()
	defer c.Unlock()

	if c.closed {
		return
	}
	c.closed = true
	c.listener.Close()
	close(c.doneCh)
}

func (c *testConsumer) numConsumed() int {
	c.Lock()
	defer c.Unlock()

	return c.consumed
}

func newTestConsumer(t *testing.T, cs *testConsumerService) *testConsumer {
	consumerListener, err := consumer.NewListener("127.0.0.1:0", testConsumerOptions(t))
	require.NoError(t, err)

	addr := consumerListener.Addr().String()
	c := &testConsumer{
		cs:       cs,
		listener: consumerListener,
		instance: placement.NewInstance().
			SetID(addr).
			SetEndpoint(addr).
			SetIsolationGroup(addr).
			SetWeight(1),
		consumed: 0,
		closed:   false,
		doneCh:   make(chan struct{}),
	}
	return c
}

func (c *testConsumer) closeOneConsumer() {
	for {
		c.Lock()
		l := len(c.consumers)
		if l == 0 {
			c.Unlock()
			time.Sleep(200 * time.Millisecond)
			continue
		}
		c.consumers[l-1].Close()
		c.consumers = c.consumers[:l-1]
		c.Unlock()
		break
	}
}

func (c *testConsumer) consumeAndAck(totalConsumed *atomic.Int64) {
	wp := xsync.NewWorkerPool(numConcurrentMessages)
	wp.Init()

	go func() {
		for {
			consumer, err := c.listener.Accept()
			if err != nil {
				return
			}
			c.Lock()
			c.consumers = append(c.consumers, consumer)
			c.Unlock()
			go func() {
				for {
					select {
					case <-c.doneCh:
						consumer.Close()
						return
					default:
						msg, err := consumer.Message()
						if err != nil {
							consumer.Close()
							return
						}

						wp.Go(
							func() {
								c.Lock()
								if c.closed {
									c.Unlock()
									return
								}
								c.consumed++
								c.Unlock()
								totalConsumed.Inc()
								c.cs.markConsumed(msg.Bytes())
								msg.Ack()
							},
						)
					}
				}
			}()
		}
	}()
}

func testPlacementService(store kv.Store, sid services.ServiceID, isSharded bool) placement.Service {
	opts := placement.NewOptions().SetShardStateMode(placement.StableShardStateOnly).SetIsSharded(isSharded)

	return service.NewPlacementService(
		storage.NewPlacementStorage(store, sid.String(), opts),
		service.WithPlacementOptions(opts))
}

func testProducer(
	t *testing.T,
	cs client.Client,
) producer.Producer {
	str := `
buffer:
  closeCheckInterval: 200ms
  cleanupRetry:
    initialBackoff: 100ms
    maxBackoff: 200ms
writer:
  topicName: topicName
  topicWatchInitTimeout: 100ms
  placementWatchInitTimeout: 100ms
  # FIXME: Consumers sharing the same pool trigger false-positives in race detector
  messagePool: ~
  messageRetry:
    initialBackoff: 20ms
    maxBackoff: 50ms
  messageQueueNewWritesScanInterval: 10ms
  messageQueueFullScanInterval: 50ms
  closeCheckInterval: 200ms
  ackErrorRetry:
    initialBackoff: 20ms
    maxBackoff: 50ms
  connection:
    dialTimeout: 500ms
    keepAlivePeriod: 2s
    retry:
      initialBackoff: 20ms
      maxBackoff: 50ms
    flushInterval: 50ms
    writeBufferSize: 4096
    resetDelay: 50ms
`

	var cfg config.ProducerConfiguration
	require.NoError(t, yaml.Unmarshal([]byte(str), &cfg))

	p, err := cfg.NewProducer(cs, instrument.NewOptions(), xio.NewOptions())
	require.NoError(t, err)
	return p
}

func testConsumerOptions(t *testing.T) consumer.Options {
	str := `
ackFlushInterval: 100ms
ackBufferSize: 4
connectionWriteBufferSize: 32
`
	var cfg consumer.Configuration
	require.NoError(t, yaml.Unmarshal([]byte(str), &cfg))

	return cfg.NewOptions(instrument.NewOptions(), xio.NewOptions())
}

func serviceID(id int) services.ServiceID {
	return services.NewServiceID().SetName("serviceName" + strconv.Itoa(id))
}
