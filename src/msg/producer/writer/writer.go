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
	"errors"
	"fmt"
	"sync"

	"github.com/uber-go/tally"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/aggregator/aggregator/handler/filter"
	handlerWriter "github.com/m3db/m3/src/aggregator/aggregator/handler/writer"
	"github.com/m3db/m3/src/aggregator/sharding"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/msg/producer"
	"github.com/m3db/m3/src/msg/routing"
	"github.com/m3db/m3/src/msg/topic"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/watch"
)

var (
	errWriterClosed = errors.New("writer is closed")
)

type writerMetrics struct {
	topicUpdateSuccess  tally.Counter
	topicUpdateError    tally.Counter
	invalidTopicUpdate  tally.Counter
	invalidShard        tally.Counter
	numConsumerServices tally.Gauge
}

func newWriterMetrics(scope tally.Scope) writerMetrics {
	return writerMetrics{
		topicUpdateSuccess: scope.Counter("topic-update-success"),
		topicUpdateError:   scope.Counter("topic-update-error"),
		invalidTopicUpdate: scope.Counter("invalid-topic"),
		invalidShard: scope.Tagged(map[string]string{"reason": "invalid-shard"}).
			Counter("invalid-shard-write"),
		numConsumerServices: scope.Gauge("num-consumer-services"),
	}
}

// nolint: maligned
type writer struct {
	sync.RWMutex

	topic                  string
	ts                     topic.Service
	opts                   Options
	logger                 *zap.Logger
	value                  watch.Value
	initType               initType
	numShards              uint32
	consumerServiceWriters map[string]consumerServiceWriter
	filterRegistry         map[string][]producer.FilterFunc
	routingPolicyHandler   routing.PolicyHandler
	isClosed               bool
	m                      writerMetrics

	processFn watch.ProcessFn
}

// NewWriter creates a new writer.
func NewWriter(opts Options) producer.Writer {
	w := &writer{
		topic:                  opts.TopicName(),
		ts:                     opts.TopicService(),
		opts:                   opts,
		logger:                 opts.InstrumentOptions().Logger(),
		initType:               failOnError,
		consumerServiceWriters: make(map[string]consumerServiceWriter),
		filterRegistry:         make(map[string][]producer.FilterFunc),
		isClosed:               false,
		m:                      newWriterMetrics(opts.InstrumentOptions().MetricsScope()),
	}
	w.processFn = w.process
	return w
}

func (w *writer) Write(rm *producer.RefCountedMessage) error {
	w.RLock()
	if w.isClosed {
		rm.Drop()
		w.RUnlock()
		return errWriterClosed
	}
	shard := rm.Shard()
	if shard >= w.numShards {
		w.m.invalidShard.Inc(1)
		rm.Drop()
		w.RUnlock()
		return fmt.Errorf("could not write message for shard %d which is larger than max shard id %d", shard, w.numShards-1)
	}
	// NB(cw): Need to inc ref here in case a consumer service
	// writes the message too fast and close the message.
	rm.IncRef()
	for _, csw := range w.consumerServiceWriters {
		csw.Write(rm)
	}
	rm.DecRef()
	w.RUnlock()
	return nil
}

func (w *writer) Init() error {
	newUpdatableFn := func() (watch.Updatable, error) {
		return w.ts.Watch(w.topic)
	}
	getUpdateFn := func(value watch.Updatable) (interface{}, error) {
		t, err := value.(topic.Watch).Get()
		if err != nil {
			w.m.invalidTopicUpdate.Inc(1)
			return nil, err
		}
		return t, nil
	}
	vOptions := watch.NewOptions().
		SetInitWatchTimeout(w.opts.TopicWatchInitTimeout()).
		SetInstrumentOptions(w.opts.InstrumentOptions()).
		SetNewUpdatableFn(newUpdatableFn).
		SetGetUpdateFn(getUpdateFn).
		SetProcessFn(w.processFn).
		SetKey(w.opts.TopicName())
	w.value = watch.NewValue(vOptions)
	if err := w.value.Watch(); err != nil {
		return fmt.Errorf("writer init error: %v", err)
	}
	return nil
}

func (w *writer) NumShards() uint32 {
	w.RLock()
	n := w.numShards
	w.RUnlock()
	return n
}

func (w *writer) process(update interface{}) error {
	t := update.(topic.Topic)
	if err := t.Validate(); err != nil {
		return err
	}

	// We don't allow changing number of shards for topics, it will be
	// prevented on topic service side, but also being defensive here as well.
	numShards := w.NumShards()
	if numShards != 0 && numShards != t.NumberOfShards() {
		w.m.topicUpdateError.Inc(1)
		return fmt.Errorf("invalid topic update with %d shards, expecting %d", t.NumberOfShards(), numShards)
	}
	var (
		iOpts                     = w.opts.InstrumentOptions()
		newConsumerServiceWriters = make(map[string]consumerServiceWriter, len(t.ConsumerServices()))
		toBeClosed                []consumerServiceWriter
		multiErr                  xerrors.MultiError
	)
	for _, cs := range t.ConsumerServices() {
		key := cs.ServiceID().String()
		csw, ok := w.consumerServiceWriters[key]
		scope := iOpts.MetricsScope().Tagged(map[string]string{
			"consumer-service-name": cs.ServiceID().Name(),
			"consumer-service-zone": cs.ServiceID().Zone(),
			"consumer-service-env":  cs.ServiceID().Environment(),
			"consumption-type":      cs.ConsumptionType().String(),
		})

		if ok {
			// update existing consumer service writer

			csw.SetMessageTTLNanos(cs.MessageTTLNanos())

			if cs.DynamicFilterConfigs() != nil {
				dynamicFilters, err := ParseDynamicFilters(
					w.logger, scope, csw, w.routingPolicyHandler, cs.DynamicFilterConfigs())

				if err != nil {
					w.logger.Error("could not update dynamic filters on consumer service writer, error registering dynamic filters",
						zap.String("writer", cs.String()), zap.Error(err))

					multiErr = multiErr.Add(err)
				} else {
					// atomically set the new dynamic filters
					// and remove the old filters
					csw.SetFilters(dynamicFilters)
				}
			} else {
				// sending no dynamic filters means we should remove all filters,
				// if there are any static filters, we need to re-add them

				w.RLock()
				staticFilters := w.filterRegistry[key]
				w.RUnlock()

				csw.SetFilters(staticFilters)
			}

			newConsumerServiceWriters[key] = csw

			w.logger.Info("Updated consumer service writer", zap.String("consumer-service", cs.String()))

			continue
		}

		// create new consumer service writer
		csw, err := newConsumerServiceWriter(cs, t.NumberOfShards(), w.opts.SetInstrumentOptions(iOpts.SetMetricsScope(scope)))

		if err != nil {
			w.logger.Error("could not create consumer service writer",
				zap.String("writer", cs.String()), zap.Error(err))
			multiErr = multiErr.Add(err)
			continue
		}

		// if there are dynamicly configured filters, they are the source of truth
		if cs.DynamicFilterConfigs() != nil {
			dynamicFilters, err := ParseDynamicFilters(
				w.logger, scope, csw, w.routingPolicyHandler, cs.DynamicFilterConfigs())

			if err != nil {
				w.logger.Error("could not create consumer service writer, error registering dynamic filters",
					zap.String("writer", cs.String()), zap.Error(err))

				multiErr = multiErr.Add(err)
				continue
			} else {
				csw.SetFilters(dynamicFilters)
			}

		} else {
			w.RLock()
			staticFilters := w.filterRegistry[key]
			w.RUnlock()

			// if there are no dynamicly configured filters, static filters are the source of truth
			csw.SetFilters(staticFilters)
		}

		if err = csw.Init(w.initType); err != nil {
			w.logger.Error("could not init consumer service writer",
				zap.String("writer", cs.String()), zap.Error(err))
			multiErr = multiErr.Add(err)
			// Could not initialize the consumer service, simply close it.
			csw.Close()
			continue
		}
		csw.SetMessageTTLNanos(cs.MessageTTLNanos())
		newConsumerServiceWriters[key] = csw
		w.logger.Info("initialized consumer service writer", zap.String("writer", cs.String()))
	}
	for key, csw := range w.consumerServiceWriters {
		if _, ok := newConsumerServiceWriters[key]; !ok {
			toBeClosed = append(toBeClosed, csw)
		}
	}
	// Allow InitValueError for any future topic updates after starting up.
	// This is to handle the case when a new consumer service got added to
	// the topic, but the producer could not get initial value for its
	// placement. We will continue to watch for placement updates for the new
	// consumer service in the background, so the producer can write to it once
	// the placement came in.
	w.initType = allowInitValueError
	w.m.numConsumerServices.Update(float64(len(newConsumerServiceWriters)))

	// Apply the new consumer service writers.
	w.Lock()

	w.consumerServiceWriters = newConsumerServiceWriters
	w.numShards = t.NumberOfShards()
	w.Unlock()

	// Close removed consumer service.
	go func() {
		for _, csw := range toBeClosed {
			csw.Close()
		}
	}()

	if err := multiErr.FinalError(); err != nil {
		w.m.topicUpdateError.Inc(1)
		return err
	}
	w.m.topicUpdateSuccess.Inc(1)
	return nil
}

func (w *writer) Close() {
	w.Lock()
	if w.isClosed {
		w.Unlock()
		return
	}
	w.isClosed = true
	w.Unlock()

	w.value.Unwatch()
	for _, csw := range w.consumerServiceWriters {
		csw.Close()
	}
}

func (w *writer) RegisterFilter(sid services.ServiceID, filter producer.FilterFunc) {
	w.Lock()
	defer w.Unlock()

	key := sid.String()
	w.filterRegistry[key] = append(w.filterRegistry[key], filter)

	csw, ok := w.consumerServiceWriters[key]
	if ok {
		csw.RegisterFilter(filter)
	}
}

func (w *writer) UnregisterFilters(sid services.ServiceID) {
	w.Lock()
	defer w.Unlock()

	key := sid.String()
	delete(w.filterRegistry, key)
	csw, ok := w.consumerServiceWriters[key]
	if ok {
		csw.UnregisterFilters()
	}
}

func (w *writer) SetRoutingPolicyHandler(policyHandler routing.PolicyHandler) {
	w.Lock()
	defer w.Unlock()
	w.routingPolicyHandler = policyHandler
}

// ParseDynamicFilters parses the dynamic filters for a consumer service from a topic update.
func ParseDynamicFilters(
	logger *zap.Logger,
	scope tally.Scope,
	csw consumerServiceWriter,
	rph routing.PolicyHandler,
	filterConfig topic.FilterConfig,
) ([]producer.FilterFunc, error) {
	filterFuncs := []producer.FilterFunc{}

	if filterConfig == nil {
		return filterFuncs, errors.New("nil filter config")
	}

	if filterConfig.ShardSetFilter() != nil {
		shardSetFilterFunc, err := ParseShardSetFilterFromTopicUpdate(csw, filterConfig.ShardSetFilter())

		if err != nil {
			return filterFuncs, fmt.Errorf("Error registering shard set filter: %w", err)
		}

		filterFuncs = append(filterFuncs, shardSetFilterFunc)
	}

	if filterConfig.StoragePolicyFilter() != nil {
		storagePolicyFilterFunc, err := ParseStoragePolicyFilterFromTopicUpdate(csw, filterConfig.StoragePolicyFilter())

		if err != nil {
			return filterFuncs, fmt.Errorf("Error registering storage policy filter: %w", err)
		}

		filterFuncs = append(filterFuncs, storagePolicyFilterFunc)
	}

	if filterConfig.PercentageFilter() != nil {
		percentageFilterFunc, err := ParsePercentageFilterFromFromTopicUpdate(csw, filterConfig.PercentageFilter())

		if err != nil {
			return filterFuncs, fmt.Errorf("Error registering percentage filter: %w", err)
		}

		filterFuncs = append(filterFuncs, percentageFilterFunc)
	}

	if filterConfig.RoutingPolicyFilter() != nil {
		if rph == nil {
			return filterFuncs, errors.New("routing policy handler is not set, but routing policy filter is configured")
		}
		routingPolicyFilterFunc, err := ParseRoutingPolicyFilterFromFromTopicUpdate(
			logger, scope, rph, filterConfig.RoutingPolicyFilter())

		if err != nil {
			return filterFuncs, fmt.Errorf("Error registering routing policy filter: %w", err)
		}

		filterFuncs = append(filterFuncs, routingPolicyFilterFunc)
	}

	return filterFuncs, nil
}

// ParseShardSetFilterFromTopicUpdate parses a shard set filter from a topic update.
func ParseShardSetFilterFromTopicUpdate(
	csw consumerServiceWriter,
	ssf topic.ShardSetFilter) (producer.FilterFunc, error) {
	var filterFunc producer.FilterFunc

	shardSetString := ssf.ShardSet()

	shardSet, err := sharding.ParseShardSet(shardSetString)

	if err != nil {
		return filterFunc, errors.New("Error parsing shard set")
	}

	filterFunc = filter.NewShardSetFilter(shardSet, producer.DynamicConfig)

	return filterFunc, nil
}

// ParseStoragePolicyFilterFromTopicUpdate parses a storage policy filter from a topic update.
func ParseStoragePolicyFilterFromTopicUpdate(
	csw consumerServiceWriter,
	spf topic.StoragePolicyFilter) (producer.FilterFunc, error) {
	var filterFunc producer.FilterFunc

	storagePolicies := spf.StoragePolicies()

	parsedPolicies := []policy.StoragePolicy{}
	for _, storagePolicyString := range storagePolicies {
		parsedPolicy, err := policy.ParseStoragePolicy(storagePolicyString)

		if err != nil {
			return filterFunc, fmt.Errorf("Error parsing storage policy: %w", err)
		}

		parsedPolicies = append(parsedPolicies, parsedPolicy)

		filterFunc = handlerWriter.NewStoragePolicyFilter(parsedPolicies, producer.DynamicConfig)
	}

	return filterFunc, nil
}

// ParsePercentageFilterFromFromTopicUpdate parses a percentage filter from a topic update.
func ParsePercentageFilterFromFromTopicUpdate(
	csw consumerServiceWriter,
	pf topic.PercentageFilter) (producer.FilterFunc, error) {
	var filterFunc producer.FilterFunc

	percentage := pf.Percentage()

	filterFunc = filter.NewPercentageFilter(percentage, producer.DynamicConfig)

	return filterFunc, nil
}

// ParseRoutingPolicyFilterFromFromTopicUpdate parses a routing policy filter from a topic update.
func ParseRoutingPolicyFilterFromFromTopicUpdate(
	logger *zap.Logger,
	scope tally.Scope,
	rph routing.PolicyHandler,
	rpf topic.RoutingPolicyFilter) (producer.FilterFunc, error) {
	params := handlerWriter.RoutingPolicyFilterParams{
		Scope:                scope,
		Logger:               logger,
		RoutingPolicyHandler: rph,
		IsDefault:            rpf.IsDefault(),
		AllowedTrafficTypes:  rpf.AllowedTrafficTypes(),
	}
	filterFunc := handlerWriter.NewRoutingPolicyFilter(params, producer.DynamicConfig)
	return filterFunc, nil
}
