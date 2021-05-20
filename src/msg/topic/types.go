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

package topic

import (
	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/services"
)

// Topic defines the topic of messages.
type Topic interface {
	// Name returns the name of the topic.
	Name() string

	// SetName sets the name of the topic.
	SetName(value string) Topic

	// NumberOfShards returns the total number of shards of the topic.
	NumberOfShards() uint32

	// SetNumberOfShards sets the total number of shards of the topic.
	SetNumberOfShards(value uint32) Topic

	// ConsumerServices returns the consumers of the topic.
	ConsumerServices() []ConsumerService

	// SetConsumerServices sets the consumers of the topic.
	SetConsumerServices(value []ConsumerService) Topic

	// Version returns the version of the topic.
	Version() int

	// SetVersion sets the version of the topic.
	SetVersion(value int) Topic

	// AddConsumerService adds a consumer to the topic.
	AddConsumerService(value ConsumerService) (Topic, error)

	// RemoveConsumerService removes a consumer from the topic.
	RemoveConsumerService(value services.ServiceID) (Topic, error)

	// UpdateConsumerService updates a consumer in the topic.
	UpdateConsumerService(value ConsumerService) (Topic, error)

	// String returns the string representation of the topic.
	String() string

	// Validate validates the topic.
	Validate() error
}

// ConsumerService is a service that consumes the messages in a topic.
type ConsumerService interface {
	// ServiceID returns the service id of the consumer service.
	ServiceID() services.ServiceID

	// SetServiceID sets the service id of the consumer service.
	SetServiceID(value services.ServiceID) ConsumerService

	// ConsumptionType returns the consumption type of the consumer service.
	ConsumptionType() ConsumptionType

	// SetConsumptionType sets the consumption type of the consumer service.
	SetConsumptionType(value ConsumptionType) ConsumerService

	// MessageTTLNanos returns ttl for each message in nanoseconds.
	MessageTTLNanos() int64

	// SetMessageTTLNanos sets ttl for each message in nanoseconds.
	SetMessageTTLNanos(value int64) ConsumerService

	// String returns the string representation of the consumer service.
	String() string
}

// Watch watches the updates of a topic.
type Watch interface {
	// C returns the notification channel.
	C() <-chan struct{}

	// Get returns the latest version of the topic.
	Get() (Topic, error)

	// Close stops watching for topic updates.
	Close()
}

// Service provides accessibility to topics.
type Service interface {
	// Get returns the topic and version for the given name.
	Get(name string) (Topic, error)

	// CheckAndSet sets the topic for the name if the version matches.
	CheckAndSet(t Topic, version int) (Topic, error)

	// Delete deletes the topic with the name.
	Delete(name string) error

	// Watch returns a topic watch.
	Watch(name string) (Watch, error)
}

// ServiceOptions configures the topic service.
type ServiceOptions interface {
	// ConfigService returns the client of config service.
	ConfigService() client.Client

	// SetConfigService sets the client of config service.
	SetConfigService(c client.Client) ServiceOptions

	// KVOverrideOptions returns the override options for KV store.
	KVOverrideOptions() kv.OverrideOptions

	// SetKVOverrideOptions sets the override options for KV store.
	SetKVOverrideOptions(value kv.OverrideOptions) ServiceOptions
}

// ConsumptionType defines how the consumer consumes messages.
type ConsumptionType string

const (
	// Unknown is the unknown consumption type.
	Unknown ConsumptionType = "unknown"

	// Shared means the messages for each shard will be
	// shared by all the responsible instances.
	Shared ConsumptionType = "shared"

	// Replicated means the messages for each shard will be
	// replicated to all the responsible instances.
	Replicated ConsumptionType = "replicated"
)
