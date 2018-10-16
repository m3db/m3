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
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/msg/generated/proto/topicpb"
)

var (
	errEmptyName  = errors.New("invalid topic: empty name")
	errZeroShards = errors.New("invalid topic: zero shards")
)

type topic struct {
	name             string
	numOfShards      uint32
	consumerServices []ConsumerService
	version          int
}

// NewTopic creates a new topic.
func NewTopic() Topic { return new(topic) }

// NewTopicFromValue creates a topic from a kv.Value.
func NewTopicFromValue(v kv.Value) (Topic, error) {
	var topic topicpb.Topic
	if err := v.Unmarshal(&topic); err != nil {
		return nil, err
	}
	t, err := NewTopicFromProto(&topic)
	if err != nil {
		return nil, err
	}
	return t.SetVersion(v.Version()), nil
}

// NewTopicFromProto creates a topic from a proto.
func NewTopicFromProto(t *topicpb.Topic) (Topic, error) {
	css := make([]ConsumerService, len(t.ConsumerServices))
	for i, cspb := range t.ConsumerServices {
		cs, err := NewConsumerServiceFromProto(cspb)
		if err != nil {
			return nil, err
		}
		css[i] = cs
	}
	return NewTopic().
		SetName(t.Name).
		SetNumberOfShards(t.NumberOfShards).
		SetConsumerServices(css), nil
}

func (t *topic) Name() string {
	return t.name
}

func (t *topic) SetName(value string) Topic {
	newt := *t
	newt.name = value
	return &newt
}

func (t *topic) NumberOfShards() uint32 {
	return t.numOfShards
}

func (t *topic) SetNumberOfShards(value uint32) Topic {
	newt := *t
	newt.numOfShards = value
	return &newt
}

func (t *topic) ConsumerServices() []ConsumerService {
	return t.consumerServices
}

func (t *topic) SetConsumerServices(value []ConsumerService) Topic {
	newt := *t
	newt.consumerServices = value
	return &newt
}

func (t *topic) Version() int {
	return t.version
}

func (t *topic) SetVersion(value int) Topic {
	newt := *t
	newt.version = value
	return &newt
}

func (t *topic) AddConsumerService(value ConsumerService) (Topic, error) {
	cur := t.ConsumerServices()
	for _, cs := range cur {
		if cs.ServiceID().Equal(value.ServiceID()) {
			return nil, fmt.Errorf("service %s is already consuming the topic", value.ServiceID().String())
		}
	}

	return t.SetConsumerServices(append(cur, value)), nil
}

func (t *topic) RemoveConsumerService(value services.ServiceID) (Topic, error) {
	cur := t.ConsumerServices()
	for i, cs := range cur {
		if cs.ServiceID().Equal(value) {
			cur = append(cur[:i], cur[i+1:]...)
			return t.SetConsumerServices(cur), nil
		}
	}

	return nil, fmt.Errorf("could not find consumer service %s in the topic", value.String())
}

func (t *topic) UpdateConsumerService(value ConsumerService) (Topic, error) {
	css := t.ConsumerServices()
	for i, cs := range css {
		if !cs.ServiceID().Equal(value.ServiceID()) {
			continue
		}
		if value.ConsumptionType() != cs.ConsumptionType() {
			return nil, fmt.Errorf("could not change consumption type for consumer service %s", value.ServiceID().String())
		}
		css[i] = value
		return t.SetConsumerServices(css), nil
	}
	return nil, fmt.Errorf("could not find consumer service %s in the topic", value.String())
}

func (t *topic) String() string {
	var buf bytes.Buffer
	buf.WriteString("\n{\n")
	buf.WriteString(fmt.Sprintf("\tversion: %d\n", t.version))
	buf.WriteString(fmt.Sprintf("\tname: %s\n", t.name))
	buf.WriteString(fmt.Sprintf("\tnumOfShards: %d\n", t.numOfShards))
	if len(t.consumerServices) > 0 {
		buf.WriteString("\tconsumerServices: {\n")
	}
	for _, cs := range t.consumerServices {
		buf.WriteString(fmt.Sprintf("\t\t%s\n", cs.String()))
	}
	if len(t.consumerServices) > 0 {
		buf.WriteString("\t}\n")
	}
	buf.WriteString("}\n")
	return buf.String()
}

func (t *topic) Validate() error {
	if t.Name() == "" {
		return errEmptyName
	}
	if t.NumberOfShards() == 0 {
		return errZeroShards
	}
	uniqConsumers := make(map[string]struct{}, len(t.ConsumerServices()))
	for _, cs := range t.ConsumerServices() {
		_, ok := uniqConsumers[cs.ServiceID().String()]
		if ok {
			return fmt.Errorf("invalid topic: duplicated consumer %s", cs.ServiceID().String())
		}
		uniqConsumers[cs.ServiceID().String()] = struct{}{}
	}
	return nil
}

// ToProto creates proto from a topic.
func ToProto(t Topic) (*topicpb.Topic, error) {
	css := t.ConsumerServices()
	csspb := make([]*topicpb.ConsumerService, len(css))
	for i, cs := range css {
		cspb, err := ConsumerServiceToProto(cs)
		if err != nil {
			return nil, err
		}
		csspb[i] = cspb
	}
	return &topicpb.Topic{
		Name:             t.Name(),
		NumberOfShards:   t.NumberOfShards(),
		ConsumerServices: csspb,
	}, nil
}

type consumerService struct {
	sid      services.ServiceID
	ct       ConsumptionType
	ttlNanos int64
}

// NewConsumerService creates a ConsumerService.
func NewConsumerService() ConsumerService {
	return new(consumerService)
}

// NewConsumerServiceFromProto creates a ConsumerService from a proto.
func NewConsumerServiceFromProto(cs *topicpb.ConsumerService) (ConsumerService, error) {
	ct, err := NewConsumptionTypeFromProto(cs.ConsumptionType)
	if err != nil {
		return nil, err
	}
	return NewConsumerService().
		SetServiceID(NewServiceIDFromProto(cs.ServiceId)).
		SetConsumptionType(ct).
		SetMessageTTLNanos(cs.MessageTtlNanos), nil
}

// ConsumerServiceToProto creates proto from a ConsumerService.
func ConsumerServiceToProto(cs ConsumerService) (*topicpb.ConsumerService, error) {
	ct, err := ConsumptionTypeToProto(cs.ConsumptionType())
	if err != nil {
		return nil, err
	}
	return &topicpb.ConsumerService{
		ConsumptionType: ct,
		ServiceId:       ServiceIDToProto(cs.ServiceID()),
		MessageTtlNanos: cs.MessageTTLNanos(),
	}, nil
}

func (cs *consumerService) ServiceID() services.ServiceID {
	return cs.sid
}

func (cs *consumerService) SetServiceID(value services.ServiceID) ConsumerService {
	newcs := *cs
	newcs.sid = value
	return &newcs
}

func (cs *consumerService) ConsumptionType() ConsumptionType {
	return cs.ct
}

func (cs *consumerService) SetConsumptionType(value ConsumptionType) ConsumerService {
	newcs := *cs
	newcs.ct = value
	return &newcs
}

func (cs *consumerService) MessageTTLNanos() int64 {
	return cs.ttlNanos
}

func (cs *consumerService) SetMessageTTLNanos(value int64) ConsumerService {
	newcs := *cs
	newcs.ttlNanos = value
	return &newcs
}

func (cs *consumerService) String() string {
	var buf bytes.Buffer
	buf.WriteString("{")
	buf.WriteString(fmt.Sprintf("service: %s, consumption type: %s", cs.sid.String(), cs.ct.String()))
	if cs.ttlNanos != 0 {
		buf.WriteString(fmt.Sprintf(", ttl: %v", time.Duration(cs.ttlNanos)))
	}
	buf.WriteString("}")
	return buf.String()
}

// NewServiceIDFromProto creates service id from a proto.
func NewServiceIDFromProto(sid *topicpb.ServiceID) services.ServiceID {
	return services.NewServiceID().SetName(sid.Name).SetEnvironment(sid.Environment).SetZone(sid.Zone)
}

// ServiceIDToProto creates proto from a service id.
func ServiceIDToProto(sid services.ServiceID) *topicpb.ServiceID {
	return &topicpb.ServiceID{
		Name:        sid.Name(),
		Environment: sid.Environment(),
		Zone:        sid.Zone(),
	}
}
