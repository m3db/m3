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
	"fmt"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3msg/generated/proto/topicpb"
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
	sid services.ServiceID
	ct  ConsumptionType
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
		SetConsumptionType(ct), nil
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

func (cs *consumerService) String() string {
	return fmt.Sprintf("{service: %s, consumption type: %s}", cs.sid.String(), cs.ct.String())
}

// NewConsumptionTypeFromProto creates ConsumptionType from a proto.
func NewConsumptionTypeFromProto(ct topicpb.ConsumptionType) (ConsumptionType, error) {
	switch ct {
	case topicpb.ConsumptionType_SHARED:
		return Shared, nil
	case topicpb.ConsumptionType_REPLICATED:
		return Replicated, nil
	}
	return Unknown, fmt.Errorf("invalid consumption type in protobuf: %v", ct)
}

// ConsumptionTypeToProto creates proto from a ConsumptionType.
func ConsumptionTypeToProto(ct ConsumptionType) (topicpb.ConsumptionType, error) {
	switch ct {
	case Shared:
		return topicpb.ConsumptionType_SHARED, nil
	case Replicated:
		return topicpb.ConsumptionType_REPLICATED, nil
	}
	return topicpb.ConsumptionType_UNKNOWN, fmt.Errorf("invalid consumption type: %v", ct)
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
