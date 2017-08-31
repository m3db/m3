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

package services

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/m3db/m3cluster/generated/proto/metadatapb"
	"github.com/m3db/m3cluster/generated/proto/placementpb"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/shard"
)

const (
	defaultLeaderTimeout = 10 * time.Second
	defaultResignTimeout = 10 * time.Second
)

var (
	errInstanceNotFound          = errors.New("instance not found")
	errNilPlacementProto         = errors.New("nil placement proto")
	errNilPlacementInstanceProto = errors.New("nil placement instance proto")
	errNilMetadataProto          = errors.New("nil metadata proto")
)

// NewService creates a new Service
func NewService() Service { return new(service) }

// NewServiceFromProto takes the data from a placement and a service id and
// returns the corresponding Service object.
func NewServiceFromProto(
	p *placementpb.Placement,
	sid ServiceID,
) (Service, error) {
	if p == nil {
		return nil, errNilPlacementProto
	}
	r := make([]ServiceInstance, 0, len(p.Instances))
	for _, instance := range p.Instances {
		instance, err := NewServiceInstanceFromProto(instance, sid)
		if err != nil {
			return nil, err
		}
		r = append(r, instance)
	}

	return NewService().
		SetReplication(NewServiceReplication().SetReplicas(int(p.ReplicaFactor))).
		SetSharding(NewServiceSharding().SetNumShards(int(p.NumShards)).SetIsSharded(p.IsSharded)).
		SetInstances(r), nil
}

// NewServiceFromPlacement creates a Service from the placement and service ID.
func NewServiceFromPlacement(p placement.Placement, sid ServiceID) Service {
	var (
		placementInstances = p.Instances()
		serviceInstances   = make([]ServiceInstance, len(placementInstances))
	)

	for i, placementInstance := range placementInstances {
		serviceInstances[i] = NewServiceInstanceFromPlacementInstance(placementInstance, sid)
	}

	return NewService().
		SetReplication(NewServiceReplication().SetReplicas(p.ReplicaFactor())).
		SetSharding(NewServiceSharding().SetNumShards(p.NumShards()).SetIsSharded(p.IsSharded())).
		SetInstances(serviceInstances)
}

type service struct {
	instances   []ServiceInstance
	replication ServiceReplication
	sharding    ServiceSharding
}

func (s *service) Instance(instanceID string) (ServiceInstance, error) {
	for _, instance := range s.instances {
		if instance.InstanceID() == instanceID {
			return instance, nil
		}
	}
	return nil, errInstanceNotFound
}
func (s *service) Instances() []ServiceInstance                 { return s.instances }
func (s *service) Replication() ServiceReplication              { return s.replication }
func (s *service) Sharding() ServiceSharding                    { return s.sharding }
func (s *service) SetInstances(insts []ServiceInstance) Service { s.instances = insts; return s }
func (s *service) SetReplication(r ServiceReplication) Service  { s.replication = r; return s }
func (s *service) SetSharding(ss ServiceSharding) Service       { s.sharding = ss; return s }

// NewServiceReplication creates a new ServiceReplication
func NewServiceReplication() ServiceReplication { return new(serviceReplication) }

type serviceReplication struct {
	replicas int
}

func (r *serviceReplication) Replicas() int                          { return r.replicas }
func (r *serviceReplication) SetReplicas(rep int) ServiceReplication { r.replicas = rep; return r }

// NewServiceSharding creates a new ServiceSharding
func NewServiceSharding() ServiceSharding { return new(serviceSharding) }

type serviceSharding struct {
	isSharded bool
	numShards int
}

func (s *serviceSharding) NumShards() int                      { return s.numShards }
func (s *serviceSharding) IsSharded() bool                     { return s.isSharded }
func (s *serviceSharding) SetNumShards(n int) ServiceSharding  { s.numShards = n; return s }
func (s *serviceSharding) SetIsSharded(v bool) ServiceSharding { s.isSharded = v; return s }

// NewServiceInstance creates a new ServiceInstance
func NewServiceInstance() ServiceInstance { return new(serviceInstance) }

// NewServiceInstanceFromProto creates a new service instance from proto.
func NewServiceInstanceFromProto(
	instance *placementpb.Instance,
	sid ServiceID,
) (ServiceInstance, error) {
	if instance == nil {
		return nil, errNilPlacementInstanceProto
	}
	shards, err := shard.NewShardsFromProto(instance.Shards)
	if err != nil {
		return nil, err
	}
	return NewServiceInstance().
		SetServiceID(sid).
		SetInstanceID(instance.Id).
		SetEndpoint(instance.Endpoint).
		SetShards(shards), nil
}

// NewServiceInstanceFromPlacementInstance creates a new service instance from placement instance.
func NewServiceInstanceFromPlacementInstance(
	instance placement.Instance,
	sid ServiceID,
) ServiceInstance {
	return NewServiceInstance().
		SetServiceID(sid).
		SetInstanceID(instance.ID()).
		SetEndpoint(instance.Endpoint()).
		SetShards(instance.Shards())
}

type serviceInstance struct {
	service  ServiceID
	id       string
	endpoint string
	shards   shard.Shards
}

func (i *serviceInstance) InstanceID() string                       { return i.id }
func (i *serviceInstance) Endpoint() string                         { return i.endpoint }
func (i *serviceInstance) Shards() shard.Shards                     { return i.shards }
func (i *serviceInstance) ServiceID() ServiceID                     { return i.service }
func (i *serviceInstance) SetInstanceID(id string) ServiceInstance  { i.id = id; return i }
func (i *serviceInstance) SetEndpoint(e string) ServiceInstance     { i.endpoint = e; return i }
func (i *serviceInstance) SetShards(s shard.Shards) ServiceInstance { i.shards = s; return i }

func (i *serviceInstance) SetServiceID(service ServiceID) ServiceInstance {
	i.service = service
	return i
}

// NewAdvertisement creates a new Advertisement
func NewAdvertisement() Advertisement { return new(advertisement) }

type advertisement struct {
	instance placement.Instance
	service  ServiceID
	health   func() error
}

func (a *advertisement) ServiceID() ServiceID                   { return a.service }
func (a *advertisement) Health() func() error                   { return a.health }
func (a *advertisement) PlacementInstance() placement.Instance  { return a.instance }
func (a *advertisement) SetServiceID(s ServiceID) Advertisement { a.service = s; return a }
func (a *advertisement) SetHealth(h func() error) Advertisement { a.health = h; return a }
func (a *advertisement) SetPlacementInstance(p placement.Instance) Advertisement {
	a.instance = p
	return a
}

// NewServiceID creates new ServiceID
func NewServiceID() ServiceID { return new(serviceID) }

type serviceID struct {
	name string
	env  string
	zone string
}

func (sid *serviceID) Name() string                      { return sid.name }
func (sid *serviceID) Environment() string               { return sid.env }
func (sid *serviceID) Zone() string                      { return sid.zone }
func (sid *serviceID) SetName(n string) ServiceID        { sid.name = n; return sid }
func (sid *serviceID) SetEnvironment(e string) ServiceID { sid.env = e; return sid }
func (sid *serviceID) SetZone(z string) ServiceID        { sid.zone = z; return sid }
func (sid *serviceID) String() string {
	return fmt.Sprintf("[name: %s, env: %s, zone: %s]", sid.name, sid.env, sid.zone)
}

// NewQueryOptions creates new QueryOptions
func NewQueryOptions() QueryOptions { return new(queryOptions) }

type queryOptions struct {
	includeUnhealthy bool
}

func (qo *queryOptions) IncludeUnhealthy() bool                  { return qo.includeUnhealthy }
func (qo *queryOptions) SetIncludeUnhealthy(h bool) QueryOptions { qo.includeUnhealthy = h; return qo }

// NewMetadata creates new Metadata
func NewMetadata() Metadata { return new(metadata) }

// NewMetadataFromProto converts a Metadata proto message to an instance of
// Metadata.
func NewMetadataFromProto(m *metadatapb.Metadata) (Metadata, error) {
	if m == nil {
		return nil, errNilMetadataProto
	}
	return NewMetadata().
		SetPort(m.Port).
		SetLivenessInterval(time.Duration(m.LivenessInterval)).
		SetHeartbeatInterval(time.Duration(m.HeartbeatInterval)), nil
}

type metadata struct {
	port              uint32
	livenessInterval  time.Duration
	heartbeatInterval time.Duration
}

func (m *metadata) Port() uint32                     { return m.port }
func (m *metadata) LivenessInterval() time.Duration  { return m.livenessInterval }
func (m *metadata) HeartbeatInterval() time.Duration { return m.heartbeatInterval }
func (m *metadata) SetPort(p uint32) Metadata        { m.port = p; return m }

func (m *metadata) SetLivenessInterval(l time.Duration) Metadata {
	m.livenessInterval = l
	return m
}

func (m *metadata) SetHeartbeatInterval(l time.Duration) Metadata {
	m.heartbeatInterval = l
	return m
}

func (m *metadata) String() string {
	return fmt.Sprintf("[port: %d, livenessInterval: %v, heartbeatInterval: %v]",
		m.port,
		m.livenessInterval,
		m.heartbeatInterval,
	)
}

func (m *metadata) Proto() (*metadatapb.Metadata, error) {
	return &metadatapb.Metadata{
		Port:              m.Port(),
		LivenessInterval:  int64(m.LivenessInterval()),
		HeartbeatInterval: int64(m.HeartbeatInterval()),
	}, nil
}

// NewElectionOptions returns an empty ElectionOptions.
func NewElectionOptions() ElectionOptions {
	eo := electionOpts{
		leaderTimeout: defaultLeaderTimeout,
		resignTimeout: defaultResignTimeout,
	}

	return eo
}

type electionOpts struct {
	leaderTimeout time.Duration
	resignTimeout time.Duration
	ttlSecs       int
}

func (e electionOpts) LeaderTimeout() time.Duration {
	return e.leaderTimeout
}

func (e electionOpts) SetLeaderTimeout(t time.Duration) ElectionOptions {
	e.leaderTimeout = t
	return e
}

func (e electionOpts) ResignTimeout() time.Duration {
	return e.resignTimeout
}

func (e electionOpts) SetResignTimeout(t time.Duration) ElectionOptions {
	e.resignTimeout = t
	return e
}

func (e electionOpts) TTLSecs() int {
	return e.ttlSecs
}

func (e electionOpts) SetTTLSecs(ttl int) ElectionOptions {
	e.ttlSecs = ttl
	return e
}

type campaignOpts struct {
	val string
}

// NewCampaignOptions returns an empty CampaignOptions.
func NewCampaignOptions() (CampaignOptions, error) {
	h, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	return campaignOpts{val: h}, nil
}

func (c campaignOpts) LeaderValue() string {
	return c.val
}

func (c campaignOpts) SetLeaderValue(v string) CampaignOptions {
	c.val = v
	return c
}

type options struct {
	namespaceOpts NamespaceOptions
}

// NewOptions constructs a new Options.
func NewOptions() Options {
	return &options{
		namespaceOpts: NewNamespaceOptions(),
	}
}

func (o options) NamespaceOptions() NamespaceOptions {
	return o.namespaceOpts
}

func (o options) SetNamespaceOptions(opts NamespaceOptions) Options {
	o.namespaceOpts = opts
	return o
}

type namespaceOpts struct {
	placement string
	metadata  string
}

// NewNamespaceOptions constructs a new NamespaceOptions.
func NewNamespaceOptions() NamespaceOptions {
	return &namespaceOpts{}
}

func (opts namespaceOpts) PlacementNamespace() string {
	return opts.placement
}

func (opts namespaceOpts) SetPlacementNamespace(v string) NamespaceOptions {
	opts.placement = v
	return opts
}

func (opts namespaceOpts) MetadataNamespace() string {
	return opts.metadata
}

func (opts namespaceOpts) SetMetadataNamespace(v string) NamespaceOptions {
	opts.metadata = v
	return opts
}
