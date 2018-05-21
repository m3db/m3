package topology

import (
	"fmt"
	"net"
	"strconv"

	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/services"
)

// NodeWeightHeuristic enumerates methods of calculating a node's weight in a
// placement.
type NodeWeightHeuristic int

const (
	// Static indicates the weight will be statically defined in config.
	Static NodeWeightHeuristic = iota + 1
)

// BootstrapPlacementConfig configures options for a seed node to bootstrap its
// own placement. If enabled, the initial placement will only contain the nodes
// specified in the seed node list. Further placement customizations must be
// done using the coordinator or some other mechanism.
type BootstrapPlacementConfig struct {
	Enabled        bool   `yaml:"enabled"`
	ReplicaFactor  int    `yaml:"replicaFactor"`
	NumShards      int    `yaml:"numShards"`
	IsSharded      bool   `yaml:"isSharded"`
	NodeWeight     uint32 `yaml:"nodeWeight"`
	IsolationGroup string `yaml:"isolationGroup"`
}

// BootstrapInstance describes an instance in the placement.
type BootstrapInstance struct {
	// ID is a user-friendly ID for the instance.
	ID string
	// Address is an address reachable on the network (can be an IP or
	// hostname).
	Address string
}

// BootstrapConfigOption configures dynamically generated (not statically
// defined in YAML) bootstrap placement options.
type BootstrapConfigOption func(*bootstrapOptions)

// WithInstances accepts a list of hostnames that will be part of the
// bootstrapped placement. This should be strictly equal to the list of seed
// hostnames. Each instance in the generated placement will have an ID equal to
// its hostname, an endpoint of <hostname>:<port>.
func WithInstances(instances []BootstrapInstance) BootstrapConfigOption {
	return func(o *bootstrapOptions) {
		o.instances = instances
	}
}

// WithPort defines the cluster listen port.
func WithPort(port uint32) BootstrapConfigOption {
	return func(o *bootstrapOptions) {
		o.port = port
	}
}

// WithClient sets the m3cluster client.
func WithClient(cl client.Client) BootstrapConfigOption {
	return func(o *bootstrapOptions) {
		o.client = cl
	}
}

// WithServiceID sets the service ID to bootstrap the placement for.
func WithServiceID(sid services.ServiceID) BootstrapConfigOption {
	return func(o *bootstrapOptions) {
		o.serviceID = sid
	}
}

// WithBootstrapConfig sets the bootstrap configuration.
func WithBootstrapConfig(bc BootstrapPlacementConfig) BootstrapConfigOption {
	return func(o *bootstrapOptions) {
		o.bootstrapConfig = bc
	}
}

type bootstrapOptions struct {
	client          client.Client
	serviceID       services.ServiceID
	bootstrapConfig BootstrapPlacementConfig
	instances       []BootstrapInstance
	port            uint32
}

func (o *bootstrapOptions) getPlacementSvc() (placement.Service, error) {
	// TODO(schallert): figure out namespace overrides here
	svcs, err := o.client.Services(services.NewOverrideOptions())
	if err != nil {
		return nil, fmt.Errorf("services init err: %v", err)
	}

	po := placement.NewOptions().SetValidZone(o.serviceID.Zone())
	return svcs.PlacementService(o.serviceID, po)
}

func (o *bootstrapOptions) buildInstances() []placement.Instance {
	insts := make([]placement.Instance, len(o.instances))
	for i, bo := range o.instances {
		ep := net.JoinHostPort(bo.Address, strconv.Itoa(int(o.port)))
		insts[i] = placement.NewInstance().
			SetEndpoint(ep).
			SetPort(o.port).
			SetZone(o.serviceID.Zone()).
			SetID(bo.ID).
			SetHostname(bo.ID).
			SetIsolationGroup(o.bootstrapConfig.IsolationGroup).
			SetWeight(o.bootstrapConfig.NodeWeight)
	}

	return insts
}

// BootstrapPlacement attempts to create an initial placement per the defined
// config.
func BootstrapPlacement(bOpts ...BootstrapConfigOption) error {
	opts := bootstrapOptions{}
	for _, f := range bOpts {
		f(&opts)
	}

	psvc, err := opts.getPlacementSvc()
	if err != nil {
		return fmt.Errorf("placement svc init err: %v", err)
	}

	insts := opts.buildInstances()

	// TODO(mschalle): catch the case where another seed node beat us to the
	// atomic swap and don't fatal error
	_, err = psvc.BuildInitialPlacement(insts,
		opts.bootstrapConfig.NumShards,
		opts.bootstrapConfig.ReplicaFactor)

	return err
}
