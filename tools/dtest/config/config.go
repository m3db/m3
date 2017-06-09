package config

import (
	"fmt"
	"time"

	"google.golang.org/grpc"

	etcdclient "github.com/m3db/m3cluster/client/etcd"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
	m3dbnode "github.com/m3db/m3db/x/m3em/node"
	"github.com/m3db/m3em/generated/proto/m3em"
	"github.com/m3db/m3em/node"
	"github.com/m3db/m3x/config"
	xlog "github.com/m3db/m3x/log"
)

var (
	defaultConnectionTimeout = 2 * time.Minute
)

// Configuration is a collection of knobs to control test behavior
type Configuration struct {
	DTest DTestConfig              `yaml:"dtest"`
	M3EM  M3EMConfig               `yaml:"m3em"`
	KV    etcdclient.Configuration `yaml:"kv"`
}

// TODO(prateek): merge DTest/M3EM
// TODO(prateek): make config structs for options in m3em

// M3EMConfig is a list of m3em environment settings
type M3EMConfig struct {
	HeartbeatPort int                 `yaml:"heartbeatPort" validate:"nonzero"`
	AgentPort     int                 `yaml:"agentPort" validate:"nonzero"`
	M3DBPort      int                 `yaml:"m3dbPort" validate:"nonzero"`
	M3DBServiceID string              `yaml:"m3dbServiceID" validate:"nonzero"`
	Instances     []PlacementInstance `yaml:"instances" validate:"min=1"`
	Node          node.Configuration  `yaml:"node"`
}

// DTestConfig is a collection of DTest configs
type DTestConfig struct {
	DebugPort            int `yaml:"debugPort" validate:"nonzero"`
	NumShards            int `yaml:"numShards" validate:"nonzero"`
	BootstrapTimeoutMins int `yaml:"bootstrapTimeoutMins" validate:"nonzero"`
}

// PlacementInstance is a config for a services.PlacementInstance
type PlacementInstance struct {
	ID       string `yaml:"id" validate:"nonzero"`
	Rack     string `yaml:"rack" validate:"nonzero"`
	Zone     string `yaml:"zone" validate:"nonzero"`
	Weight   uint32 `yaml:"weight" validate:"nonzero"`
	Hostname string `yaml:"hostname" validate:"nonzero"`
}

// New constructs a Configuration object from the path specified
func New(m3emConfigPath string) (*Configuration, error) {
	var conf Configuration
	if err := xconfig.LoadFile(&conf, m3emConfigPath); err != nil {
		return nil, err
	}

	return &conf, nil
}

// M3DBNodes returns a slice of m3dbnode.Nodes per the config provided
func (mc *M3EMConfig) M3DBNodes(opts node.Options, numNodes int) ([]m3dbnode.Node, error) {
	// use all nodes if numNodes is zero
	if numNodes <= 0 {
		numNodes = len(mc.Instances)
	}

	var (
		logger  = opts.InstrumentOptions().Logger()
		nodes   = make([]m3dbnode.Node, 0, len(mc.Instances))
		nodeNum = 0
	)

	for _, inst := range mc.Instances {
		if nodeNum >= numNodes {
			break
		}

		pi := inst.newServicesPlacementInstance(mc.M3DBPort)
		newOpts := opts.
			SetOperatorClientFn(inst.operatorClientFn(mc.AgentPort)).
			SetInstrumentOptions(opts.InstrumentOptions().SetLogger(
				logger.WithFields(xlog.NewLogField("host", inst.Hostname))))

		svcNode, err := node.New(pi, newOpts)
		if err != nil {
			return nil, fmt.Errorf("unable to create service node for %+v, error: %v", inst, err)
		}

		m3dbNodeOpts := m3dbnode.NewOptions(newOpts.InstrumentOptions()).SetNodeOptions(newOpts)
		m3dbNode, err := m3dbnode.New(svcNode, m3dbNodeOpts)
		if err != nil {
			return nil, fmt.Errorf("unable to create m3db node for %+v, error: %v", inst, err)
		}
		nodes = append(nodes, m3dbNode)

		nodeNum++
	}

	return nodes, nil
}

func (pi *PlacementInstance) operatorClientFn(agentPort int) node.OperatorClientFn {
	agentEndpoint := fmt.Sprintf("%s:%d", pi.Hostname, agentPort)
	return func() (*grpc.ClientConn, m3em.OperatorClient, error) {
		conn, err := grpc.Dial(agentEndpoint, grpc.WithTimeout(defaultConnectionTimeout), grpc.WithInsecure())
		if err != nil {
			return nil, nil, err
		}
		return conn, m3em.NewOperatorClient(conn), nil
	}
}

func (pi *PlacementInstance) newServicesPlacementInstance(m3dbPort int) services.PlacementInstance {
	endpoint := fmt.Sprintf("%s:%d", pi.Hostname, m3dbPort)
	return placement.NewInstance().
		SetID(pi.ID).
		SetRack(pi.Rack).
		SetZone(pi.Zone).
		SetEndpoint(endpoint).
		SetWeight(pi.Weight)
}
