package config

import (
	"fmt"
	"io/ioutil"
	"time"

	etcdclient "github.com/m3db/m3cluster/client/etcd"
	"github.com/m3db/m3cluster/placement"
	m3emnode "github.com/m3db/m3db/src/dbnode/x/m3em/node"
	"github.com/m3db/m3em/cluster"
	"github.com/m3db/m3em/generated/proto/m3em"
	"github.com/m3db/m3em/node"
	"github.com/m3db/m3em/x/grpc"
	xconfig "github.com/m3db/m3x/config"
	xlog "github.com/m3db/m3x/log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Configuration is a collection of knobs to control test behavior
type Configuration struct {
	DTest DTestConfig              `yaml:"dtest"`
	M3EM  M3EMConfig               `yaml:"m3em"`
	KV    etcdclient.Configuration `yaml:"kv"`
}

// DTestConfig is a collection of DTest configs
type DTestConfig struct {
	DebugPort               int                 `yaml:"debugPort" validate:"nonzero"`
	BootstrapTimeout        time.Duration       `yaml:"bootstrapTimeout" validate:"nonzero"`
	BootstrapReportInterval time.Duration       `yaml:"bootstrapReportInterval" validate:"nonzero"`
	NodePort                int                 `yaml:"nodePort" validate:"nonzero"`
	ServiceID               string              `yaml:"serviceID" validate:"nonzero"`
	DataDir                 string              `yaml:"dataDir" validate:"nonzero"` // path relative to m3em agent working directory
	Seeds                   []SeedConfig        `yaml:"seeds"`
	Instances               []PlacementInstance `yaml:"instances" validate:"min=1"`
}

// SeedConfig is a collection of Seed Data configurations
type SeedConfig struct {
	Namespace     string        `yaml:"namespace" validate:"nonzero"`
	LocalShardNum uint32        `yaml:"localShardNum" validate:"nonzero"`
	Retention     time.Duration `yaml:"retention" validate:"nonzero"`
	BlockSize     time.Duration `yaml:"blockSize" validate:"nonzero"`
	Delay         time.Duration `yaml:"delay"`
}

// PlacementInstance is a config for a placement instance.
type PlacementInstance struct {
	ID       string `yaml:"id" validate:"nonzero"`
	Rack     string `yaml:"rack" validate:"nonzero"`
	Zone     string `yaml:"zone" validate:"nonzero"`
	Weight   uint32 `yaml:"weight" validate:"nonzero"`
	Hostname string `yaml:"hostname" validate:"nonzero"`
}

// M3EMConfig is a list of m3em environment settings
type M3EMConfig struct {
	AgentPort     int                   `yaml:"agentPort" validate:"nonzero"`
	AgentTLS      *TLSConfiguration     `yaml:"agentTLS"`
	HeartbeatPort int                   `yaml:"heartbeatPort" validate:"nonzero"`
	Node          node.Configuration    `yaml:"node"`
	Cluster       cluster.Configuration `yaml:"cluster"`
}

// TLSConfiguration are the resources required for TLS Communication
type TLSConfiguration struct {
	ServerName    string `yaml:"serverName" validate:"nonzero"`
	CACrtPath     string `yaml:"caCrt" validate:"nonzero"`
	ClientCrtPath string `yaml:"clientCrt" validate:"nonzero"`
	ClientKeyPath string `yaml:"clientKey" validate:"nonzero"`
}

// Credentials returns the TransportCredentials corresponding to the provided struct
func (t TLSConfiguration) Credentials() (credentials.TransportCredentials, error) {
	caCrt, err := ioutil.ReadFile(t.CACrtPath)
	if err != nil {
		return nil, err
	}

	clientCrt, err := ioutil.ReadFile(t.ClientCrtPath)
	if err != nil {
		return nil, err
	}

	clientKey, err := ioutil.ReadFile(t.ClientKeyPath)
	if err != nil {
		return nil, err
	}

	return xgrpc.NewClientCredentials(t.ServerName, caCrt, clientCrt, clientKey)
}

// New constructs a Configuration object from the path specified
func New(m3emConfigPath string) (*Configuration, error) {
	var conf Configuration
	if err := xconfig.LoadFile(&conf, m3emConfigPath, xconfig.Options{}); err != nil {
		return nil, err
	}

	return &conf, nil
}

// Zone returns the zone configured for kv, and the instances if they are all the same;
// it returns an error if they're not.
func (c *Configuration) Zone() (string, error) {
	kvZone := c.KV.Zone
	for _, inst := range c.DTest.Instances {
		if kvZone != inst.Zone {
			return "", fmt.Errorf("instance has zone %s which differs from kv zone %s", kvZone, inst.Zone)
		}
	}
	return kvZone, nil
}

// Nodes returns a slice of m3emnode.Nodes per the config provided
func (c *Configuration) Nodes(opts node.Options, numNodes int) ([]m3emnode.Node, error) {
	// use all nodes if numNodes is zero
	if numNodes <= 0 {
		numNodes = len(c.DTest.Instances)
	}

	var (
		logger  = opts.InstrumentOptions().Logger()
		nodes   = make([]m3emnode.Node, 0, len(c.DTest.Instances))
		nodeNum = 0
	)

	for _, inst := range c.DTest.Instances {
		if nodeNum >= numNodes {
			break
		}

		pi := inst.newServicesPlacementInstance(c.DTest.NodePort)
		clientFn, err := inst.operatorClientFn(c.M3EM.AgentPort, c.M3EM.AgentTLS)
		if err != nil {
			return nil, fmt.Errorf("unable to create operationClientFn for %+v, error: %v", inst, err)
		}

		newOpts := opts.
			SetOperatorClientFn(clientFn).
			SetInstrumentOptions(opts.InstrumentOptions().SetLogger(
				logger.WithFields(xlog.NewField("host", inst.Hostname))))

		svcNode, err := node.New(pi, newOpts)
		if err != nil {
			return nil, fmt.Errorf("unable to create service node for %+v, error: %v", inst, err)
		}

		nodeOpts := m3emnode.NewOptions(newOpts.InstrumentOptions()).SetNodeOptions(newOpts)
		n, err := m3emnode.New(svcNode, nodeOpts)
		if err != nil {
			return nil, fmt.Errorf("unable to create m3emnode for %+v, error: %v", inst, err)
		}
		nodes = append(nodes, n)

		nodeNum++
	}

	return nodes, nil
}

func (pi *PlacementInstance) operatorClientFn(agentPort int, tlsConfig *TLSConfiguration) (node.OperatorClientFn, error) {
	agentEndpoint := fmt.Sprintf("%s:%d", pi.Hostname, agentPort)

	dialOpt := grpc.WithInsecure()
	if tlsConfig != nil {
		tc, err := tlsConfig.Credentials()
		if err != nil {
			return nil, err
		}
		dialOpt = grpc.WithTransportCredentials(tc)
	}

	return func() (*grpc.ClientConn, m3em.OperatorClient, error) {
		conn, err := grpc.Dial(agentEndpoint, dialOpt)
		if err != nil {
			return nil, nil, err
		}
		return conn, m3em.NewOperatorClient(conn), nil
	}, nil
}

func (pi *PlacementInstance) newServicesPlacementInstance(nodePort int) placement.Instance {
	endpoint := fmt.Sprintf("%s:%d", pi.Hostname, nodePort)
	return placement.NewInstance().
		SetID(pi.ID).
		SetIsolationGroup(pi.Rack).
		SetZone(pi.Zone).
		SetEndpoint(endpoint).
		SetWeight(pi.Weight)
}
