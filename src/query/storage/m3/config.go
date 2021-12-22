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

package m3

import (
	goerrors "errors"
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"github.com/m3db/m3/src/query/stores/m3db"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
)

var (
	errNotAggregatedClusterNamespace = goerrors.New("not an aggregated cluster namespace")
	errNoNamespaceInitializerSet     = goerrors.New("no namespace initializer set")
)

// ClustersStaticConfiguration is a set of static cluster configurations.
type ClustersStaticConfiguration []ClusterStaticConfiguration

// NewClientFromConfig is a method that can be set on
// ClusterStaticConfiguration to allow overriding the client initialization.
type NewClientFromConfig func(
	cfg client.Configuration,
	params client.ConfigurationParameters,
	custom ...client.CustomAdminOption,
) (client.Client, error)

// ClusterStaticConfiguration is a static cluster configuration.
type ClusterStaticConfiguration struct {
	NewClientFromConfig NewClientFromConfig                   `yaml:"-"`
	Namespaces          []ClusterStaticNamespaceConfiguration `yaml:"namespaces"`
	Client              client.Configuration                  `yaml:"client"`
}

func (c ClusterStaticConfiguration) newClient(
	params client.ConfigurationParameters,
	custom ...client.CustomAdminOption,
) (client.Client, error) {
	if c.NewClientFromConfig != nil {
		return c.NewClientFromConfig(c.Client, params, custom...)
	}
	return c.Client.NewAdminClient(params, custom...)
}

// ClusterStaticNamespaceConfiguration describes the namespaces in a
// static cluster.
type ClusterStaticNamespaceConfiguration struct {
	// Namespace is namespace in the cluster that is specified.
	Namespace string `yaml:"namespace"`

	// Type is the type of values stored by the namespace, current
	// supported values are "unaggregated" or "aggregated".
	Type storagemetadata.MetricsType `yaml:"type"`

	// Retention is the length of which values are stored by the namespace.
	Retention time.Duration `yaml:"retention" validate:"nonzero"`

	// Resolution is the frequency of which values are stored by the namespace.
	Resolution time.Duration `yaml:"resolution" validate:"min=0"`

	// Downsample is the configuration for downsampling options to use with
	// the namespace.
	Downsample *DownsampleClusterStaticNamespaceConfiguration `yaml:"downsample"`

	// ReadOnly prevents any writes to this namespace.
	ReadOnly bool `yaml:"readOnly"`

	// DataLatency is the duration after which the data is available in this namespace.
	DataLatency time.Duration `yaml:"dataLatency"`
}

func (c ClusterStaticNamespaceConfiguration) metricsType() (storagemetadata.MetricsType, error) {
	unset := storagemetadata.MetricsType(0)

	if c.Type != unset {
		// New field value set
		return c.Type, nil
	}

	// Both are unset
	return storagemetadata.DefaultMetricsType, nil
}

func (c ClusterStaticNamespaceConfiguration) downsampleOptions() (
	ClusterNamespaceDownsampleOptions,
	error,
) {
	nsType, err := c.metricsType()
	if err != nil {
		return ClusterNamespaceDownsampleOptions{}, err
	}
	if nsType != storagemetadata.AggregatedMetricsType {
		return ClusterNamespaceDownsampleOptions{}, errNotAggregatedClusterNamespace
	}
	if c.Downsample == nil {
		return DefaultClusterNamespaceDownsampleOptions, nil
	}

	return c.Downsample.downsampleOptions(), nil
}

// DownsampleClusterStaticNamespaceConfiguration is configuration
// specified for downsampling options on an aggregated cluster namespace.
type DownsampleClusterStaticNamespaceConfiguration struct {
	All bool `yaml:"all"`
}

func (c DownsampleClusterStaticNamespaceConfiguration) downsampleOptions() ClusterNamespaceDownsampleOptions {
	return ClusterNamespaceDownsampleOptions(c)
}

type unaggregatedClusterNamespaceConfiguration struct {
	client    client.Client
	namespace ClusterStaticNamespaceConfiguration
	result    clusterConnectResult
}

type aggregatedClusterNamespacesConfiguration struct {
	client     client.Client
	namespaces []ClusterStaticNamespaceConfiguration
	result     clusterConnectResult
}

type clusterConnectResult struct {
	session client.Session
	err     error
}

// ClustersStaticConfigurationOptions are options to use when
// constructing clusters from config.
type ClustersStaticConfigurationOptions struct {
	AsyncSessions      bool
	ProvidedSession    client.Session
	CustomAdminOptions []client.CustomAdminOption
	EncodingOptions    encoding.Options
}

// NewStaticClusters instantiates a new Clusters instance based on
// static configuration.
func (c ClustersStaticConfiguration) NewStaticClusters(
	instrumentOpts instrument.Options,
	opts ClustersStaticConfigurationOptions,
	clusterNamespacesWatcher ClusterNamespacesWatcher,
) (Clusters, error) {
	var (
		numUnaggregatedClusterNamespaces int
		numAggregatedClusterNamespaces   int
		unaggregatedClusterNamespaceCfg  = &unaggregatedClusterNamespaceConfiguration{}
		aggregatedClusterNamespacesCfgs  []*aggregatedClusterNamespacesConfiguration
		unaggregatedClusterNamespace     UnaggregatedClusterNamespaceDefinition
		aggregatedClusterNamespaces      []AggregatedClusterNamespaceDefinition
	)
	for _, clusterCfg := range c {
		var (
			result client.Client
			err    error
		)

		if opts.ProvidedSession == nil {
			// NB(r): Only create client session if not already provided.
			result, err = clusterCfg.newClient(client.ConfigurationParameters{
				InstrumentOptions: instrumentOpts,
				EncodingOptions:   opts.EncodingOptions,
			}, opts.CustomAdminOptions...)
			if err != nil {
				return nil, err
			}
		}

		aggregatedClusterNamespacesCfg := &aggregatedClusterNamespacesConfiguration{
			client: result,
		}

		for _, n := range clusterCfg.Namespaces {
			nsType, err := n.metricsType()
			if err != nil {
				return nil, err
			}

			switch nsType {
			case storagemetadata.UnaggregatedMetricsType:
				numUnaggregatedClusterNamespaces++
				if numUnaggregatedClusterNamespaces > 1 {
					return nil, fmt.Errorf("only one unaggregated cluster namespace  "+
						"can be specified: specified %d", numUnaggregatedClusterNamespaces)
				}

				unaggregatedClusterNamespaceCfg.client = result
				unaggregatedClusterNamespaceCfg.namespace = n

			case storagemetadata.AggregatedMetricsType:
				numAggregatedClusterNamespaces++

				aggregatedClusterNamespacesCfg.namespaces =
					append(aggregatedClusterNamespacesCfg.namespaces, n)

			default:
				return nil, fmt.Errorf("unknown storage metrics type: %v", nsType)
			}
		}

		if len(aggregatedClusterNamespacesCfg.namespaces) > 0 {
			aggregatedClusterNamespacesCfgs =
				append(aggregatedClusterNamespacesCfgs, aggregatedClusterNamespacesCfg)
		}
	}

	if numUnaggregatedClusterNamespaces != 1 {
		return nil, fmt.Errorf("one unaggregated cluster namespace  "+
			"must be specified: specified %d", numUnaggregatedClusterNamespaces)
	}

	// Connect to all clusters in parallel.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		cfg := unaggregatedClusterNamespaceCfg
		if opts.ProvidedSession != nil {
			cfg.result.session = opts.ProvidedSession
		} else if !opts.AsyncSessions {
			cfg.result.session, cfg.result.err = cfg.client.DefaultSession()
		} else {
			cfg.result.session = m3db.NewAsyncSession(func() (client.Client, error) {
				return cfg.client, nil
			}, nil)
		}
	}()
	for _, cfg := range aggregatedClusterNamespacesCfgs {
		cfg := cfg // Capture var
		wg.Add(1)
		go func() {
			defer wg.Done()
			if opts.ProvidedSession != nil {
				cfg.result.session = opts.ProvidedSession
			} else if !opts.AsyncSessions {
				cfg.result.session, cfg.result.err = cfg.client.DefaultSession()
			} else {
				cfg.result.session = m3db.NewAsyncSession(func() (client.Client, error) {
					return cfg.client, nil
				}, nil)
			}
		}()
	}

	// Wait for connections.
	wg.Wait()

	if unaggregatedClusterNamespaceCfg.result.err != nil {
		return nil, fmt.Errorf("could not connect to unaggregated cluster: %v",
			unaggregatedClusterNamespaceCfg.result.err)
	}

	unaggregatedClusterNamespace = UnaggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID(unaggregatedClusterNamespaceCfg.namespace.Namespace),
		Session:     unaggregatedClusterNamespaceCfg.result.session,
		Retention:   unaggregatedClusterNamespaceCfg.namespace.Retention,
	}

	for i, cfg := range aggregatedClusterNamespacesCfgs {
		if cfg.result.err != nil {
			return nil, fmt.Errorf("could not connect to aggregated cluster #%d: %v",
				i, cfg.result.err)
		}

		for _, n := range cfg.namespaces {
			downsampleOpts, err := n.downsampleOptions()
			if err != nil {
				return nil, fmt.Errorf("error parse downsample options for cluster #%d namespace %s: %v",
					i, n.Namespace, err)
			}

			def := AggregatedClusterNamespaceDefinition{
				NamespaceID: ident.StringID(n.Namespace),
				Session:     cfg.result.session,
				Retention:   n.Retention,
				Resolution:  n.Resolution,
				Downsample:  &downsampleOpts,
				ReadOnly:    n.ReadOnly,
				DataLatency: n.DataLatency,
			}
			aggregatedClusterNamespaces = append(aggregatedClusterNamespaces, def)
		}
	}

	clusters, err := NewClusters(unaggregatedClusterNamespace,
		aggregatedClusterNamespaces...)
	if err != nil {
		return nil, err
	}

	if err := clusterNamespacesWatcher.Update(clusters.ClusterNamespaces()); err != nil {
		return nil, err
	}

	return clusters, nil
}

// NB(nate): exists primarily for testing.
type newClustersFn func(DynamicClusterOptions) (Clusters, error)

// NewDynamicClusters instantiates a new Clusters instance that pulls
// cluster information from etcd.
func (c ClustersStaticConfiguration) NewDynamicClusters(
	instrumentOpts instrument.Options,
	opts ClustersStaticConfigurationOptions,
	clusterNamespacesWatcher ClusterNamespacesWatcher,
) (Clusters, error) {
	return c.newDynamicClusters(NewDynamicClusters, instrumentOpts, opts, clusterNamespacesWatcher)
}

func (c ClustersStaticConfiguration) newDynamicClusters(
	newFn newClustersFn,
	instrumentOpts instrument.Options,
	opts ClustersStaticConfigurationOptions,
	clusterNamespacesWatcher ClusterNamespacesWatcher,
) (Clusters, error) {
	clients := make([]client.Client, 0, len(c))
	for _, clusterCfg := range c {
		clusterClient, err := clusterCfg.newClient(client.ConfigurationParameters{
			InstrumentOptions: instrumentOpts,
			EncodingOptions:   opts.EncodingOptions,
		}, opts.CustomAdminOptions...)
		if err != nil {
			return nil, err
		}
		clients = append(clients, clusterClient)
	}

	// Connect to all clusters in parallel
	var (
		wg   sync.WaitGroup
		cfgs = make([]DynamicClusterNamespaceConfiguration, len(clients))

		errLock  sync.Mutex
		multiErr xerrors.MultiError
	)
	for i, clusterClient := range clients {
		i := i
		clusterClient := clusterClient
		nsInit := clusterClient.Options().NamespaceInitializer()

		// TODO(nate): move this validation to client.Options once static configuration of namespaces
		// is no longer allowed.
		if nsInit == nil {
			return nil, errNoNamespaceInitializerSet
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			cfgs[i].nsInitializer = nsInit
			if opts.ProvidedSession != nil {
				cfgs[i].session = opts.ProvidedSession
			} else if !opts.AsyncSessions {
				var err error
				session, err := clusterClient.DefaultSession()
				if err != nil {
					errLock.Lock()
					multiErr = multiErr.Add(err)
					errLock.Unlock()
				}
				cfgs[i].session = session
			} else {
				cfgs[i].session = m3db.NewAsyncSession(func() (client.Client, error) {
					return clusterClient, nil
				}, nil)
			}
		}()
	}

	wg.Wait()

	if !multiErr.Empty() {
		// Close any created sessions on failure.
		for _, cfg := range cfgs {
			if cfg.session != nil {
				// Returns an error if session is already closed which, in this case,
				// is fine
				_ = cfg.session.Close()
			}
		}
		return nil, multiErr.FinalError()
	}

	dcOpts := NewDynamicClusterOptions().
		SetDynamicClusterNamespaceConfiguration(cfgs).
		SetClusterNamespacesWatcher(clusterNamespacesWatcher).
		SetInstrumentOptions(instrumentOpts)

	return newFn(dcOpts)
}
