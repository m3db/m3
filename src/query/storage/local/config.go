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

package local

import (
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/stores/m3db"
	"github.com/m3db/m3x/ident"
)

var (
	defaultNewClientConfigurationParams = client.ConfigurationParameters{}
)

// ClustersStaticConfiguration is a set of static cluster configurations.
type ClustersStaticConfiguration []ClusterStaticConfiguration

// NewClientFromConfig is a method that can be set on
// ClusterStaticConfiguration to allow overriding the client initialization.
type NewClientFromConfig func(
	cfg client.Configuration,
	params client.ConfigurationParameters,
	custom ...client.CustomOption,
) (client.Client, error)

// ClusterStaticConfiguration is a static cluster configuration.
type ClusterStaticConfiguration struct {
	NewClientFromConfig NewClientFromConfig
	Namespaces          []ClusterStaticNamespaceConfiguration `yaml:"namespaces"`
	Client              client.Configuration                  `yaml:"client"`
}

func (c ClusterStaticConfiguration) newClient(
	params client.ConfigurationParameters,
	custom ...client.CustomOption,
) (client.Client, error) {
	if c.NewClientFromConfig != nil {
		return c.NewClientFromConfig(c.Client, params, custom...)
	}
	return c.Client.NewClient(params, custom...)
}

// ClusterStaticNamespaceConfiguration describes the namespaces in a
// static cluster.
type ClusterStaticNamespaceConfiguration struct {
	Namespace          string              `yaml:"namespace"`
	StorageMetricsType storage.MetricsType `yaml:"storageMetricsType"`
	Retention          time.Duration       `yaml:"retention" validate:"nonzero"`
	Resolution         time.Duration       `yaml:"resolution" validate:"min=0"`
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
	AsyncSessions bool
}

// NewClusters instantiates a new Clusters instance.
func (c ClustersStaticConfiguration) NewClusters(
	opts ClustersStaticConfigurationOptions,
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
		client, err := clusterCfg.newClient(defaultNewClientConfigurationParams)
		if err != nil {
			return nil, err
		}

		aggregatedClusterNamespacesCfg := &aggregatedClusterNamespacesConfiguration{
			client: client,
		}

		for _, n := range clusterCfg.Namespaces {
			switch n.StorageMetricsType {
			case storage.UnaggregatedMetricsType:
				numUnaggregatedClusterNamespaces++
				if numUnaggregatedClusterNamespaces > 1 {
					return nil, fmt.Errorf("only one unaggregated cluster namespace  "+
						"can be specified: specified %d", numUnaggregatedClusterNamespaces)
				}

				unaggregatedClusterNamespaceCfg.client = client
				unaggregatedClusterNamespaceCfg.namespace = n

			case storage.AggregatedMetricsType:
				numAggregatedClusterNamespaces++

				aggregatedClusterNamespacesCfg.namespaces =
					append(aggregatedClusterNamespacesCfg.namespaces, n)

			default:
				return nil, fmt.Errorf("unknown storage metrics type: %v",
					n.StorageMetricsType)
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

	// Connect to all clusters in parallel
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		cfg := unaggregatedClusterNamespaceCfg
		if !opts.AsyncSessions {
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
			if !opts.AsyncSessions {
				cfg.result.session, cfg.result.err = cfg.client.DefaultSession()
			} else {
				cfg.result.session = m3db.NewAsyncSession(func() (client.Client, error) {
					return cfg.client, nil
				}, nil)
			}
		}()
	}

	// Wait
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
			def := AggregatedClusterNamespaceDefinition{
				NamespaceID: ident.StringID(n.Namespace),
				Session:     cfg.result.session,
				Retention:   n.Retention,
				Resolution:  n.Resolution,
			}
			aggregatedClusterNamespaces = append(aggregatedClusterNamespaces, def)
		}
	}

	return NewClusters(unaggregatedClusterNamespace,
		aggregatedClusterNamespaces...)
}
