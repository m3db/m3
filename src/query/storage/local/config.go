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
	goerrors "errors"
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/stores/m3db"
	"github.com/m3db/m3x/ident"
)

var (
	errNotAggregatedClusterNamespace              = goerrors.New("not an aggregated cluster namespace")
	errBothNamespaceTypeNewAndDeprecatedFieldsSet = goerrors.New("cannot specify both deprecated and non-deprecated fields for namespace type")

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
	// Namespace is namespace in the cluster that is specified.
	Namespace string `yaml:"namespace"`

	// Type is the type of values stored by the namespace, current
	// supported values are "unaggregated" or "aggregated".
	Type storage.MetricsType `yaml:"type"`

	// Retention is the length of which values are stored by the namespace.
	Retention time.Duration `yaml:"retention" validate:"nonzero"`

	// Resolution is the frequency of which values are stored by the namespace.
	Resolution time.Duration `yaml:"resolution" validate:"min=0"`

	// Downsample is the configuration for downsampling options to use with
	// the namespace.
	Downsample *DownsampleClusterStaticNamespaceConfiguration `yaml:"downsample"`

	// StorageMetricsType is the namespace type.
	//
	// Deprecated: Use "Type" field when specifying config instead, it is
	// invalid to use both.
	StorageMetricsType storage.MetricsType `yaml:"storageMetricsType"`
}

func (c ClusterStaticNamespaceConfiguration) metricsType() (storage.MetricsType, error) {
	result := storage.DefaultMetricsType
	if c.Type != storage.DefaultMetricsType && c.StorageMetricsType != storage.DefaultMetricsType {
		// Don't allow both to not be default
		return result, errBothNamespaceTypeNewAndDeprecatedFieldsSet
	}

	if c.Type != storage.DefaultMetricsType {
		// New field value set
		return c.Type, nil
	}

	if c.StorageMetricsType != storage.DefaultMetricsType {
		// Deprecated field value set
		return c.StorageMetricsType, nil
	}

	// Both are default
	return result, nil
}

func (c ClusterStaticNamespaceConfiguration) downsampleOptions() (
	ClusterNamespaceDownsampleOptions,
	error,
) {
	nsType, err := c.metricsType()
	if err != nil {
		return ClusterNamespaceDownsampleOptions{}, err
	}
	if nsType != storage.AggregatedMetricsType {
		return ClusterNamespaceDownsampleOptions{}, errNotAggregatedClusterNamespace
	}
	if c.Downsample == nil {
		return defaultClusterNamespaceDownsampleOptions, nil
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
			nsType, err := n.metricsType()
			if err != nil {
				return nil, err
			}

			switch nsType {
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
			}
			aggregatedClusterNamespaces = append(aggregatedClusterNamespaces, def)
		}
	}

	return NewClusters(unaggregatedClusterNamespace,
		aggregatedClusterNamespaces...)
}
