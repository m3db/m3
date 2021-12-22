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
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
)

var (
	errNamespaceIDNotSet   = errors.New("namespace ID not set")
	errSessionNotSet       = errors.New("session not set")
	errRetentionNotSet     = errors.New("retention not set")
	errResolutionNotSet    = errors.New("resolution not set")
	errNegativeDataLatency = errors.New("negative dataLatency")

	// DefaultClusterNamespaceDownsampleOptions is a default options.
	// NB(antanas): this was made public to access it in promremote storage.
	// Ideally downsampling could be decoupled from m3 storage.
	DefaultClusterNamespaceDownsampleOptions = ClusterNamespaceDownsampleOptions{
		All: true,
	}
)

// ClusterConfigType is an enum representing the configuration used
// to create a Clusters interface
type ClusterConfigType int

const (
	// ClusterConfigTypeStatic is for static configuration.
	ClusterConfigTypeStatic = iota
	// ClusterConfigTypeDynamic is for dynamic configuration.
	ClusterConfigTypeDynamic
)

// Clusters is a flattened collection of local storage clusters and namespaces.
type Clusters interface {
	io.Closer

	// ClusterNamespaces returns all known and ready cluster namespaces.
	ClusterNamespaces() ClusterNamespaces

	// NonReadyClusterNamespaces returns all cluster namespaces not in the ready state.
	NonReadyClusterNamespaces() ClusterNamespaces

	// UnaggregatedClusterNamespace returns the valid unaggregated
	// cluster namespace. If the namespace is not yet initialized, returns false.
	UnaggregatedClusterNamespace() (ClusterNamespace, bool)

	// AggregatedClusterNamespace returns an aggregated cluster namespace
	// at a specific retention and resolution.
	AggregatedClusterNamespace(attrs RetentionResolution) (ClusterNamespace, bool)

	// ConfigType returns the type of configuration used to create this Clusters
	// object.
	ConfigType() ClusterConfigType
}

// RetentionResolution is a tuple of retention and resolution that describes
// an aggregated metrics policy.
type RetentionResolution struct {
	Retention  time.Duration
	Resolution time.Duration
}

// ClusterNamespace is a local storage cluster namespace.
type ClusterNamespace interface {
	NamespaceID() ident.ID
	Options() ClusterNamespaceOptions
	Session() client.Session
}

// ClusterNamespaceOptions is a set of options
type ClusterNamespaceOptions struct {
	// Note: Don't allow direct access, as we want to provide defaults
	// and/or error if call to access a field is not relevant/correct.
	attributes  storagemetadata.Attributes
	downsample  *ClusterNamespaceDownsampleOptions
	dataLatency time.Duration
	readOnly    bool
}

// NewClusterNamespaceOptions creates new cluster namespace options.
func NewClusterNamespaceOptions(
	attributes storagemetadata.Attributes,
	downsample *ClusterNamespaceDownsampleOptions,
) ClusterNamespaceOptions {
	return ClusterNamespaceOptions{
		attributes: attributes,
		downsample: downsample,
	}
}

// Attributes returns the storage attributes of the cluster namespace.
func (o ClusterNamespaceOptions) Attributes() storagemetadata.Attributes {
	return o.attributes
}

// DataLatency returns the duration after which the data is available in this cluster namespace.
func (o ClusterNamespaceOptions) DataLatency() time.Duration {
	return o.dataLatency
}

// ReadOnly returns the value of ReadOnly option for a cluster namespace.
func (o ClusterNamespaceOptions) ReadOnly() bool {
	return o.readOnly
}

// DownsampleOptions returns the downsample options for a cluster namespace,
// which is only valid if the namespace is an aggregated cluster namespace.
func (o ClusterNamespaceOptions) DownsampleOptions() (
	ClusterNamespaceDownsampleOptions,
	error,
) {
	if o.attributes.MetricsType != storagemetadata.AggregatedMetricsType {
		return ClusterNamespaceDownsampleOptions{}, errNotAggregatedClusterNamespace
	}
	if o.downsample == nil {
		return DefaultClusterNamespaceDownsampleOptions, nil
	}
	return *o.downsample, nil
}

// ClusterNamespaceDownsampleOptions is the downsample options for
// a cluster namespace.
type ClusterNamespaceDownsampleOptions struct {
	All bool
}

// ClusterNamespaces is a slice of ClusterNamespace instances.
type ClusterNamespaces []ClusterNamespace

// NumAggregatedClusterNamespaces returns the number of aggregated
// cluster namespaces.
func (n ClusterNamespaces) NumAggregatedClusterNamespaces() int {
	count := 0
	for _, namespace := range n {
		if namespace.Options().Attributes().MetricsType == storagemetadata.AggregatedMetricsType {
			count++
		}
	}
	return count
}

// UnaggregatedClusterNamespaceDefinition is the definition for the
// cluster namespace that holds unaggregated metrics data.
type UnaggregatedClusterNamespaceDefinition struct {
	NamespaceID ident.ID
	Session     client.Session
	Retention   time.Duration
}

// Validate will validate the cluster namespace definition.
func (def UnaggregatedClusterNamespaceDefinition) Validate() error {
	if def.NamespaceID == nil || len(def.NamespaceID.String()) == 0 {
		return errNamespaceIDNotSet
	}
	if def.Session == nil {
		return errSessionNotSet
	}
	if def.Retention <= 0 {
		return errRetentionNotSet
	}
	return nil
}

// AggregatedClusterNamespaceDefinition is a definition for a
// cluster namespace that holds aggregated metrics data at a
// specific retention and resolution.
type AggregatedClusterNamespaceDefinition struct {
	NamespaceID ident.ID
	Session     client.Session
	Retention   time.Duration
	Resolution  time.Duration
	Downsample  *ClusterNamespaceDownsampleOptions
	DataLatency time.Duration
	ReadOnly    bool
}

// Validate validates the cluster namespace definition.
func (def AggregatedClusterNamespaceDefinition) Validate() error {
	if def.NamespaceID == nil || len(def.NamespaceID.String()) == 0 {
		return errNamespaceIDNotSet
	}
	if def.Session == nil {
		return errSessionNotSet
	}
	if def.Retention <= 0 {
		return errRetentionNotSet
	}
	if def.Resolution <= 0 {
		return errResolutionNotSet
	}
	if def.DataLatency < 0 {
		return errNegativeDataLatency
	}
	return nil
}

type clusters struct {
	namespaces            []ClusterNamespace
	unaggregatedNamespace ClusterNamespace
	aggregatedNamespaces  map[RetentionResolution]ClusterNamespace
}

// NewClusters instantiates a new Clusters instance.
func NewClusters(
	unaggregatedClusterNamespace UnaggregatedClusterNamespaceDefinition,
	aggregatedClusterNamespaces ...AggregatedClusterNamespaceDefinition,
) (Clusters, error) {
	expectedAggregated := len(aggregatedClusterNamespaces)
	expectedAll := 1 + expectedAggregated
	namespaces := make(ClusterNamespaces, 0, expectedAll)
	aggregatedNamespaces := make(map[RetentionResolution]ClusterNamespace,
		expectedAggregated)

	def := unaggregatedClusterNamespace
	unaggregatedNamespace, err := newUnaggregatedClusterNamespace(def)
	if err != nil {
		return nil, err
	}

	namespaces = append(namespaces, unaggregatedNamespace)
	for _, def := range aggregatedClusterNamespaces {
		namespace, err := newAggregatedClusterNamespace(def)
		if err != nil {
			return nil, err
		}

		namespaces = append(namespaces, namespace)
		key := RetentionResolution{
			Retention:  namespace.Options().Attributes().Retention,
			Resolution: namespace.Options().Attributes().Resolution,
		}

		_, exists := aggregatedNamespaces[key]
		if exists {
			return nil, fmt.Errorf("duplicate aggregated namespace exists for: "+
				"retention=%s, resolution=%s",
				key.Retention.String(), key.Resolution.String())
		}

		aggregatedNamespaces[key] = namespace
	}

	return &clusters{
		namespaces:            namespaces,
		unaggregatedNamespace: unaggregatedNamespace,
		aggregatedNamespaces:  aggregatedNamespaces,
	}, nil
}

func (c *clusters) ClusterNamespaces() ClusterNamespaces {
	return c.namespaces
}

func (c *clusters) NonReadyClusterNamespaces() ClusterNamespaces {
	// statically configured cluster namespaces are always considered ready.
	return nil
}

func (c *clusters) UnaggregatedClusterNamespace() (ClusterNamespace, bool) {
	return c.unaggregatedNamespace, true
}

func (c *clusters) AggregatedClusterNamespace(
	attrs RetentionResolution,
) (ClusterNamespace, bool) {
	namespace, ok := c.aggregatedNamespaces[attrs]
	return namespace, ok
}

func (c *clusters) ConfigType() ClusterConfigType {
	return ClusterConfigTypeStatic
}

func (c *clusters) Close() error {
	var (
		wg             sync.WaitGroup
		syncMultiErrs  syncMultiErrs
		uniqueSessions []client.Session
	)
	// Collect unique sessions, some namespaces may share same
	// client session (same cluster)
	uniqueSessions = append(uniqueSessions, c.unaggregatedNamespace.Session())
	for _, namespace := range c.aggregatedNamespaces {
		unique := true
		for _, session := range uniqueSessions {
			if namespace.Session() == session {
				unique = false
				break
			}
		}
		if unique {
			uniqueSessions = append(uniqueSessions, namespace.Session())
		}
	}

	for _, session := range uniqueSessions {
		session := session // Capture for lambda
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := session.Close()
			syncMultiErrs.add(err)
		}()
	}

	wg.Wait()

	return syncMultiErrs.lastError()
}

type clusterNamespace struct {
	namespaceID ident.ID
	options     ClusterNamespaceOptions
	session     client.Session
}

func newUnaggregatedClusterNamespace(
	def UnaggregatedClusterNamespaceDefinition,
) (ClusterNamespace, error) {
	if err := def.Validate(); err != nil {
		return nil, err
	}

	ns := def.NamespaceID
	// Set namespace to NoFinalize to avoid cloning it in write operations
	ns.NoFinalize()
	return &clusterNamespace{
		namespaceID: ns,
		options: ClusterNamespaceOptions{
			attributes: storagemetadata.Attributes{
				MetricsType: storagemetadata.UnaggregatedMetricsType,
				Retention:   def.Retention,
			},
		},
		session: def.Session,
	}, nil
}

func newAggregatedClusterNamespace(
	def AggregatedClusterNamespaceDefinition,
) (ClusterNamespace, error) {
	if err := def.Validate(); err != nil {
		return nil, err
	}

	ns := def.NamespaceID
	// Set namespace to NoFinalize to avoid cloning it in write operations
	ns.NoFinalize()
	return &clusterNamespace{
		namespaceID: ns,
		options: ClusterNamespaceOptions{
			attributes: storagemetadata.Attributes{
				MetricsType: storagemetadata.AggregatedMetricsType,
				Retention:   def.Retention,
				Resolution:  def.Resolution,
			},
			downsample:  def.Downsample,
			dataLatency: def.DataLatency,
			readOnly:    def.ReadOnly,
		},
		session: def.Session,
	}, nil
}

func (n *clusterNamespace) NamespaceID() ident.ID {
	return n.namespaceID
}

func (n *clusterNamespace) Options() ClusterNamespaceOptions {
	return n.options
}

func (n *clusterNamespace) Session() client.Session {
	return n.session
}

type syncMultiErrs struct {
	sync.Mutex
	multiErr xerrors.MultiError
}

func (errs *syncMultiErrs) add(err error) {
	errs.Lock()
	errs.multiErr = errs.multiErr.Add(err)
	errs.Unlock()
}

func (errs *syncMultiErrs) lastError() error {
	errs.Lock()
	defer errs.Unlock()
	// TODO: consider taking a debug param when building a syncMultiErrs
	// which would determine wether to return only the last error message
	// or the consolidated list of errors.
	return errs.multiErr.LastError()
}
