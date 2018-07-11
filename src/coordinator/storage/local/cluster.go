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
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/m3db/m3db/src/coordinator/storage"
	"github.com/m3db/m3db/src/dbnode/client"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/ident"
)

var (
	errNamespaceIDNotSet = errors.New("namespace ID not set")
	errSessionNotSet     = errors.New("session not set")
	errRetentionNotSet   = errors.New("retention not set")
	errResolutionNotSet  = errors.New("resolution not set")
)

// Clusters is a flattened collection of local storage clusters and namespaces.
type Clusters interface {
	io.Closer

	// ClusterNamespaces returns all known cluster namespaces.
	ClusterNamespaces() []ClusterNamespace

	// UnaggregatedClusterNamespace returns the valid unaggregated
	// cluster namespace.
	UnaggregatedClusterNamespace() ClusterNamespace

	// AggregatedClusterNamespace returns an aggregated cluster namespace
	// at a specific retention and resolution.
	AggregatedClusterNamespace(attrs RetentionResolution) (ClusterNamespace, bool)
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
	Attributes() storage.Attributes
	Session() client.Session
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
	namespaces := make([]ClusterNamespace, 0, expectedAll)
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
			Retention:  namespace.Attributes().Retention,
			Resolution: namespace.Attributes().Resolution,
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

func (c *clusters) ClusterNamespaces() []ClusterNamespace {
	return c.namespaces
}

func (c *clusters) UnaggregatedClusterNamespace() ClusterNamespace {
	return c.unaggregatedNamespace
}

func (c *clusters) AggregatedClusterNamespace(
	attrs RetentionResolution,
) (ClusterNamespace, bool) {
	namespace, ok := c.aggregatedNamespaces[attrs]
	return namespace, ok
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

	return syncMultiErrs.finalError()
}

type clusterNamespace struct {
	namespaceID ident.ID
	attributes  storage.Attributes
	session     client.Session
}

func newUnaggregatedClusterNamespace(
	def UnaggregatedClusterNamespaceDefinition,
) (ClusterNamespace, error) {
	if err := def.Validate(); err != nil {
		return nil, err
	}
	return &clusterNamespace{
		namespaceID: def.NamespaceID,
		attributes: storage.Attributes{
			MetricsType: storage.UnaggregatedMetricsType,
			Retention:   def.Retention,
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
	return &clusterNamespace{
		namespaceID: def.NamespaceID,
		attributes: storage.Attributes{
			MetricsType: storage.AggregatedMetricsType,
			Retention:   def.Retention,
			Resolution:  def.Resolution,
		},
		session: def.Session,
	}, nil
}

func (n *clusterNamespace) NamespaceID() ident.ID {
	return n.namespaceID
}

func (n *clusterNamespace) Attributes() storage.Attributes {
	return n.attributes
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

func (errs *syncMultiErrs) finalError() error {
	errs.Lock()
	defer errs.Unlock()
	return errs.multiErr.FinalError()
}
