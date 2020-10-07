// Copyright (c) 2020  Uber Technologies, Inc.
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
	"sync"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"

	"go.uber.org/zap"
)

var (
	errDynamicClusterNamespaceConfigurationNotSet = errors.New("dynamicClusterNamespaceConfiguration not set")
	errInstrumentOptionsNotSet                    = errors.New("instrumentOptions not set")
	errNsWatchAlreadyClosed                       = errors.New("namespace watch already closed")
)

type dynamicCluster struct {
	clusterCfgs []DynamicClusterNamespaceConfiguration
	logger      *zap.Logger
	iOpts       instrument.Options

	sync.RWMutex

	namespaces              ClusterNamespaces
	unaggregatedNamespace   ClusterNamespace
	aggregatedNamespaces    map[RetentionResolution]ClusterNamespace
	namespacesByEtcdCluster map[int]clusterNamespaceLookup

	nsWatches []namespace.NamespaceWatch
	closed    bool
}

// NewDynamicClusters creates an implementation of the Clusters interface
// supports dynamic updating of cluster namespaces.
func NewDynamicClusters(_ DynamicClusterOptions) (Clusters, error) {
	return nil, errors.New("dynamic cluster configuration not yet supported")
}

// TODO(nate): Replace constructor above with this method
// once namespace staging is complete.
//
// newDynamicClusters creates an implementation of the Clusters interface
// supports dynamic updating of cluster namespaces.
func newDynamicClusters(opts DynamicClusterOptions) (Clusters, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	cluster := dynamicCluster{
		clusterCfgs:             opts.DynamicClusterNamespaceConfiguration(),
		logger:                  opts.InstrumentOptions().Logger(),
		iOpts:                   opts.InstrumentOptions(),
		namespacesByEtcdCluster: make(map[int]clusterNamespaceLookup),
	}

	if err := cluster.init(); err != nil {
		if cErr := cluster.Close(); cErr != nil {
			return nil, cErr
		}

		return nil, err
	}

	return &cluster, nil
}

func (d *dynamicCluster) init() error {
	d.logger.Info("creating namespaces watches", zap.Int("clusters", len(d.clusterCfgs)))

	var (
		wg       sync.WaitGroup
		multiErr xerrors.MultiError
		errLock  sync.Mutex
	)
	// Configure watch for each cluster provided
	for i, cfg := range d.clusterCfgs {
		i := i
		cfg := cfg

		wg.Add(1)
		go func() {
			if err := d.initNamespaceWatch(i, cfg); err != nil {
				errLock.Lock()
				multiErr = multiErr.Add(err)
				errLock.Unlock()
			}
			wg.Done()
		}()
	}

	wg.Wait()
	if !multiErr.Empty() {
		return multiErr.FinalError()
	}

	return nil
}

func (d *dynamicCluster) initNamespaceWatch(etcdClusterId int, cfg DynamicClusterNamespaceConfiguration) error {
	registry, err := cfg.nsInitializer.Init()
	if err != nil {
		return err
	}

	// Get a namespace watch
	watch, err := registry.Watch()
	if err != nil {
		return err
	}

	// Wait till first namespaces value is received and set the value.
	d.logger.Info("resolving namespaces with namespace watch", zap.Int("cluster", etcdClusterId))
	<-watch.C()

	updater := func(namespaces namespace.Map) error {
		d.updateNamespaces(etcdClusterId, cfg, namespaces)
		return nil
	}
	nsWatch := namespace.NewNamespaceWatch(updater, watch, d.iOpts)
	if err = nsWatch.Start(); err != nil {
		return err
	}

	nsMap := watch.Get()
	d.updateNamespaces(etcdClusterId, cfg, nsMap)

	d.Lock()
	d.nsWatches = append(d.nsWatches, nsWatch)
	d.Unlock()

	return nil
}

func (d *dynamicCluster) updateNamespaces(
	etcdClusterId int,
	clusterCfg DynamicClusterNamespaceConfiguration,
	newNamespaces namespace.Map,
) {
	if newNamespaces == nil {
		d.logger.Debug("received empty namespace map. ignoring", zap.Int("cluster", etcdClusterId))
		return
	}

	d.Lock()
	defer d.Unlock()

	d.updateNamespacesByEtcdClusterWithLock(etcdClusterId, clusterCfg, newNamespaces)
	d.updateClusterNamespacesWithLock()
}

func (d *dynamicCluster) updateNamespacesByEtcdClusterWithLock(
	etcdClusterId int,
	clusterCfg DynamicClusterNamespaceConfiguration,
	newNamespaces namespace.Map,
) {
	// TODO(nate): incorporate checking if new namespace is ready once staging state has landed.

	// Check if existing namespaces still exist or need to be updated.
	if _, ok := d.namespacesByEtcdCluster[etcdClusterId]; !ok {
		d.namespacesByEtcdCluster[etcdClusterId] = newClusterNamespaceLookup(len(newNamespaces.IDs()))
	}
	existing := d.namespacesByEtcdCluster[etcdClusterId]
	var (
		sz      = len(newNamespaces.Metadatas())
		added   = make([]string, 0, sz)
		updated = make([]string, 0, sz)
		removed = make([]string, 0, sz)
	)
	for nsId, nsMd := range existing.idToMetadata {
		newNsMd, err := newNamespaces.Get(ident.StringID(nsId))
		// non-nil error here means namespace is not present (i.e. namespace has been removed)
		if err != nil {
			existing.remove(nsId)
			removed = append(removed, nsId)
		}

		// Namespace options have been updated; regenerate cluster namespaces.
		if !nsMd.Equal(newNsMd) {
			// Replace with new metadata and cluster namespaces.
			newClusterNamespaces, err := toClusterNamespaces(clusterCfg, newNsMd)
			if err != nil {
				// Log error, but don't allow singular failed namespace update to fail all namespace updates.
				d.logger.Error("failed to update namespace", zap.String("namespace", nsId),
					zap.Error(err))
				continue
			}
			existing.update(nsId, newNsMd, newClusterNamespaces)
			updated = append(updated, nsId)
		}
	}

	// Check for new namespaces to add.
	for _, newNsMd := range newNamespaces.Metadatas() {
		// Namespace has been added.
		if !existing.exists(newNsMd.ID().String()) {
			newClusterNamespaces, err := toClusterNamespaces(clusterCfg, newNsMd)
			if err != nil {
				// Log error, but don't allow singular failed namespace update to fail all namespace updates.
				d.logger.Error("failed to update namespace", zap.String("namespace", newNsMd.ID().String()),
					zap.Error(err))
				continue
			}
			existing.add(newNsMd.ID().String(), newNsMd, newClusterNamespaces)
			added = append(added, newNsMd.ID().String())
		}
	}

	if len(added) > 0 {
		d.logger.Info("added cluster namespaces", zap.Strings("namespaces", added))
	}

	if len(updated) > 0 {
		d.logger.Info("updated cluster namespaces", zap.Strings("namespaces", updated))
	}

	if len(removed) > 0 {
		d.logger.Info("removed cluster namespaces", zap.Strings("namespaces", removed))
	}
}

func toClusterNamespaces(clusterCfg DynamicClusterNamespaceConfiguration, md namespace.Metadata) (ClusterNamespaces, error) {
	aggOpts := md.Options().AggregationOptions()
	if aggOpts == nil {
		return nil, fmt.Errorf("no aggregationOptions present for namespace %v", md.ID().String())
	}

	if len(aggOpts.Aggregations()) == 0 {
		return nil, fmt.Errorf("no aggregations present for namespace %v", md.ID().String())
	}

	retOpts := md.Options().RetentionOptions()
	if retOpts == nil {
		return nil, fmt.Errorf("no retentionOptions present for namespace %v", md.ID().String())
	}

	clusterNamespaces := make(ClusterNamespaces, 0, len(aggOpts.Aggregations()))
	for _, agg := range aggOpts.Aggregations() {
		var (
			clusterNamespace ClusterNamespace
			err              error
		)
		if agg.Aggregated {
			clusterNamespace, err = newAggregatedClusterNamespace(AggregatedClusterNamespaceDefinition{
				NamespaceID: md.ID(),
				Session:     clusterCfg.session,
				Retention:   retOpts.RetentionPeriod(),
				Resolution:  agg.Attributes.Resolution,
				Downsample: &ClusterNamespaceDownsampleOptions{
					All: agg.Attributes.DownsampleOptions.All,
				},
			})
			if err != nil {
				return nil, err
			}
		} else {
			clusterNamespace, err = newUnaggregatedClusterNamespace(UnaggregatedClusterNamespaceDefinition{
				NamespaceID: md.ID(),
				Session:     clusterCfg.session,
				Retention:   retOpts.RetentionPeriod(),
			})
			if err != nil {
				return nil, err
			}
		}
		clusterNamespaces = append(clusterNamespaces, clusterNamespace)
	}

	return clusterNamespaces, nil
}

func (d *dynamicCluster) updateClusterNamespacesWithLock() {
	nsCount := 0
	for _, nsMap := range d.namespacesByEtcdCluster {
		for _, clusterNamespaces := range nsMap.metadataToClusterNamespaces {
			nsCount += len(clusterNamespaces)
		}
	}

	var (
		newNamespaces            = make(ClusterNamespaces, 0, nsCount)
		newAggregatedNamespaces  = make(map[RetentionResolution]ClusterNamespace)
		newUnaggregatedNamespace ClusterNamespace
	)

	for _, nsMap := range d.namespacesByEtcdCluster {
		for _, clusterNamespaces := range nsMap.metadataToClusterNamespaces {
			for _, clusterNamespace := range clusterNamespaces {
				attrs := clusterNamespace.Options().Attributes()
				if attrs.MetricsType == storagemetadata.UnaggregatedMetricsType {
					if newUnaggregatedNamespace != nil {
						d.logger.Warn("more than one unaggregated namespace found. using most recently "+
							"discovered unaggregated namespace",
							zap.String("existing", newUnaggregatedNamespace.NamespaceID().String()),
							zap.String("new", clusterNamespace.NamespaceID().String()))
					}
					newUnaggregatedNamespace = clusterNamespace
				} else {
					retRes := RetentionResolution{
						Retention:  attrs.Retention,
						Resolution: attrs.Resolution,
					}
					existing, ok := newAggregatedNamespaces[retRes]
					if ok {
						d.logger.Warn("more than one aggregated namespace found for retention and resolution. "+
							"using most recently discovered aggregated namespace",
							zap.String("retention", retRes.Retention.String()),
							zap.String("resolution", retRes.Resolution.String()),
							zap.String("existing", existing.NamespaceID().String()),
							zap.String("new", clusterNamespace.NamespaceID().String()))
					}
					newAggregatedNamespaces[retRes] = clusterNamespace
				}
			}
		}
	}

	newNamespaces = append(newNamespaces, newUnaggregatedNamespace)
	for _, ns := range newAggregatedNamespaces {
		newNamespaces = append(newNamespaces, ns)
	}

	d.unaggregatedNamespace = newUnaggregatedNamespace
	d.aggregatedNamespaces = newAggregatedNamespaces
	d.namespaces = newNamespaces
}

func (d *dynamicCluster) Close() error {
	d.Lock()
	defer d.Unlock()

	if d.closed {
		return errNsWatchAlreadyClosed
	}

	d.closed = true

	var multiErr xerrors.MultiError
	for _, watch := range d.nsWatches {
		if err := watch.Close(); err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	if !multiErr.Empty() {
		return multiErr.FinalError()
	}

	return nil
}

func (d *dynamicCluster) ClusterNamespaces() ClusterNamespaces {
	d.RLock()
	defer d.RUnlock()

	return d.namespaces
}

func (d *dynamicCluster) UnaggregatedClusterNamespace() ClusterNamespace {
	d.RLock()
	defer d.RUnlock()

	return d.unaggregatedNamespace
}

func (d *dynamicCluster) AggregatedClusterNamespace(attrs RetentionResolution) (ClusterNamespace, bool) {
	d.RLock()
	defer d.RUnlock()

	namespace, ok := d.aggregatedNamespaces[attrs]
	return namespace, ok
}

// clusterNamespaceLookup is a helper to track namespace changes. Two maps are necessary
// to handle the update case which causes the metadata for a previously seen namespaces to change.
// idToMetadata map allows us to find the previous metadata to detect changes. metadataToClusterNamespaces
// map allows us to find ClusterNamespaces generated from the metadata's AggregationOptions.
type clusterNamespaceLookup struct {
	idToMetadata                map[string]namespace.Metadata
	metadataToClusterNamespaces map[namespace.Metadata]ClusterNamespaces
}

func newClusterNamespaceLookup(size int) clusterNamespaceLookup {
	return clusterNamespaceLookup{
		idToMetadata:                make(map[string]namespace.Metadata, size),
		metadataToClusterNamespaces: make(map[namespace.Metadata]ClusterNamespaces, size),
	}
}

func (c *clusterNamespaceLookup) exists(nsId string) bool {
	_, ok := c.idToMetadata[nsId]
	return ok
}

func (c *clusterNamespaceLookup) add(nsId string, nsMd namespace.Metadata, clusterNamespaces ClusterNamespaces) {
	c.idToMetadata[nsId] = nsMd
	c.metadataToClusterNamespaces[nsMd] = clusterNamespaces
}

func (c *clusterNamespaceLookup) update(nsId string, nsMd namespace.Metadata, clusterNamespaces ClusterNamespaces) {
	existingMd := c.idToMetadata[nsId]
	c.idToMetadata[nsId] = nsMd
	delete(c.metadataToClusterNamespaces, existingMd)
	c.metadataToClusterNamespaces[nsMd] = clusterNamespaces
}

func (c *clusterNamespaceLookup) remove(nsId string) {
	existingMd := c.idToMetadata[nsId]
	delete(c.metadataToClusterNamespaces, existingMd)
	delete(c.idToMetadata, nsId)
}
