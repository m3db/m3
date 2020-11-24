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

package namespace

import (
	"errors"
	"fmt"
	"net/http"
	"path"
	"time"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/query/util/queryhttp"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"
)

const (
	// M3DBServiceName is the service name for M3DB.
	M3DBServiceName = "m3db"

	// ServicesPathName is the services part of the API path.
	ServicesPathName = "services"

	// M3DBNodeNamespacesKey is the KV key that holds namespaces.
	M3DBNodeNamespacesKey = "m3db.node.namespaces"

	// NamespacePathName is the namespace part of the API path.
	NamespacePathName = "namespace"

	// SchemaPathName is the schema part of the API path.
	SchemaPathName = "schema"
)

var (
	// M3DBServiceNamespacePathName is the M3DB service namespace API path.
	M3DBServiceNamespacePathName = path.Join(ServicesPathName, M3DBServiceName, NamespacePathName)
	// M3DBServiceSchemaPathName is the M3DB service schema API path.
	M3DBServiceSchemaPathName = path.Join(ServicesPathName, M3DBServiceName, SchemaPathName)

	errNamespaceNotFound = xhttp.NewError(errors.New("unable to find a namespace with specified name"), http.StatusNotFound)
)

// Handler represents a generic handler for namespace endpoints.
type Handler struct {
	// This is used by other namespace Handlers
	// nolint: structcheck
	client         clusterclient.Client
	clusters       m3.Clusters
	instrumentOpts instrument.Options
}

// Metadata returns the current metadata in the given store and its version
func Metadata(store kv.Store) ([]namespace.Metadata, int, error) {
	value, err := store.Get(M3DBNodeNamespacesKey)
	if err != nil {
		// From the perspective of namespace handlers, having had no metadata
		// set at all is semantically the same as having an empty slice of
		// metadata and is not a real error state.
		if err == kv.ErrNotFound {
			return []namespace.Metadata{}, 0, nil
		}

		return nil, -1, err
	}

	var protoRegistry nsproto.Registry
	if err := value.Unmarshal(&protoRegistry); err != nil {
		return nil, -1, fmt.Errorf("unable to parse value, err: %v", err)
	}

	nsMap, err := namespace.FromProto(protoRegistry)
	if err != nil {
		return nil, -1, err
	}

	return nsMap.Metadatas(), value.Version(), nil
}

type applyMiddlewareFn func(
	svc handleroptions.ServiceNameAndDefaults,
	w http.ResponseWriter,
	r *http.Request,
)

// RegisterRoutes registers the namespace routes.
func RegisterRoutes(
	r *queryhttp.EndpointRegistry,
	client clusterclient.Client,
	clusters m3.Clusters,
	defaults []handleroptions.ServiceOptionsDefault,
	instrumentOpts instrument.Options,
	namespaceValidator options.NamespaceValidator,
) error {
	applyMiddleware := func(
		f func(svc handleroptions.ServiceNameAndDefaults,
			w http.ResponseWriter, r *http.Request),
		defaults []handleroptions.ServiceOptionsDefault,
	) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			svc := handleroptions.ServiceNameAndDefaults{
				ServiceName: handleroptions.M3DBServiceName,
				Defaults:    defaults,
			}
			f(svc, w, r)
		})
	}

	// Get M3DB namespaces.
	if err := r.Register(queryhttp.RegisterOptions{
		Path:    M3DBGetURL,
		Handler: applyMiddleware(NewGetHandler(client, instrumentOpts).ServeHTTP, defaults),
		Methods: []string{GetHTTPMethod},
	}); err != nil {
		return err
	}

	// Add M3DB namespaces.
	if err := r.Register(queryhttp.RegisterOptions{
		Path:    M3DBAddURL,
		Handler: applyMiddleware(NewAddHandler(client, instrumentOpts, namespaceValidator).ServeHTTP, defaults),
		Methods: []string{AddHTTPMethod},
	}); err != nil {
		return err
	}

	// Update M3DB namespaces.
	if err := r.Register(queryhttp.RegisterOptions{
		Path:    M3DBUpdateURL,
		Handler: applyMiddleware(NewUpdateHandler(client, instrumentOpts).ServeHTTP, defaults),
		Methods: []string{UpdateHTTPMethod},
	}); err != nil {
		return err
	}

	// Delete M3DB namespaces.
	if err := r.Register(queryhttp.RegisterOptions{
		Path:    M3DBDeleteURL,
		Handler: applyMiddleware(NewDeleteHandler(client, instrumentOpts).ServeHTTP, defaults),
		Methods: []string{DeleteHTTPMethod},
	}); err != nil {
		return err
	}

	// Deploy M3DB schemas.
	if err := r.Register(queryhttp.RegisterOptions{
		Path:    M3DBSchemaURL,
		Handler: applyMiddleware(NewSchemaHandler(client, instrumentOpts).ServeHTTP, defaults),
		Methods: []string{SchemaDeployHTTPMethod},
	}); err != nil {
		return err
	}

	// Reset M3DB schemas.
	if err := r.Register(queryhttp.RegisterOptions{
		Path:    M3DBSchemaURL,
		Handler: applyMiddleware(NewSchemaResetHandler(client, instrumentOpts).ServeHTTP, defaults),
		Methods: []string{DeleteHTTPMethod},
	}); err != nil {
		return err
	}

	// Mark M3DB namespace as ready.
	if err := r.Register(queryhttp.RegisterOptions{
		Path:    M3DBReadyURL,
		Handler: applyMiddleware(NewReadyHandler(client, clusters, instrumentOpts).ServeHTTP, defaults),
		Methods: []string{ReadyHTTPMethod},
	}); err != nil {
		return err
	}

	return nil
}

func validateNamespaceAggregationOptions(mds []namespace.Metadata) error {
	resolutionRetentionMap := make(map[resolutionRetentionKey]bool, len(mds))

	for _, md := range mds {
		aggOpts := md.Options().AggregationOptions()
		if aggOpts == nil || len(aggOpts.Aggregations()) == 0 {
			continue
		}

		retention := md.Options().RetentionOptions().RetentionPeriod()
		for _, agg := range aggOpts.Aggregations() {
			if agg.Aggregated {
				key := resolutionRetentionKey{
					retention:  retention,
					resolution: agg.Attributes.Resolution,
				}

				if resolutionRetentionMap[key] {
					return fmt.Errorf("resolution and retention combination must be unique. "+
						"namespace with resolution=%v retention=%v already exists", key.resolution, key.retention)
				}
				resolutionRetentionMap[key] = true
			}
		}
	}

	return nil
}

type resolutionRetentionKey struct {
	resolution time.Duration
	retention  time.Duration
}
