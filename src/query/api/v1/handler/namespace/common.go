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

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/gorilla/mux"
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

	errNamespaceNotFound = errors.New("unable to find a namespace with specified name")
)

// Handler represents a generic handler for namespace endpoints.
type Handler struct {
	// This is used by other namespace Handlers
	// nolint: structcheck
	client         clusterclient.Client
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

// RegisterRoutes registers the namespace routes.
func RegisterRoutes(
	r *mux.Router,
	client clusterclient.Client,
	defaults []handleroptions.ServiceOptionsDefault,
	instrumentOpts instrument.Options,
) {
	wrapped := func(n http.Handler) http.Handler {
		return logging.WithResponseTimeAndPanicErrorLogging(n, instrumentOpts)
	}
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
	getHandler := wrapped(NewGetHandler(client, instrumentOpts))
	r.HandleFunc(DeprecatedM3DBGetURL, getHandler.ServeHTTP).Methods(GetHTTPMethod)
	r.HandleFunc(M3DBGetURL, getHandler.ServeHTTP).Methods(GetHTTPMethod)

	// Add M3DB namespaces.
	addHandler := wrapped(
		applyMiddleware(NewAddHandler(client, instrumentOpts).ServeHTTP, defaults))
	r.HandleFunc(DeprecatedM3DBAddURL, addHandler.ServeHTTP).Methods(AddHTTPMethod)
	r.HandleFunc(M3DBAddURL, addHandler.ServeHTTP).Methods(AddHTTPMethod)

	// Update M3DB namespaces.
	updateHandler := wrapped(
		applyMiddleware(NewUpdateHandler(client, instrumentOpts).ServeHTTP, defaults))
	r.HandleFunc(M3DBUpdateURL, updateHandler.ServeHTTP).Methods(UpdateHTTPMethod)

	// Delete M3DB namespaces.
	deleteHandler := wrapped(NewDeleteHandler(client, instrumentOpts))
	r.HandleFunc(DeprecatedM3DBDeleteURL, deleteHandler.ServeHTTP).Methods(DeleteHTTPMethod)
	r.HandleFunc(M3DBDeleteURL, deleteHandler.ServeHTTP).Methods(DeleteHTTPMethod)

	// Deploy M3DB schemas.
	schemaHandler := wrapped(
		applyMiddleware(NewSchemaHandler(client, instrumentOpts).ServeHTTP, defaults))
	r.HandleFunc(M3DBSchemaURL, schemaHandler.ServeHTTP).Methods(SchemaDeployHTTPMethod)

	// Reset M3DB schemas.
	schemaResetHandler := wrapped(
		applyMiddleware(NewSchemaResetHandler(client, instrumentOpts).ServeHTTP, defaults))
	r.HandleFunc(M3DBSchemaURL, schemaResetHandler.ServeHTTP).Methods(DeleteHTTPMethod)
}
