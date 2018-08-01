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
	"fmt"

	clusterclient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/kv"
	nsproto "github.com/m3db/m3db/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3db/src/dbnode/storage/namespace"
	"github.com/m3db/m3db/src/query/util/logging"

	"github.com/gorilla/mux"
)

const (
	// M3DBNodeNamespacesKey is the KV key that holds namespaces
	M3DBNodeNamespacesKey = "m3db.node.namespaces"
)

// Handler represents a generic handler for namespace endpoints.
type Handler struct {
	// This is used by other namespace Handlers
	// nolint: structcheck
	client clusterclient.Client
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

// RegisterRoutes registers the namespace routes
func RegisterRoutes(r *mux.Router, client clusterclient.Client) {
	logged := logging.WithResponseTimeLogging

	r.HandleFunc(GetURL, logged(NewGetHandler(client)).ServeHTTP).Methods(GetHTTPMethod)
	r.HandleFunc(AddURL, logged(NewAddHandler(client)).ServeHTTP).Methods(AddHTTPMethod)
	r.HandleFunc(DeleteURL, logged(NewDeleteHandler(client)).ServeHTTP).Methods(DeleteHTTPMethod)
}
