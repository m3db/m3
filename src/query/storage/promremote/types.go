// Copyright (c) 2021  Uber Technologies, Inc.
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

// Package promremote implements storage interface backed by Prometheus remote write capable endpoints.
package promremote

import (
	"github.com/uber-go/tally"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"github.com/m3db/m3/src/x/ident"
	xhttp "github.com/m3db/m3/src/x/net/http"
)

// Options for storage.
type Options struct {
	endpoints   []EndpointOptions
	httpOptions xhttp.HTTPClientOptions
	scope       tally.Scope
	logger      *zap.Logger
}

// Namespaces returns M3 namespaces from endpoint opts.
func (o Options) Namespaces() m3.ClusterNamespaces {
	namespaces := make(m3.ClusterNamespaces, 0, len(o.endpoints))
	for _, endpoint := range o.endpoints {
		namespaces = append(namespaces, newClusterNamespace(endpoint))
	}
	return namespaces
}

// EndpointOptions for single prometheus remote write capable endpoint.
type EndpointOptions struct {
	name          string
	address       string
	attributes    storagemetadata.Attributes
	downsampleAll bool
}

func newClusterNamespace(endpoint EndpointOptions) m3.ClusterNamespace {
	return promRemoteNamespace{
		// NB(antanas): NewOptions validates endpoint name to be unique in the list of endpoints.
		nsID: ident.StringID(endpoint.name),
		options: m3.NewClusterNamespaceOptions(
			endpoint.attributes,
			&m3.ClusterNamespaceDownsampleOptions{
				All: endpoint.downsampleAll,
			},
		),
	}
}

type promRemoteNamespace struct {
	nsID    ident.ID
	options m3.ClusterNamespaceOptions
}

func (e promRemoteNamespace) NamespaceID() ident.ID {
	return e.nsID
}

func (e promRemoteNamespace) Options() m3.ClusterNamespaceOptions {
	return e.options
}

func (e promRemoteNamespace) Session() client.Session {
	// NB(antanas): should never be called since there is no m3db backend in this case.
	panic("M3DB client session can't be used when using prom remote storage backend")
}
