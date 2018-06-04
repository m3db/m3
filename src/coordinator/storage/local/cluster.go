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
	"time"

	"github.com/m3db/m3db/src/coordinator/storage"
	"github.com/m3db/m3db/src/dbnode/client"
	"github.com/m3db/m3x/ident"
)

// Clusters is a flattened collection of local storage clusters and namespaces.
type Clusters interface {
	ClusterNamespaces() ([]ClusterNamespace, error)

	UnaggregatedClusterNamespace() (ClusterNamespace, error)

	AggregatedClusterNamespace(
		params AggregatedClusterNamespaceParams,
	) (ClusterNamespace, error)
}

// AggregatedClusterNamespaceParams is a set of parameters required to resolve
// an aggregated cluster namespace.
type AggregatedClusterNamespaceParams struct {
	Retention  time.Duration
	Resolution time.Duration
}

// ClusterNamespace is a local storage cluster namespace.
type ClusterNamespace interface {
	NamespaceID() ident.ID
	Attributes() storage.Attributes
	Session() (client.Session, error)
}
