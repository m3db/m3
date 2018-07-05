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
)

// ClustersStaticConfiguration is a set of static cluster configurations.
type ClustersStaticConfiguration []ClusterStaticConfiguration

// ClusterStaticConfiguration is a static cluster configuration.
type ClusterStaticConfiguration struct {
	Client     client.Configuration                 `yaml:"client"`
	Namespaces ClusterStaticNamespacesConfiguration `yaml:"namespaces"`
}

// ClusterStaticNamespacesConfiguration describes the namespaces in a static
// cluster.
type ClusterStaticNamespacesConfiguration struct {
	StorageMetricsType storage.MetricsType `yaml:"storageMetricsType"`
	Retention          time.Duration       `yaml:"retention" validate:"nonzero"`
	Resolution         time.Duration       `yaml:"resolution" validate:"min=0"`
}
