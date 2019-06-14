// Copyright (c) 2019 Uber Technologies, Inc.
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

package kvadmin

import (
	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
)

type NamespaceMetadataAdminService interface {
	// GetAll gets namespace options for all namespaces.
	GetAll() (*nsproto.Registry, error)

	// Get gets option for the specified namespace.
	Get(name string) (*nsproto.NamespaceOptions, error)

	// Add adds a new namespace and set its options.
	Add(name string, options *nsproto.NamespaceOptions) error

	// Set sets the options for the specified namespace.
	Set(name string, options *nsproto.NamespaceOptions) error

	// Delete deletes the specified namespace.
	Delete(name string) error

	// DeploySchema deploys a new version schema to the specified namespace.
	// An opaque string (deployID) is returned if successful.
	// Application developer is to include the deployID in their m3db client configuration
	// when they upgrade their application to use the new schema version.
	DeploySchema(name, protoFileName, msgName string, protos map[string]string) (string, error)

	// ResetSchema reset schema for the specified namespace.
	ResetSchema(name string) error
}
