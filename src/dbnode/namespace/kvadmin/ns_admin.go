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
	"errors"
	"fmt"

	"github.com/m3db/m3/src/cluster/kv"
	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/dbnode/namespace"
	xerrors "github.com/m3db/m3/src/x/errors"

	"github.com/satori/go.uuid"
)

var (
	ErrNotImplemented = errors.New("api not implemented")
	ErrNamespaceNotFound = errors.New("namespace is not found")
)

type adminService struct {
	store kv.Store
	key   string
	idGen func() string
}

func NewAdminService(store kv.Store, key string, idGen func() string) NamespaceMetadataAdminService {
	if idGen == nil {
		idGen = func() string {
			return uuid.NewV4().String()
		}
	}
	return &adminService{
		store: store,
		key:   key,
		idGen: idGen,
	}
}

func (as *adminService) GetAll() ([]*nsproto.NamespaceOptions, error) {
	return nil, ErrNotImplemented
}

func (as *adminService) Get(name string) (*nsproto.NamespaceOptions, error) {
	return nil, ErrNotImplemented
}

func (as *adminService) Add(name string, options *nsproto.NamespaceOptions) error {
	return ErrNotImplemented

}

func (as *adminService) Set(name string, options *nsproto.NamespaceOptions) error {
	return ErrNotImplemented
}

func (as *adminService) Delete(name string) error {
	return ErrNotImplemented
}

func (as *adminService) DeploySchema(name string, protoFileName, msgName string, protos map[string]string) (string, error) {
	currentRegistry, currentVersion, err := as.currentRegistry()
	if err == kv.ErrNotFound {
		return "", ErrNamespaceNotFound
	}
	if err != nil {
		return "", xerrors.Wrapf(err, "failed to load current namespace metadatas for %s", as.key)
	}
	var targetMeta *nsproto.NamespaceOptions
	for nsID, nsOpts := range currentRegistry.GetNamespaces() {
		if nsID == name {
			targetMeta = nsOpts
			break
		}
	}
	if targetMeta == nil {
		return "", ErrNamespaceNotFound
	}

	deployID := as.idGen()

	schemaOpt, err := namespace.AppendSchemaOptions(targetMeta.SchemaOptions,
		protoFileName, msgName, protos, deployID)
	if err != nil {
		return "", xerrors.Wrapf(err, "failed to append schema history from %s for message %s", protoFileName, msgName)
	}

	// Update schema options in place.
	targetMeta.SchemaOptions = schemaOpt

	_, err = as.store.CheckAndSet(as.key, currentVersion, currentRegistry)
	if err != nil {
		return "", xerrors.Wrapf(err, "failed to deploy schema from %s with version %s to namespace %s", protoFileName, deployID, name)
	}
	return deployID, nil
}

func (as *adminService) currentRegistry() (*nsproto.Registry, int, error) {
	value, err := as.store.Get(as.key)
	if err != nil {
		return nil, -1, err
	}

	var protoRegistry nsproto.Registry
	if err := value.Unmarshal(&protoRegistry); err != nil {
		return nil, -1, fmt.Errorf("unable to parse value, err: %v", err)
	}

	return &protoRegistry, value.Version(), nil
}
