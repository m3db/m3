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

	"github.com/pborman/uuid"

	"github.com/m3db/m3/src/cluster/kv"
	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/dbnode/namespace"
	xerrors "github.com/m3db/m3/src/x/errors"
)

var (
	// ErrNamespaceNotFound is returned when namespace is not found in registry.
	ErrNamespaceNotFound = errors.New("namespace is not found")
	// ErrNamespaceAlreadyExist is returned for addition of a namespace which already exists.
	ErrNamespaceAlreadyExist = errors.New("namespace already exists")
)

type adminService struct {
	store kv.Store
	key   string
	idGen func() string
}

const (
	// M3DBNodeNamespacesKey is the KV key that holds namespaces.
	M3DBNodeNamespacesKey = "m3db.node.namespaces"
)

func NewAdminService(store kv.Store, key string, idGen func() string) NamespaceMetadataAdminService {
	if idGen == nil {
		idGen = func() string {
			return uuid.New()
		}
	}
	if len(key) == 0 {
		key = M3DBNodeNamespacesKey
	}
	return &adminService{
		store: store,
		key:   key,
		idGen: idGen,
	}
}

func (as *adminService) GetAll() (*nsproto.Registry, error) {
	currentRegistry, _, err := as.currentRegistry()
	if err == kv.ErrNotFound {
		return nil, ErrNamespaceNotFound
	}
	if err != nil {
		return nil, xerrors.Wrapf(err, "failed to load current namespace metadatas for %s", as.key)
	}
	return currentRegistry, nil
}

func (as *adminService) Get(name string) (*nsproto.NamespaceOptions, error) {
	nsReg, err := as.GetAll()
	if err != nil {
		return nil, err
	}
	if nsOpt, ok := nsReg.GetNamespaces()[name]; ok {
		return nsOpt, nil
	}
	return nil, ErrNamespaceNotFound
}

func (as *adminService) Add(name string, options *nsproto.NamespaceOptions) error {
	nsMeta, err := namespace.ToMetadata(name, options)
	if err != nil {
		return xerrors.Wrapf(err, "invalid namespace options for namespace: %v", name)
	}
	currentRegistry, currentVersion, err := as.currentRegistry()
	if err == kv.ErrNotFound {
		_, err = as.store.SetIfNotExists(as.key, &nsproto.Registry{
			Namespaces: map[string]*nsproto.NamespaceOptions{name: options},
		})
		if err != nil {
			return xerrors.Wrapf(err, "failed to add namespace %v", name)
		}
		return nil
	}
	if err != nil {
		return xerrors.Wrapf(err, "failed to load namespace registry at %s", as.key)
	}

	if _, ok := currentRegistry.GetNamespaces()[name]; ok {
		return ErrNamespaceAlreadyExist
	}
	nsMap, err := namespace.FromProto(*currentRegistry)
	if err != nil {
		return xerrors.Wrap(err, "failed to unmarshall namespace registry")
	}

	newMap, err := namespace.NewMap(append(nsMap.Metadatas(), nsMeta))
	if err != nil {
		return err
	}

	protoMap, err := namespace.ToProto(newMap)
	if err != nil {
		return err
	}

	_, err = as.store.CheckAndSet(as.key, currentVersion, protoMap)
	if err != nil {
		return xerrors.Wrapf(err, "failed to add namespace %v", name)
	}
	return nil
}

func (as *adminService) Set(name string, options *nsproto.NamespaceOptions) error {
	_, err := namespace.ToMetadata(name, options)
	if err != nil {
		return xerrors.Wrapf(err, "invalid options for namespace: %v", name)
	}
	currentRegistry, currentVersion, err := as.currentRegistry()
	if err != nil {
		return xerrors.Wrapf(err, "failed to load namespace registry at %s", as.key)
	}
	if _, ok := currentRegistry.GetNamespaces()[name]; !ok {
		return ErrNamespaceNotFound
	}

	currentRegistry.Namespaces[name] = options

	_, err = as.store.CheckAndSet(as.key, currentVersion, currentRegistry)
	if err != nil {
		return xerrors.Wrapf(err, "failed to update namespace %v", name)
	}
	return nil
}

func (as *adminService) Delete(name string) error {
	currentRegistry, currentVersion, err := as.currentRegistry()
	if err != nil {
		return xerrors.Wrapf(err, "failed to load current namespace metadatas for %s", as.key)
	}

	nsMap, err := namespace.FromProto(*currentRegistry)
	if err != nil {
		return xerrors.Wrap(err, "failed to unmarshal namespace registry")
	}

	metadatas := nsMap.Metadatas()
	mdIdx := -1
	for idx, md := range nsMap.Metadatas() {
		if md.ID().String() == name {
			mdIdx = idx

			break
		}
	}

	if mdIdx == -1 {
		return ErrNamespaceNotFound
	}

	if len(metadatas) == 1 {
		if _, err := as.store.Delete(as.key); err != nil {
			return xerrors.Wrap(err, "failed to delete kv key")
		}

		return nil
	}

	// Replace the index where we found the metadata with the last element, then truncate
	metadatas[mdIdx] = metadatas[len(metadatas)-1]
	metadatas = metadatas[:len(metadatas)-1]

	newMap, err := namespace.NewMap(metadatas)
	if err != nil {
		return xerrors.Wrap(err, "namespace map construction failed")
	}

	protoMap, err := namespace.ToProto(newMap)
	if err != nil {
		return xerrors.Wrap(err, "namespace registry proto conversion failed")
	}

	_, err = as.store.CheckAndSet(as.key, currentVersion, protoMap)
	if err != nil {
		return xerrors.Wrapf(err, "failed to delete namespace %v", name)
	}

	return nil
}

func (as *adminService) ResetSchema(name string) error {
	currentRegistry, currentVersion, err := as.currentRegistry()
	if err == kv.ErrNotFound {
		return ErrNamespaceNotFound
	}
	if err != nil {
		return xerrors.Wrapf(err, "failed to load current namespace metadatas for %s", as.key)
	}

	var targetMeta *nsproto.NamespaceOptions
	for nsID, nsOpts := range currentRegistry.GetNamespaces() {
		if nsID == name {
			targetMeta = nsOpts
			break
		}
	}
	if targetMeta == nil {
		return ErrNamespaceNotFound
	}

	// Clear schema options in place.
	targetMeta.SchemaOptions = nil

	_, err = as.store.CheckAndSet(as.key, currentVersion, currentRegistry)
	if err != nil {
		return xerrors.Wrapf(err, "failed to reset schema for namespace %s", name)
	}
	return nil
}

func (as *adminService) DeploySchema(name, protoFileName, msgName string, protos map[string]string) (string, error) {
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

func LoadSchemaRegistryFromKVStore(schemaReg namespace.SchemaRegistry, kvStore kv.Store) error {
	if kvStore == nil {
		return errors.New("m3db metadata store is not configured properly")
	}
	as := NewAdminService(kvStore, "", nil)
	nsReg, err := as.GetAll()
	if err != nil {
		return xerrors.Wrap(err, "could not get metadata from metadata store")
	}
	nsMap, err := namespace.FromProto(*nsReg)
	if err != nil {
		return xerrors.Wrap(err, "could not unmarshal metadata")
	}
	merr := xerrors.NewMultiError()
	for _, metadata := range nsMap.Metadatas() {
		err = schemaReg.SetSchemaHistory(metadata.ID(), metadata.Options().SchemaHistory())
		if err != nil {
			merr = merr.Add(xerrors.Wrapf(err, "could not set schema history for namespace %s", metadata.ID().String()))
		}
	}
	return merr.FinalError()
}
