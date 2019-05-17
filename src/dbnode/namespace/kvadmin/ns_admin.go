package kvadmin

import (
	"errors"
	"fmt"

	"github.com/m3db/m3/src/cluster/kv"
	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/dbnode/namespace"
	xerrors "github.com/m3db/m3/src/x/errors"
)

var (
	errNotImplemented = errors.New("api not implemented")
)

type adminService struct {
	store kv.Store
	key   string
}

func NewAdminService(store kv.Store, key string) NamespaceMetadataAdminService {
	return &adminService{
		store: store,
		key:   key,
	}
}

func (as *adminService) GetAll() ([]*nsproto.NamespaceOptions, error) {
	return nil, errNotImplemented
}

func (as *adminService) Get(name string) (*nsproto.NamespaceOptions, error) {
	return nil, errNotImplemented
}

func (as *adminService) Add(name string, options *nsproto.NamespaceOptions) error {
	return errNotImplemented

}

func (as *adminService) Set(name string, options *nsproto.NamespaceOptions) error {
	return errNotImplemented
}

func (as *adminService) Delete(name string) error {
	return errNotImplemented
}

func (as *adminService) DeploySchema(name string, protoFileName, msgName string, protos map[string]string, deployID string) error {
	currentRegistry, currentVersion, err := as.currentRegistry()
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
		return fmt.Errorf("namespace(%s) is not found", name)
	}

	schemaOpt, err := namespace.AppendSchemaOptions(targetMeta.SchemaOptions,
		protoFileName, msgName, protos, deployID)
	if err != nil {
		return xerrors.Wrapf(err, "failed to append schema history from %s for message %s", protoFileName, msgName)
	}

	// Update schema options in place.
	targetMeta.SchemaOptions = schemaOpt

	_, err = as.store.CheckAndSet(as.key, currentVersion, currentRegistry)
	if err != nil {
		return xerrors.Wrapf(err, "failed to deploy schema from %s with version %s to namespace %s", protoFileName, deployID, name)
	}
	return nil
}

func (as *adminService) currentRegistry() (*nsproto.Registry, int, error) {
	value, err := as.store.Get(as.key)
	if err == kv.ErrNotFound {
		return nil, -1, nil
	}
	if err != nil {
		return nil, -1, err
	}

	var protoRegistry nsproto.Registry
	if err := value.Unmarshal(&protoRegistry); err != nil {
		return nil, -1, fmt.Errorf("unable to parse value, err: %v", err)
	}

	return &protoRegistry, value.Version(), nil
}
