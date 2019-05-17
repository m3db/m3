package kvadmin

import (
	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
)

type NamespaceMetadataAdminService interface {
	GetAll() ([]*nsproto.NamespaceOptions, error)
	Get(name string) (*nsproto.NamespaceOptions, error)
	Add(name string, options *nsproto.NamespaceOptions) error
	Set(name string, options *nsproto.NamespaceOptions) error
	Delete(name string) error
	DeploySchema(name string, protoFileName, msgName string, protos map[string]string, deployID string) error
}
