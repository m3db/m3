// Package namespace provides namespace functionality for the matcher.
package namespace

import (
	"github.com/m3db/m3/src/metrics/metric/id"
)

var (
	defaultNamespaceTag   = []byte("namespace")
	defaultNamespaceValue = []byte("default")
	// Default is the default Resolver.
	Default = NewResolver(defaultNamespaceTag, defaultNamespaceValue)
)

// Resolver resolves a namespace value from an encoded metric id.
type Resolver interface {
	// Resolve the namespace value.
	Resolve(id id.ID) []byte
}

// NewResolver creates a new Resolver.
func NewResolver(namespaceTag, defaultNamespace []byte) Resolver {
	if namespaceTag == nil {
		namespaceTag = defaultNamespaceTag
	}
	if defaultNamespace == nil {
		defaultNamespace = defaultNamespaceValue
	}
	return &resolver{
		namespaceTag:     namespaceTag,
		defaultNamespace: defaultNamespace,
	}
}

type resolver struct {
	namespaceTag     []byte
	defaultNamespace []byte
}

func (r resolver) Resolve(id id.ID) []byte {
	ns, found := id.TagValue(r.namespaceTag)
	if !found {
		ns = r.defaultNamespace
	}
	return ns
}
