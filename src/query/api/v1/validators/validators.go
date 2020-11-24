// package validators contains validation logics for the api.
package validators

import (
	"errors"
	"fmt"

	"github.com/m3db/m3/src/dbnode/namespace"
	xerrors "github.com/m3db/m3/src/x/errors"
)

var (
	// NamespaceValidator is an instance of namespaceValidator.
	NamespaceValidator = &namespaceValidator{}

	// ErrNamespaceExists is returned when trying to create a namespace with id that already exists.
	ErrNamespaceExists = errors.New("namespace with the same ID already exists")
)

type namespaceValidator struct{}

// Validate new namespace inputs only. Validation that applies to namespaces
// regardless of create/update/etc belongs in the option-specific Validate
// functions which are invoked on every change operation.
func (h *namespaceValidator) ValidateNewNamespace(
	ns namespace.Metadata,
	existing []namespace.Metadata,
) error {
	var (
		id                 = ns.ID()
		indexBlockSize     = ns.Options().RetentionOptions().BlockSize()
		retentionBlockSize = ns.Options().IndexOptions().BlockSize()
	)

	if indexBlockSize != retentionBlockSize {
		return xerrors.NewInvalidParamsError(
			fmt.Errorf("index and retention block size must match (%v, %v)",
				indexBlockSize,
				retentionBlockSize))
	}

	for _, existingNs := range existing {
		if id.Equal(existingNs.ID()) {
			return ErrNamespaceExists
		}
	}

	return nil
}
