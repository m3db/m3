// Copyright (c) 2020 Uber Technologies, Inc.
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

//package validators contains validation logics for the api.
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
