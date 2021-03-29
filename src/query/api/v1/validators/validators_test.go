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

package validators

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/x/ident"
)

var (
	id   = ident.BytesID("id")
	opts = namespace.NewOptions()
)

func TestValidateNewNamespace(t *testing.T) {
	valid, err := namespace.NewMetadata(id, opts)
	require.NoError(t, err)

	assert.NoError(t, NamespaceValidator.ValidateNewNamespace(valid, nil))
}

func TestValidateNewNamespaceFailOnBlockSize(t *testing.T) {
	mismatchingBlockOpts := opts.
		SetRetentionOptions(opts.RetentionOptions().SetBlockSize(7200000000000)).
		SetIndexOptions(opts.IndexOptions().SetBlockSize(7200000000000 * 2))
	mismatchingBlocks, err := namespace.NewMetadata(id, mismatchingBlockOpts)
	require.NoError(t, err)

	err = NamespaceValidator.ValidateNewNamespace(mismatchingBlocks, nil)
	assert.EqualError(t, err, "index and retention block size must match (2h0m0s, 4h0m0s)")
}

func TestValidateNewNamespaceFailDuplicate(t *testing.T) {
	ns, err := namespace.NewMetadata(id, opts)
	require.NoError(t, err)

	err = NamespaceValidator.ValidateNewNamespace(ns, []namespace.Metadata{ns})
	assert.Equal(t, ErrNamespaceExists, err)
}
