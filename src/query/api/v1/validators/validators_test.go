package validators

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/x/ident"
)

func TestValidateNewNamespace(t *testing.T) {
	var (
		id   = ident.BytesID("id")
		opts = namespace.NewOptions()
	)

	valid, err := namespace.NewMetadata(id, opts)
	require.NoError(t, err)

	assert.NoError(t, NamespaceValidator.ValidateNewNamespace(valid, nil))

	// Prevent mismatching block sizes.
	mismatchingBlockOpts := opts.
		SetRetentionOptions(opts.RetentionOptions().SetBlockSize(7200000000000)).
		SetIndexOptions(opts.IndexOptions().SetBlockSize(7200000000000 * 2))
	mismatchingBlocks, err := namespace.NewMetadata(id, mismatchingBlockOpts)
	require.NoError(t, err)

	err = NamespaceValidator.ValidateNewNamespace(mismatchingBlocks, nil)
	assert.EqualError(t, err, "index and retention block size must match (2h0m0s, 4h0m0s)")
}
