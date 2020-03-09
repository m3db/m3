package builder

import (
	"testing"

	"github.com/m3db/m3/src/m3ninx/postings"

	"github.com/stretchr/testify/require"
)

func TestTermsReuse(t *testing.T) {
	terms := newTerms(NewOptions())

	require.NoError(t, terms.post([]byte("term"), postings.ID(1)))
	require.Equal(t, terms.size(), 1)
	require.Equal(t, terms.postings.Len(), 1)
	require.Equal(t, terms.postingsListUnion.Len(), 1)

	terms.reset()
	require.Equal(t, terms.size(), 0)
	require.Equal(t, terms.postings.Len(), 0)
	require.Equal(t, terms.postingsListUnion.Len(), 0)
}
