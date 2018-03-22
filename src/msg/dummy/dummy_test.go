package dummy

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDummy(t *testing.T) {
	require.NoError(t, Dummy())
	require.NoError(t, Dummy())
}
