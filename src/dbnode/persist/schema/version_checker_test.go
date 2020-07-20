package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexEntryValidationEnabled(t *testing.T) {
	checker := NewVersionChecker(1, 1)
	require.True(t, checker.IndexEntryValidationEnabled())

	checker = NewVersionChecker(1, 2)
	require.True(t, checker.IndexEntryValidationEnabled())

	checker = NewVersionChecker(2, 1)
	require.True(t, checker.IndexEntryValidationEnabled())

	checker = NewVersionChecker(2, 0)
	require.True(t, checker.IndexEntryValidationEnabled())
}

func TestIndexEntryValidationDisabled(t *testing.T) {
	checker := NewVersionChecker(1, 0)
	require.False(t, checker.IndexEntryValidationEnabled())
}
