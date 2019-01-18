package ts

import (
	"testing"

	"github.com/m3db/m3/src/query/graphite/context"

	"github.com/stretchr/testify/require"
)

func TestValuesClose(t *testing.T) {
	ctx := context.New()
	defer ctx.Close()

	vals := newValues(ctx, 1000, 10, 0).(*float64Values)
	err := vals.Close()

	require.NoError(t, err)
	require.Equal(t, 0, vals.numSteps)
	require.Nil(t, vals.values)
}
