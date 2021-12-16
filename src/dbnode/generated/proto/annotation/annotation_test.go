package annotation

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOneOf(t *testing.T) {
	legacy := &Payload{MetricType: MetricType_COUNTER, HandleValueResets: true}
	bytes, err := legacy.Marshal()
	require.NoError(t, err)

	alternative := &AltPayload{}
	err = alternative.Unmarshal(bytes)
	require.NoError(t, err)
}
