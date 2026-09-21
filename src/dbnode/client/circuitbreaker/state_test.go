package circuitbreaker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStateStringer(t *testing.T) {
	tests := []struct {
		state          State
		expectedString string
	}{
		{state: State(0), expectedString: "unknown"},
		{state: Healthy, expectedString: "healthy"},
		{state: Unhealthy, expectedString: "unhealthy"},
		{state: Probing, expectedString: "probing"},
	}

	for _, test := range tests {
		assert.Equal(t, test.expectedString, test.state.String(), "unexpected state string")
	}
}
