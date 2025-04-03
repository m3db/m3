package enablerprovider

import (
	"context"

	"github.com/m3db/m3/src/dbnode/circuitbreaker/middleware"
)

type (
	staticEnabler struct {
		enabled bool
	}
)

func newStaticEnabler() *staticEnabler {
	se := &staticEnabler{
		enabled: true,
	}
	return se
}

func (s *staticEnabler) IsEnabled(context.Context) bool {
	return true
}

func (s *staticEnabler) Mode(_ context.Context) middleware.Mode {
	return middleware.Rejection
}
