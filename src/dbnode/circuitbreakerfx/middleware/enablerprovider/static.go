package enablerprovider

import (
	"context"

	"github.com/m3db/m3/src/dbnode/circuitbreakerfx/middleware"
)

type (
	staticEnabler struct {
		enabled bool
		svcList map[svcKey]middleware.Mode
	}

	// svcKey is a helper struct used as the key of the map.
	svcKey struct {
		service   string
		procedure string
	}
)

var (
	allServicedKey = svcKey{service: middleware.AllServicesWildcard}
)

func newStaticEnabler(cfg middleware.Config) *staticEnabler {
	se := &staticEnabler{
		enabled: cfg.Enable,
		svcList: make(map[svcKey]middleware.Mode),
	}

	if cfg.EnableShadowMode {
		se.svcList[allServicedKey] = middleware.Shadow
	}

	// Shadow mode is not mentioned anywhere in config, therefore it should be disabled
	if _, ok := se.svcList[allServicedKey]; !ok {
		se.svcList[allServicedKey] = middleware.Rejection
	}

	return se
}

func (s *staticEnabler) IsEnabled(context.Context, string, string) bool {
	return s.enabled
}

func (s *staticEnabler) Mode(_ context.Context, service, procedure string) middleware.Mode {
	return s.getSettings(service, procedure)
}

func (s *staticEnabler) getSettings(service, procedure string) middleware.Mode {
	if mode, ok := s.svcList[svcKey{service: service, procedure: procedure}]; ok {
		return mode
	}

	if mode, ok := s.svcList[svcKey{service: service}]; ok {
		return mode
	}

	return s.svcList[allServicedKey]
}
