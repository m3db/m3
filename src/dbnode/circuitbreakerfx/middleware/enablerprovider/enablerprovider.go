package enablerprovider

import (
	"github.com/m3db/m3/src/dbnode/circuitbreakerfx/middleware"
)

var (
	_ middleware.Enabler = (*staticEnabler)(nil)
)

// New returns a static enabler for the middleware
func New(cfg middleware.Config) middleware.Enabler {
	stEnabler := newStaticEnabler(cfg)
	return stEnabler
}
