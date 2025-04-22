package enablerprovider

import (
	"github.com/m3db/m3/src/dbnode/circuitbreakerfx/middleware"
)

var (
	_ middleware.Enabler = (*staticEnabler)(nil)
)

// New returns a Flipr based enabler for the middleware when Flipr property
// is configured or fallsback to static config.
func New(cfg middleware.Config) middleware.Enabler {
	stEnabler := newStaticEnabler(cfg)
	return stEnabler
}
