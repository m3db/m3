package enablerprovider

import (
	"github.com/m3db/m3/src/dbnode/circuitbreaker/middleware"
)

var (
	_ middleware.Enabler = (*staticEnabler)(nil)
)

// New returns a Flipr based enabler for the middleware when Flipr property
// is configured or fallsback to static config.
func New() (middleware.Enabler, error) {
	stEnabler := newStaticEnabler()
	return stEnabler, nil
}
