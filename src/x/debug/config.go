package debug

import "runtime"

// Configuration for the debug package.
type Configuration struct {

	// MutexProfileFraction is used to set the runtime.SetMutexProfileFraction to report mutex convention events.
	// See https://tip.golang.org/pkg/runtime/#SetMutexProfileFraction for more details about the values.
	MutexProfileFraction int `yaml:"mutex_profile_fraction"`
}

// SetMutexProfileFraction sets the configured mutex profile fraction for the runtime.
func (c Configuration) SetMutexProfileFraction() {
	runtime.SetMutexProfileFraction(c.MutexProfileFraction)
}
