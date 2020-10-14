package config

import "runtime"

// DebugConfiguration for the debug package.
type DebugConfiguration struct {

	// MutexProfileFraction is used to set the runtime.SetMutexProfileFraction to report mutex convention events.
	// See https://tip.golang.org/pkg/runtime/#SetMutexProfileFraction for more details about the values.
	MutexProfileFraction int `yaml:"mutex_profile_fraction"`
}

// SetMutexProfileFraction sets the configured mutex profile fraction for the runtime.
func (c DebugConfiguration) SetMutexProfileFraction() {
	runtime.SetMutexProfileFraction(c.MutexProfileFraction)
}
