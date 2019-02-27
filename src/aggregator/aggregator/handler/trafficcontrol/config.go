// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package trafficcontrol

import (
	"fmt"
	"time"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3x/instrument"
)

// Type defines the type of the traffic controller.
type Type string

var (
	// TrafficEnabler enables the traffic when the runtime value equals to true.
	TrafficEnabler Type = "trafficEnabler"
	// TrafficDisabler disables the traffic when the runtime value equals to true.
	TrafficDisabler Type = "trafficDisabler"

	validTypes = []Type{
		TrafficEnabler,
		TrafficDisabler,
	}
)

// UnmarshalYAML unmarshals TrafficControllerType from yaml.
func (t *Type) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}
	var validStrings []string
	for _, validType := range validTypes {
		validString := string(validType)
		if validString == str {
			*t = validType
			return nil
		}
		validStrings = append(validStrings, validString)
	}

	return fmt.Errorf("invalid traffic controller type %s, valid types are: %v", str, validStrings)
}

// Configuration configures the traffic controller.
type Configuration struct {
	Type         Type           `yaml:"type"`
	DefaultValue *bool          `yaml:"defaultValue"`
	RuntimeKey   string         `yaml:"runtimeKey" validate:"nonzero"`
	InitTimeout  *time.Duration `yaml:"initTimeout"`
}

// NewTrafficController creates a new traffic controller.
func (c *Configuration) NewTrafficController(
	store kv.Store,
	instrumentOpts instrument.Options,
) (Controller, error) {
	opts := NewOptions().
		SetStore(store).
		SetRuntimeKey(c.RuntimeKey).
		SetInstrumentOptions(instrumentOpts)
	if c.DefaultValue != nil {
		opts = opts.SetDefaultValue(*c.DefaultValue)
	}
	if c.InitTimeout != nil {
		opts = opts.SetInitTimeout(*c.InitTimeout)
	}
	var tc Controller
	if c.Type == TrafficEnabler {
		tc = NewTrafficEnabler(opts)
	} else {
		tc = NewTrafficDisabler(opts)
	}
	if err := tc.Init(); err != nil {
		return nil, err
	}
	return tc, nil
}
