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

// Package listenaddress provides a configuration struct for resolving
// a listen address from YAML.
package listenaddress

import (
	"errors"
	"fmt"
	"os"
	"strconv"
)

// Resolver is a type of port resolver
type Resolver string

const (
	// ConfigResolver resolves port using a value provided in config
	ConfigResolver Resolver = "config"
	// EnvironmentResolver resolves port using an environment variable
	// of which the name is provided in config
	EnvironmentResolver Resolver = "environment"
)

// Configuration is the configuration for resolving a listen address.
type Configuration struct {
	// Hostname is the hostname to use
	Hostname string `yaml:"hostname" validate:"nonzero"`

	// Port specifies the port to listen on
	Port *Port `yaml:"port" validate:"nonzero"`
}

// Port specifies the port to use
type Port struct {
	// PortType is the port type for the port
	PortType Resolver `yaml:"portType" validate:"nonzero"`

	// Value is the config specified port if using config port type.
	Value *int `yaml:"value"`

	// EnvVarName is the environment specified port if using environment port type.
	EnvVarName *string `yaml:"envVarName"`
}

// Resolve returns the resolved listen address given the configuration.
func (c Configuration) Resolve() (string, error) {
	if c.Port == nil {
		return "", errors.New("missing port config")
	}
	p := c.Port

	var port int
	switch p.PortType {
	case ConfigResolver:
		if p.Value == nil {
			err := fmt.Errorf("missing port type using: resolver=%s",
				string(p.PortType))
			return "", err
		}
		port = *p.Value
	case EnvironmentResolver:
		if p.EnvVarName == nil {
			err := fmt.Errorf("missing port env var name using: resolver=%s",
				string(p.PortType))
			return "", err
		}
		portStr := os.Getenv(*p.EnvVarName)
		var err error
		port, err = strconv.Atoi(portStr)
		if err != nil {
			err := fmt.Errorf("invalid port env var value using: resolver=%s, name=%s",
				string(p.PortType), *p.EnvVarName)
			return "", err
		}
	default:
		return "", fmt.Errorf("unknown port type: resolver=%s",
			string(p.PortType))
	}

	return fmt.Sprintf("%s:%d", c.Hostname, port), nil
}
