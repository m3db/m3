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
//
// Deprecation notice:
// The environment resolution behavior of this
// class has largely been superseded
// by config environment interpolation in go.uber.org/config (see config/README.md for
// details). Instead of:
//
//     listenAddress:
//       type: environment
//       envVarListenPort: MY_PORT_VAR
//
//  one can do:
//
//     listenAddress:
//       value: 0.0.0.0:${MY_PORT_VAR}
package listenaddress

import (
	"fmt"
	"os"
	"strconv"
)

const (
	defaultHostname = "0.0.0.0"
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
	// DeprecatedListenAddressType is the port type for the port
	// DEPRECATED: use config interpolation with `value` (config/README.md)
	DeprecatedListenAddressType Resolver `yaml:"type"`

	// Value is the config specified listen address if using config port type.
	Value *string `yaml:"value"`

	// EnvVarListenPort specifies the environment variable name for the listen address port.
	// DEPRECATED: use config interpolation with `value` (config/README.md)
	DeprecatedEnvVarListenPort *string `yaml:"envVarListenPort"`

	// DeprecatedEnvVarListenHost specifies the environment variable name for the listen address hostname.
	// DEPRECATED: use config interpolation with `value` (config/README.md)
	DeprecatedEnvVarListenHost *string `yaml:"envVarListenHost"`
}

// Resolve returns the resolved listen address given the configuration.
func (c Configuration) Resolve() (string, error) {
	listenAddrType := c.DeprecatedListenAddressType

	if listenAddrType == "" {
		// Default to ConfigResolver
		listenAddrType = ConfigResolver
	}

	var listenAddress string
	switch listenAddrType {
	case ConfigResolver:
		if c.Value == nil {
			err := fmt.Errorf("missing listen address value using: resolver=%s",
				string(listenAddrType))
			return "", err
		}
		listenAddress = *c.Value

	case EnvironmentResolver:
		// environment variable for port is required
		if c.DeprecatedEnvVarListenPort == nil {
			err := fmt.Errorf("missing port env var name using: resolver=%s",
				string(listenAddrType))
			return "", err
		}
		portStr := os.Getenv(*c.DeprecatedEnvVarListenPort)
		port, err := strconv.Atoi(portStr)
		if err != nil {
			err := fmt.Errorf("invalid port env var value using: resolver=%s, name=%s",
				string(listenAddrType), *c.DeprecatedEnvVarListenPort)
			return "", err
		}
		// if environment variable for hostname is not set, use the default
		if c.DeprecatedEnvVarListenHost == nil {
			listenAddress = fmt.Sprintf("%s:%d", defaultHostname, port)
		} else {
			envHost := os.Getenv(*c.DeprecatedEnvVarListenHost)
			listenAddress = fmt.Sprintf("%s:%d", envHost, port)
		}

	default:
		return "", fmt.Errorf("unknown listen address type: resolver=%s",
			string(listenAddrType))
	}

	return listenAddress, nil
}
