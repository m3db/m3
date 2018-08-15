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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	port     = 9000
	hostname = "0.0.0.0"
)

func TestListenAddressResolver(t *testing.T) {
	cfg := Configuration{
		Hostname: hostname,
		Port: &Port{
			PortType: ConfigResolver,
			Value:    &port,
		},
	}

	value, err := cfg.Resolve()
	require.NoError(t, err)

	assert.Equal(t, "0.0.0.0:9000", value)
}

func TestConfigResolverErrorWhenMissing(t *testing.T) {
	cfg := Configuration{
		Hostname: hostname,
	}

	_, err := cfg.Resolve()
	require.Error(t, err)
}

func TestEnvironmentVariableResolver(t *testing.T) {
	varName := "PORT_ENV_NAME"
	expected := "9000"

	require.NoError(t, os.Setenv(varName, expected))

	cfg := Configuration{
		Hostname: hostname,
		Port: &Port{
			PortType:   EnvironmentResolver,
			EnvVarName: &varName,
		},
	}

	value, err := cfg.Resolve()
	require.NoError(t, err)

	assert.Equal(t, "0.0.0.0:9000", value)
}

func TestInvalidEnvironmentVariableResolver(t *testing.T) {
	varName := "PORT_ENV_NAME"
	expected := "foo"

	require.NoError(t, os.Setenv(varName, expected))

	cfg := Configuration{
		Hostname: hostname,
		Port: &Port{
			PortType:   EnvironmentResolver,
			EnvVarName: &varName,
		},
	}

	_, err := cfg.Resolve()
	require.Error(t, err)
}

func TestEnvironmentResolverErrorWhenNameMissing(t *testing.T) {
	cfg := Configuration{
		Hostname: hostname,
		Port: &Port{
			PortType: EnvironmentResolver,
		},
	}

	_, err := cfg.Resolve()
	require.Error(t, err)
}

func TestEnvironmentResolverErrorWhenValueMissing(t *testing.T) {
	varName := "PORT_ENV_NAME_OTHER"

	cfg := Configuration{
		Hostname: hostname,
		Port: &Port{
			PortType:   EnvironmentResolver,
			EnvVarName: &varName,
		},
	}

	_, err := cfg.Resolve()
	require.Error(t, err)
}

func TestUnknownResolverError(t *testing.T) {
	cfg := Configuration{
		Hostname: hostname,
		Port: &Port{
			PortType: "some-unknown-resolver",
			Value:    &port,
		},
	}

	_, err := cfg.Resolve()
	require.Error(t, err)
}
