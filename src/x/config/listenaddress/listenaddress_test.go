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
	envListenPort = "ENV_LISTEN_PORT"
	envListenHost = "ENV_LISTEN_HOST"
	defaultListen = "0.0.0.0:9000"
)

func TestListenAddressResolver(t *testing.T) {
	cfg := Configuration{
		DeprecatedListenAddressType: ConfigResolver,
		Value:                       &defaultListen,
	}

	value, err := cfg.Resolve()
	require.NoError(t, err)

	assert.Equal(t, defaultListen, value)
}

func TestListenAddressResolverDefaultsToConfigResolver(t *testing.T) {
	cfg := Configuration{
		Value: &defaultListen,
	}

	value, err := cfg.Resolve()
	require.NoError(t, err)

	assert.Equal(t, defaultListen, value)
}

func TestConfigResolverErrorWhenMissing(t *testing.T) {
	cfg := Configuration{
		DeprecatedListenAddressType: ConfigResolver,
	}

	_, err := cfg.Resolve()
	require.Error(t, err)
}

func TestEnvironmentVariableResolverWithDefault(t *testing.T) {
	envPort := "9000"

	require.NoError(t, os.Setenv(envListenPort, envPort))

	cfg := Configuration{
		DeprecatedListenAddressType: EnvironmentResolver,
		DeprecatedEnvVarListenPort:  &envListenPort,
	}

	value, err := cfg.Resolve()
	require.NoError(t, err)

	assert.Equal(t, defaultListen, value)
}

func TestEnvironmentVariableResolver(t *testing.T) {
	envHost := "127.0.0.1"
	envPort := "9000"

	require.NoError(t, os.Setenv(envListenPort, envPort))
	require.NoError(t, os.Setenv(envListenHost, envHost))

	cfg := Configuration{
		DeprecatedListenAddressType: EnvironmentResolver,
		DeprecatedEnvVarListenPort:  &envListenPort,
		DeprecatedEnvVarListenHost:  &envListenHost,
	}

	value, err := cfg.Resolve()
	require.NoError(t, err)

	assert.Equal(t, "127.0.0.1:9000", value)
}

func TestInvalidEnvironmentVariableResolver(t *testing.T) {
	varName := "BAD_LISTEN_ENV_PORT"
	expected := "foo"

	require.NoError(t, os.Setenv(varName, expected))

	cfg := Configuration{
		DeprecatedListenAddressType: EnvironmentResolver,
		DeprecatedEnvVarListenPort:  &varName,
	}

	_, err := cfg.Resolve()
	require.Error(t, err)
}

func TestEnvironmentResolverErrorWhenNameMissing(t *testing.T) {
	cfg := Configuration{
		DeprecatedListenAddressType: EnvironmentResolver,
	}

	_, err := cfg.Resolve()
	require.Error(t, err)
}

func TestEnvironmentResolverErrorWhenValueMissing(t *testing.T) {
	varName := "OTHER_LISTEN_ENV_PORT"

	cfg := Configuration{
		DeprecatedListenAddressType: EnvironmentResolver,
		DeprecatedEnvVarListenPort:  &varName,
	}

	_, err := cfg.Resolve()
	require.Error(t, err)
}

func TestUnknownResolverError(t *testing.T) {
	cfg := Configuration{
		DeprecatedListenAddressType: "some-unknown-resolver",
	}

	_, err := cfg.Resolve()
	require.Error(t, err)
}
