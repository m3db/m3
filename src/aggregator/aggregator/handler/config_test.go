// Copyright (c) 2017 Uber Technologies, Inc.
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

package handler

import (
	"testing"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestStoragePolicyFilter(t *testing.T) {
	var cfg FlushHandlerConfiguration

	str := `
dynamicBackend:
  name: test
  storagePolicyFilters:
    - serviceID:
        name: name1
        environment: env1
        zone: zone1
      storagePolicies:
        - 10m:40d
        - 1m:40d
`
	require.NoError(t, yaml.Unmarshal([]byte(str), &cfg))
	require.Equal(t, 1, len(cfg.DynamicBackend.StoragePolicyFilters))
	require.Equal(t, 2, len(cfg.DynamicBackend.StoragePolicyFilters[0].StoragePolicies))
}

func TestFlushHandlerConfigurationValidate(t *testing.T) {
	var cfg FlushHandlerConfiguration

	neitherConfigured := ``
	require.NoError(t, yaml.Unmarshal([]byte(neitherConfigured), &cfg))
	err := cfg.Validate()
	require.Error(t, err)
	require.Equal(t, errNoDynamicOrStaticBackendConfiguration, err)

	bothConfigured := `
staticBackend:
  type: blackhole
dynamicBackend:
  name: test
`
	require.NoError(t, yaml.Unmarshal([]byte(bothConfigured), &cfg))
	err = cfg.Validate()
	require.Error(t, err)
	require.Equal(t, errBothDynamicAndStaticBackendConfiguration, err)
}
