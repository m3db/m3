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

package kv

import (
	"testing"

	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/generated/proto/commonpb"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/mem"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	gvalidator "gopkg.in/validator.v2"
	yaml "gopkg.in/yaml.v2"
)

func TestNamespaceValidatorConfiguration(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cfgStr := `
kvConfig:
  zone: testZone
  environment: testEnvironment
  namespace: testNamespace
validNamespacesKey: validNamespaces
`
	var cfg NamespaceValidatorConfiguration
	require.NoError(t, yaml.Unmarshal([]byte(cfgStr), &cfg))
	require.NoError(t, gvalidator.Validate(cfg))

	kvStore := mem.NewStore()
	initNamespaces := &commonpb.StringArrayProto{
		Values: []string{"foo", "bar", "baz"},
	}
	_, err := kvStore.SetIfNotExists("validNamespaces", initNamespaces)
	require.NoError(t, err)

	kvOpts := kv.NewOverrideOptions().
		SetZone("testZone").
		SetNamespace("testNamespace").
		SetEnvironment("testEnvironment")

	kvClient := client.NewMockClient(ctrl)
	kvClient.EXPECT().Store(kvOpts).Return(kvStore, nil)

	res, err := cfg.NewNamespaceValidator(kvClient)
	require.NoError(t, err)
	validator := res.(*validator)
	require.Equal(t, kvStore, validator.kvStore)
	require.Equal(t, "validNamespaces", validator.validNamespacesKey)
	require.Equal(t, defaultInitWatchTimeout, validator.initWatchTimeout)
	expectedValidNamespaces := map[string]struct{}{
		"foo": struct{}{},
		"bar": struct{}{},
		"baz": struct{}{},
	}
	require.Equal(t, expectedValidNamespaces, validator.validNamespaces)
}
