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

package deploy

import (
	"net/http"
	"testing"

	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3cluster/services/placement"

	"github.com/stretchr/testify/require"
)

func TestValidateNoPlannerOptions(t *testing.T) {
	opts := testHelperOptions().SetPlannerOptions(nil)
	require.Equal(t, errNoPlannerOptions, opts.Validate())
}

func TestValidateNoManager(t *testing.T) {
	opts := testHelperOptions().SetManager(nil)
	require.Equal(t, errNoManager, opts.Validate())
}

func TestValidateNoHTTPClient(t *testing.T) {
	opts := testHelperOptions().SetHTTPClient(nil)
	require.Equal(t, errNoHTTPClient, opts.Validate())
}

func TestValidateNoKVStore(t *testing.T) {
	opts := testHelperOptions().SetKVStore(nil)
	require.Equal(t, errNoKVStore, opts.Validate())
}

func TestValidateNoToPlacementInstanceIDFn(t *testing.T) {
	opts := testHelperOptions().SetToPlacementInstanceIDFn(nil)
	require.Equal(t, errNoToPlacementInstanceIDFn, opts.Validate())
}

func TestValidateNoToAPIEndpointFn(t *testing.T) {
	opts := testHelperOptions().SetToAPIEndpointFn(nil)
	require.Equal(t, errNoToAPIEndpointFn, opts.Validate())
}

func TestValidateNoStagedPlacementWatcherOptions(t *testing.T) {
	opts := testHelperOptions().SetStagedPlacementWatcherOptions(nil)
	require.Equal(t, errNoStagedPlacementWatcherOptions, opts.Validate())
}

func testHelperOptions() HelperOptions {
	return NewHelperOptions().
		SetPlannerOptions(NewPlannerOptions()).
		SetManager(&mockManager{}).
		SetHTTPClient(&http.Client{}).
		SetKVStore(mem.NewStore()).
		SetStagedPlacementWatcherOptions(placement.NewStagedPlacementWatcherOptions()).
		SetToPlacementInstanceIDFn(func(deploymentInstanceID string) (string, error) {
			return "", nil
		}).
		SetToAPIEndpointFn(func(placementEndpoint string) (string, error) {
			return "", nil
		})
}
