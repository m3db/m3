// Copyright (c) 2019 Uber Technologies, Inc.
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

package handleroptions

import (
	"net/http"
	"testing"
	"time"

	"github.com/m3db/m3/src/x/headers"

	"github.com/stretchr/testify/assert"
)

func TestNewServiceOptions(t *testing.T) {
	tests := []struct {
		service string
		headers map[string]string
		aggOpts *M3AggServiceOptions
		exp     ServiceOptions
	}{
		{
			service: "foo",
			exp: ServiceOptions{
				ServiceName:        "foo",
				ServiceEnvironment: headers.DefaultServiceEnvironment,
				ServiceZone:        headers.DefaultServiceZone,
				M3Agg: &M3AggServiceOptions{
					MaxAggregationWindowSize: time.Minute,
				},
			},
		},
		{
			service: "foo",
			headers: map[string]string{
				headers.HeaderClusterEnvironmentName: "bar",
				headers.HeaderClusterZoneName:        "baz",
				headers.HeaderDryRun:                 "true",
			},
			aggOpts: &M3AggServiceOptions{
				MaxAggregationWindowSize: 2 * time.Minute,
				WarmupDuration:           time.Minute,
			},
			exp: ServiceOptions{
				ServiceName:        "foo",
				ServiceEnvironment: "bar",
				ServiceZone:        "baz",
				DryRun:             true,
				M3Agg: &M3AggServiceOptions{
					MaxAggregationWindowSize: 2 * time.Minute,
					WarmupDuration:           time.Minute,
				},
			},
		},
	}

	for _, test := range tests {
		h := http.Header{}
		for k, v := range test.headers {
			h.Add(k, v)
		}
		svcDefaults := ServiceNameAndDefaults{
			ServiceName: test.service,
		}
		opts := NewServiceOptions(svcDefaults, h, test.aggOpts)
		assert.Equal(t, test.exp, opts)
	}
}

func TestServiceOptionsValidate(t *testing.T) {
	opts := &ServiceOptions{}
	assert.Error(t, opts.Validate())
	opts.ServiceName = "foo"
	assert.Error(t, opts.Validate())
	opts.ServiceEnvironment = "foo"
	assert.Error(t, opts.Validate())
	opts.ServiceZone = "foo"
	assert.NoError(t, opts.Validate())

	opts.ServiceName = M3AggregatorServiceName
	assert.Error(t, opts.Validate())

	opts.M3Agg = &M3AggServiceOptions{}
	assert.NoError(t, opts.Validate())
}
