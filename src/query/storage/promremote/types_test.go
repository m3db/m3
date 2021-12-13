// Copyright (c) 2021  Uber Technologies, Inc.
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

package promremote

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"github.com/m3db/m3/src/x/ident"
)

func TestNamespaces(t *testing.T) {
	tcs := []struct {
		name     string
		endpoint EndpointOptions
	}{
		{
			name: "raw",
			endpoint: EndpointOptions{
				name: "raw",
				attributes: storagemetadata.Attributes{
					MetricsType: storagemetadata.UnaggregatedMetricsType,
				},
			},
		},
		{
			name: "donwsampled",
			endpoint: EndpointOptions{
				name: "downsampled",
				attributes: storagemetadata.Attributes{
					MetricsType: storagemetadata.AggregatedMetricsType,
					Retention:   time.Second,
					Resolution:  time.Millisecond,
				},
				downsampleOptions: &m3.ClusterNamespaceDownsampleOptions{All: true},
			},
		},
		{
			name: "donwsampled all false",
			endpoint: EndpointOptions{
				name: "downsampled",
				attributes: storagemetadata.Attributes{
					MetricsType: storagemetadata.AggregatedMetricsType,
					Retention:   time.Second,
					Resolution:  time.Millisecond,
				},
				downsampleOptions: &m3.ClusterNamespaceDownsampleOptions{All: false},
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			opts := Options{
				endpoints: []EndpointOptions{tc.endpoint},
			}
			nss := opts.Namespaces()
			require.Len(t, nss, 1)

			ns := nss[0]
			assert.Equal(t, ident.StringID(tc.endpoint.name), ns.NamespaceID())
			assert.Equal(t, tc.endpoint.attributes, ns.Options().Attributes())

			ds, err := ns.Options().DownsampleOptions()
			if tc.endpoint.downsampleOptions != nil {
				assert.NoError(t, err)
				assert.Equal(t, *tc.endpoint.downsampleOptions, ds)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestNewSessionPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Errorf("NewSession must panic")
		}
	}()

	opts := Options{endpoints: []EndpointOptions{{name: "raw"}}}
	opts.Namespaces()[0].Session()
}
