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

var opts = Options{
	endpoints: []EndpointOptions{
		{
			name:          "raw",
			resolution:    0,
			retention:     0,
			downsampleAll: false,
		},
		{
			name:          "downsampled1",
			retention:     time.Second,
			resolution:    time.Millisecond,
			downsampleAll: true,
		},
		{
			name:          "downsampled2",
			retention:     time.Minute,
			resolution:    time.Hour,
			downsampleAll: false,
		},
	},
}

func TestNamespaces(t *testing.T) {
	ns := opts.Namespaces()
	downsampleTrue := true
	downsampleFalse := false

	assertNamespace(expectation{
		t:          t,
		ns:         ns[0],
		expectedID: "raw",
		expectedAttributes: storagemetadata.Attributes{
			Retention:   0,
			Resolution:  0,
			MetricsType: storagemetadata.UnaggregatedMetricsType,
		},
		expectedDownsample: nil,
	})

	assertNamespace(expectation{
		t:          t,
		ns:         ns[1],
		expectedID: "downsampled1",
		expectedAttributes: storagemetadata.Attributes{
			Retention:   time.Second,
			Resolution:  time.Millisecond,
			MetricsType: storagemetadata.AggregatedMetricsType,
		},
		expectedDownsample: &downsampleTrue,
	})

	assertNamespace(expectation{
		t:          t,
		ns:         ns[2],
		expectedID: "downsampled2",
		expectedAttributes: storagemetadata.Attributes{
			Retention:   time.Minute,
			Resolution:  time.Hour,
			MetricsType: storagemetadata.AggregatedMetricsType,
		},
		expectedDownsample: &downsampleFalse,
	})
}

func TestNewSessionPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Errorf("NewSession must panic")
		}
	}()

	opts.Namespaces()[0].Session()
}

type expectation struct {
	t                  *testing.T
	ns                 m3.ClusterNamespace
	expectedID         string
	expectedAttributes storagemetadata.Attributes
	expectedDownsample *bool
}

func assertNamespace(e expectation) {
	assert.Equal(e.t, ident.StringID(e.expectedID), e.ns.NamespaceID())
	assert.Equal(e.t, e.expectedAttributes, e.ns.Options().Attributes())
	if e.expectedDownsample != nil {
		ds, err := e.ns.Options().DownsampleOptions()
		require.NoError(e.t, err)
		assert.Equal(e.t, *e.expectedDownsample, ds.All)
	} else {
		_, err := e.ns.Options().DownsampleOptions()
		require.Error(e.t, err)
	}
}
