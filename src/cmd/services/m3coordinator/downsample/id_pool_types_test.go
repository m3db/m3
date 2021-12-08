// Copyright (c) 2021 Uber Technologies, Inc.
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

package downsample

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/serialize"
)

//nolint: dupl
func TestRollupIdProvider(t *testing.T) {
	cases := []struct {
		name         string
		nameTag      string
		metricName   string
		tags         []id.TagPair
		expectedTags []id.TagPair
	}{
		{
			name:       "name first",
			metricName: "http_requests",
			tags: []id.TagPair{
				{
					Name:  []byte("bar"),
					Value: []byte("barValue"),
				},
				{
					Name:  []byte("foo"),
					Value: []byte("fooValue"),
				},
			},
			expectedTags: []id.TagPair{
				{
					Name:  []byte("__name__"),
					Value: []byte("http_requests"),
				},
				{
					Name:  []byte("__rollup__"),
					Value: []byte("true"),
				},
				{
					Name:  []byte("bar"),
					Value: []byte("barValue"),
				},
				{
					Name:  []byte("foo"),
					Value: []byte("fooValue"),
				},
			},
		},
		{
			name:       "rollup first",
			nameTag:    "__sAfterRollupName__",
			metricName: "http_requests",
			tags: []id.TagPair{
				{
					Name:  []byte("bar"),
					Value: []byte("barValue"),
				},
				{
					Name:  []byte("foo"),
					Value: []byte("fooValue"),
				},
			},
			expectedTags: []id.TagPair{
				{
					Name:  []byte("__rollup__"),
					Value: []byte("true"),
				},
				{
					Name:  []byte("__sAfterRollupName__"),
					Value: []byte("http_requests"),
				},
				{
					Name:  []byte("bar"),
					Value: []byte("barValue"),
				},
				{
					Name:  []byte("foo"),
					Value: []byte("fooValue"),
				},
			},
		},
		{
			name:       "custom tags first",
			metricName: "http_requests",
			tags: []id.TagPair{
				{
					Name:  []byte("__bar__"),
					Value: []byte("barValue"),
				},
				{
					Name:  []byte("__foo__"),
					Value: []byte("fooValue"),
				},
			},
			expectedTags: []id.TagPair{
				{
					Name:  []byte("__bar__"),
					Value: []byte("barValue"),
				},
				{
					Name:  []byte("__foo__"),
					Value: []byte("fooValue"),
				},
				{
					Name:  []byte("__name__"),
					Value: []byte("http_requests"),
				},
				{
					Name:  []byte("__rollup__"),
					Value: []byte("true"),
				},
			},
		},
		{
			name:       "only name first",
			metricName: "http_requests",
			tags: []id.TagPair{
				{
					Name:  []byte("__oAfterName__"),
					Value: []byte("barValue"),
				},
				{
					Name:  []byte("foo"),
					Value: []byte("fooValue"),
				},
			},
			expectedTags: []id.TagPair{
				{
					Name:  []byte("__name__"),
					Value: []byte("http_requests"),
				},
				{
					Name:  []byte("__oAfterName__"),
					Value: []byte("barValue"),
				},
				{
					Name:  []byte("__rollup__"),
					Value: []byte("true"),
				},
				{
					Name:  []byte("foo"),
					Value: []byte("fooValue"),
				},
			},
		},
		{
			name:       "only rollup first",
			nameTag:    "cBar",
			metricName: "http_requests",
			tags: []id.TagPair{
				{
					Name:  []byte("bar"),
					Value: []byte("barValue"),
				},
				{
					Name:  []byte("foo"),
					Value: []byte("fooValue"),
				},
			},
			expectedTags: []id.TagPair{
				{
					Name:  []byte("__rollup__"),
					Value: []byte("true"),
				},
				{
					Name:  []byte("bar"),
					Value: []byte("barValue"),
				},
				{
					Name:  []byte("cBar"),
					Value: []byte("http_requests"),
				},
				{
					Name:  []byte("foo"),
					Value: []byte("fooValue"),
				},
			},
		},
		{
			name:       "name last",
			nameTag:    "zzzName",
			metricName: "http_requests",
			tags: []id.TagPair{
				{
					Name:  []byte("__bar__"),
					Value: []byte("barValue"),
				},
				{
					Name:  []byte("foo"),
					Value: []byte("fooValue"),
				},
			},
			expectedTags: []id.TagPair{
				{
					Name:  []byte("__bar__"),
					Value: []byte("barValue"),
				},
				{
					Name:  []byte("__rollup__"),
					Value: []byte("true"),
				},
				{
					Name:  []byte("foo"),
					Value: []byte("fooValue"),
				},
				{
					Name:  []byte("zzzName"),
					Value: []byte("http_requests"),
				},
			},
		},
		{
			name:       "rollup last",
			nameTag:    "__cBar__",
			metricName: "http_requests",
			tags: []id.TagPair{
				{
					Name:  []byte("__bar__"),
					Value: []byte("barValue"),
				},
				{
					Name:  []byte("__foo__"),
					Value: []byte("fooValue"),
				},
			},
			expectedTags: []id.TagPair{
				{
					Name:  []byte("__bar__"),
					Value: []byte("barValue"),
				},
				{
					Name:  []byte("__cBar__"),
					Value: []byte("http_requests"),
				},
				{
					Name:  []byte("__foo__"),
					Value: []byte("fooValue"),
				},
				{
					Name:  []byte("__rollup__"),
					Value: []byte("true"),
				},
			},
		},
		{
			name:       "rollup and name already exists",
			nameTag:    "__name__",
			metricName: "http_requests",
			tags: []id.TagPair{
				{
					Name:  []byte("__name__"),
					Value: []byte("http_requests"),
				},
				{
					Name:  []byte("__rollup__"),
					Value: []byte("true"),
				},
				{
					Name:  []byte("foo"),
					Value: []byte("fooValue"),
				},
			},
			expectedTags: []id.TagPair{
				{
					Name:  []byte("__name__"),
					Value: []byte("http_requests"),
				},
				{
					Name:  []byte("__rollup__"),
					Value: []byte("true"),
				},
				{
					Name:  []byte("foo"),
					Value: []byte("fooValue"),
				},
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if tc.nameTag == "" {
				tc.nameTag = nameTag
			}
			encoder := &serialize.FakeTagEncoder{}
			p := newRollupIDProvider(encoder, nil, ident.BytesID(tc.nameTag))
			p.reset([]byte(tc.metricName), tc.tags)
			require.Equal(t, len(tc.expectedTags), p.Len())
			curIdx := 0
			for p.Next() {
				require.Equal(t, curIdx, p.CurrentIndex())
				curIdx++
				require.Equal(t, p.Len()-curIdx, p.Remaining())
			}
			p.Rewind()
			curIdx = 0
			for p.Next() {
				require.Equal(t, curIdx, p.CurrentIndex())
				curIdx++
			}
			rollupID, err := p.provide([]byte(tc.metricName), tc.tags)
			require.NoError(t, err)
			encoded, _ := encoder.Data()
			require.Equal(t, rollupID, encoded.Bytes())
			require.Equal(t, tc.expectedTags, encoder.Decode())
		})
	}
}
