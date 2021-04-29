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
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if tc.nameTag == "" {
				tc.nameTag = nameTag
			}
			encoder := &serialize.FakeTagEncoder{}
			p := newRollupIDProvider(encoder, nil, ident.BytesID(tc.nameTag))
			rollupID, err := p.provide([]byte(tc.metricName), tc.tags)
			require.NoError(t, err)
			encoded, _ := encoder.Data()
			require.Equal(t, rollupID, encoded.Bytes())
			require.Equal(t, tc.expectedTags, encoder.Decode())
		})
	}
}
