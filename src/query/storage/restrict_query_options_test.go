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

package storage

import (
	"testing"

	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"

	"github.com/stretchr/testify/require"
)

func TestGetRestrict(t *testing.T) {
	var opts *RestrictQueryOptions
	require.Nil(t, opts.GetRestrictByTag())
	require.Nil(t, opts.GetRestrictByType())
	require.Nil(t, opts.GetRestrictByTypes())

	opts = &RestrictQueryOptions{}
	require.Nil(t, opts.GetRestrictByTag())
	require.Nil(t, opts.GetRestrictByType())
	require.Nil(t, opts.GetRestrictByTypes())

	opts.RestrictByTag = &RestrictByTag{}
	require.NotNil(t, opts.GetRestrictByTag())

	matcher, err := models.NewMatcher(models.MatchEqual, []byte("f"), []byte("b"))
	require.NoError(t, err)
	matchers := models.Matchers{matcher}

	opts.RestrictByTag.Restrict = matchers
	require.Equal(t, matchers, opts.GetRestrictByTag().GetMatchers())

	byType := &RestrictByType{}
	opts.RestrictByType = byType
	require.Equal(t, byType, opts.GetRestrictByType())

	byTypes := []*RestrictByType{
		{
			MetricsType:   storagemetadata.AggregatedMetricsType,
			StoragePolicy: policy.MustParseStoragePolicy("10s:4d"),
		},
	}
	opts.RestrictByTypes = byTypes
	require.Equal(t, byTypes, opts.GetRestrictByTypes())
}
