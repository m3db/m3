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

package storage

import (
	"testing"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"github.com/m3db/m3/src/query/ts"
	xerrors "github.com/m3db/m3/src/x/errors"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

func TestNewWriteQueryValidateTags(t *testing.T) {
	// Create known bad tags.
	tags := models.NewTags(0, models.NewTagOptions().
		SetIDSchemeType(models.TypeQuoted))
	tags = tags.AddTag(models.Tag{Name: []byte(""), Value: []byte("foo")})
	require.Error(t, tags.Validate())

	// Try to create a write query.
	_, err := NewWriteQuery(WriteQueryOptions{
		Tags: tags,
		Datapoints: ts.Datapoints{
			{
				Timestamp: xtime.Now(),
				Value:     42,
			},
		},
		Unit: xtime.Second,
		Attributes: storagemetadata.Attributes{
			MetricsType: storagemetadata.UnaggregatedMetricsType,
		},
	})
	require.Error(t, err)
	require.True(t, xerrors.IsInvalidParams(err))
}

func TestNewWriteQueryValidateDatapoints(t *testing.T) {
	// Try to create a write query.
	_, err := NewWriteQuery(WriteQueryOptions{
		Tags:       models.MustMakeTags("foo", "bar"),
		Datapoints: ts.Datapoints{},
		Unit:       xtime.Second,
		Attributes: storagemetadata.Attributes{
			MetricsType: storagemetadata.UnaggregatedMetricsType,
		},
	})
	require.Error(t, err)
	require.True(t, xerrors.IsInvalidParams(err))
}

func TestNewWriteQueryValidateUnit(t *testing.T) {
	// Try to create a write query.
	_, err := NewWriteQuery(WriteQueryOptions{
		Tags: models.MustMakeTags("foo", "bar"),
		Datapoints: ts.Datapoints{
			{
				Timestamp: xtime.Now(),
				Value:     42,
			},
		},
		Unit: xtime.Unit(999),
		Attributes: storagemetadata.Attributes{
			MetricsType: storagemetadata.UnaggregatedMetricsType,
		},
	})
	require.Error(t, err)
	require.True(t, xerrors.IsInvalidParams(err))
}
