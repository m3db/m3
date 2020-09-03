// Copyright (c) 2020 Uber Technologies, Inc.
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

package remote

import (
	"testing"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/stretchr/testify/assert"
)

func TestMapTags_Append(t *testing.T) {
	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: []byte("tag1"), Value: []byte("val1")},
					{Name: []byte("tag2"), Value: []byte("val1")},
					{Name: []byte("tag3"), Value: []byte("val4")},
				},
				Samples: []prompb.Sample{},
			},
			{
				Labels: []prompb.Label{
					{Name: []byte("tag1"), Value: []byte("val1")},
				},
				Samples: []prompb.Sample{},
			},
			{
				Labels:  []prompb.Label{},
				Samples: []prompb.Sample{},
			},
		},
	}

	opts := handleroptions.MapTagsOptions{
		TagMappers: []handleroptions.TagMapper{
			{Write: handleroptions.WriteOp{Tag: "tag1", Value: "val2"}},
			{Write: handleroptions.WriteOp{Tag: "tag2", Value: "val3"}},
		},
	}

	err := mapTags(req, opts)
	assert.NoError(t, err)

	exp := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: []byte("tag1"), Value: []byte("val2")},
					{Name: []byte("tag2"), Value: []byte("val3")},
					{Name: []byte("tag3"), Value: []byte("val4")},
				},
				Samples: []prompb.Sample{},
			},
			{
				Labels: []prompb.Label{
					{Name: []byte("tag1"), Value: []byte("val2")},
					{Name: []byte("tag2"), Value: []byte("val3")},
				},
				Samples: []prompb.Sample{},
			},
			{
				Labels: []prompb.Label{
					{Name: []byte("tag1"), Value: []byte("val2")},
					{Name: []byte("tag2"), Value: []byte("val3")},
				},
				Samples: []prompb.Sample{},
			},
		},
	}

	assert.Equal(t, exp, req)
}

func TestMapTags_Err(t *testing.T) {
	req := &prompb.WriteRequest{}
	opts := handleroptions.MapTagsOptions{
		TagMappers: []handleroptions.TagMapper{
			{
				Write: handleroptions.WriteOp{Tag: "tag1", Value: "val2"},
				Drop:  handleroptions.DropOp{Tag: "tag2"},
			},
		},
	}

	err := mapTags(req, opts)
	assert.Error(t, err)

	opts.TagMappers[0] = handleroptions.TagMapper{
		Drop: handleroptions.DropOp{Tag: "foo"},
	}
	err = mapTags(req, opts)
	assert.Error(t, err)

	opts.TagMappers[0] = handleroptions.TagMapper{
		Replace: handleroptions.ReplaceOp{Tag: "foo"},
	}
	err = mapTags(req, opts)
	assert.Error(t, err)
}
