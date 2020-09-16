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
	"bytes"
	"errors"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
)

// mapTags modifies a given write request based on the tag mappers passed.
func mapTags(req *prompb.WriteRequest, opts handleroptions.MapTagsOptions) error {
	for _, mapper := range opts.TagMappers {
		if err := mapper.Validate(); err != nil {
			return err
		}

		if op := mapper.Write; !op.IsEmpty() {
			tag := []byte(op.Tag)
			value := []byte(op.Value)

			for i, ts := range req.Timeseries {
				replaced := false
				for j, l := range ts.Labels {
					if bytes.Equal(l.Name, tag) {
						ts.Labels[j].Value = value
						replaced = true
					}
				}

				if !replaced {
					// No existing labels with this tag, append it.
					req.Timeseries[i].Labels = append(ts.Labels, prompb.Label{
						Name:  tag,
						Value: value,
					})
				}
			}
		}

		if op := mapper.Drop; !op.IsEmpty() {
			return errors.New("Drop operation is not yet supported")
		}

		if op := mapper.DropWithValue; !op.IsEmpty() {
			return errors.New("DropWithValue operation is not yet supported")
		}

		if op := mapper.Replace; !op.IsEmpty() {
			return errors.New("Replace operation is not yet supported")
		}
	}

	return nil
}
