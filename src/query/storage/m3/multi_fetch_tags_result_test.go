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

package m3

import (
	"testing"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/query/block"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestExhaustiveTagMerge(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	r := NewMultiFetchTagsResult()
	for _, tt := range exhaustTests {
		t.Run(tt.name, func(t *testing.T) {
			for _, ex := range tt.exhaustives {
				it := client.NewMockTaggedIDsIterator(ctrl)
				it.EXPECT().Next().Return(false)
				it.EXPECT().Err().Return(nil)
				it.EXPECT().Finalize().Return()
				meta := block.NewResultMetadata()
				meta.Exhaustive = ex
				r.Add(it, meta, nil)
			}

			tagResult, err := r.FinalResult()
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, tagResult.Metadata.Exhaustive)
			assert.NoError(t, r.Close())
		})
	}
}
