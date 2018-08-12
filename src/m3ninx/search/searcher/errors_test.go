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

package searcher

import (
	"testing"

	"github.com/m3db/m3/src/m3ninx/search"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestValidateSearchers(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	firstTestSearcher1 := search.NewMockSearcher(mockCtrl)
	firstTestSearcher2 := search.NewMockSearcher(mockCtrl)
	firstTestSeachers := search.Searchers{firstTestSearcher1, firstTestSearcher2}

	secondTestSearcher1 := search.NewMockSearcher(mockCtrl)
	secondTestSearcher2 := search.NewMockSearcher(mockCtrl)
	secondTestSeachers := search.Searchers{secondTestSearcher1, secondTestSearcher2}

	gomock.InOrder(
		// All searchers have the same number of readers in the first test.
		firstTestSearcher1.EXPECT().NumReaders().Return(3),
		firstTestSearcher2.EXPECT().NumReaders().Return(3),

		// The searchers do not all have the same number of readers in the second test.
		secondTestSearcher1.EXPECT().NumReaders().Return(3),
		secondTestSearcher2.EXPECT().NumReaders().Return(4),
	)

	tests := []struct {
		name       string
		numReaders int
		searchers  search.Searchers
		expectErr  bool
	}{
		{
			name:       "valid",
			numReaders: 3,
			searchers:  firstTestSeachers,
			expectErr:  false,
		},
		{
			name:       "invalid",
			numReaders: 3,
			searchers:  secondTestSeachers,
			expectErr:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateSearchers(test.numReaders, test.searchers)
			if test.expectErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}
