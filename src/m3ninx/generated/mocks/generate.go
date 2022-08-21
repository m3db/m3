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

package mocks

// mockgen rules for generating mocks (file mode)
//go:generate sh -c "mockgen -package=postings -destination=../../postings/postings_mock.go -source=../../postings/types.go"
//go:generate sh -c "mockgen -package=doc -destination=../../doc/doc_mock.go -source=../../doc/types.go"
//go:generate sh -c "mockgen -package=search -destination=../../search/search_mock.go -source=../../search/types.go"
//go:generate sh -c "mockgen -package=persist -destination=../../persist/persist_mock.go -source=../../persist/types.go"
//go:generate sh -c "mockgen -package=segment -destination=../../index/segment/segment_mock.go -source=../../index/segment/types.go"

// mockgen rules for generating mocks (reflection mode)
//go:generate sh -c "mockgen -package=mem -destination=../../index/segment/mem/mem_mock.go github.com/m3db/m3/src/m3ninx/index/segment/mem ReadableSegment"
//go:generate sh -c "mockgen -package=fst -destination=../../index/segment/fst/fst_mock.go github.com/m3db/m3/src/m3ninx/index/segment/fst Writer,Segment"
//go:generate sh -c "mockgen -package=index -destination=../../index/index_mock.go github.com/m3db/m3/src/m3ninx/index Reader,DocRetriever,MetadataRetriever"
