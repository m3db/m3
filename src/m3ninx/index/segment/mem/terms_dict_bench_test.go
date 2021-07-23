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

package mem

import (
	"regexp"
	"testing"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/m3db/m3/src/m3ninx/util"
)

var (
	benchTermsDictField    = []byte("__name__")
	benchTermsDictRegexp   = []byte("node_netstat_Tcp_.*")
	benchTermsDictCompiled = regexp.MustCompile(string(benchTermsDictRegexp))
)

func BenchmarkTermsDict(b *testing.B) {
	benchmarks := []struct {
		name string
		fn   func(docs []doc.Metadata, b *testing.B)
	}{
		{
			name: "benchmark Insert",
			fn:   benchmarkTermsDictInsert,
		},
		{
			name: "benchmark MatchTerm",
			fn:   benchmarkTermsDictMatchTerm,
		},
		{
			name: "benchmark MatchRegex",
			fn:   benchmarkTermsDictMatchRegex,
		},
	}

	docs, err := util.ReadDocs("../../../util/testdata/node_exporter.json", 2000)
	if err != nil {
		b.Fatalf("unable to read documents for benchmarks: %v", err)
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			bm.fn(docs, b)
		})
	}
}

func benchmarkTermsDictInsert(docs []doc.Metadata, b *testing.B) {
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		b.StopTimer()
		dict := newTermsDict(NewOptions())
		b.StartTimer()
		for i, d := range docs {
			for _, f := range d.Fields {
				dict.Insert(f, postings.ID(i))
			}
		}
	}
}

func benchmarkTermsDictMatchTerm(docs []doc.Metadata, b *testing.B) {
	b.ReportAllocs()

	dict := newTermsDict(NewOptions())
	for i, d := range docs {
		for _, f := range d.Fields {
			dict.Insert(f, postings.ID(i))
		}
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for _, d := range docs {
			for _, f := range d.Fields {
				dict.MatchTerm(f.Name, f.Value)
			}
		}
	}
}

func benchmarkTermsDictMatchRegex(docs []doc.Metadata, b *testing.B) {
	b.ReportAllocs()

	dict := newTermsDict(NewOptions())
	for i, d := range docs {
		for _, f := range d.Fields {
			dict.Insert(f, postings.ID(i))
		}
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		dict.MatchRegexp(benchTermsDictField, benchTermsDictCompiled)
	}
}
