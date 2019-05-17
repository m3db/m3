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

package docs

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/m3db/m3/src/m3ninx/util"

	"github.com/stretchr/testify/require"
)

var (
	testDocuments = util.MustReadDocs("../../../../../util/testdata/node_exporter.json", 2000)
)

func TestCompressionWriterAllocs(t *testing.T) {
	w, err := newCompressedDataWriter(CompressionOptions{
		Type:     DeflateCompressionType,
		PageSize: 1 << 14,
	}, nil)
	require.NoError(t, err)
	w.Reset(ioutil.Discard)
	_, err = w.Write(testDocuments[0])
	require.NoError(t, err)
	require.NoError(t, w.Flush())

	w.Reset(ioutil.Discard)
	_, err = w.Write(testDocuments[0])
	require.NoError(t, err)
	require.NoError(t, w.Flush())
}

/*
$ go test ./src/m3ninx/index/segment/fst/encoding/docs -run 'Benchmark' -bench=BenchmarkWriteCompressedStoredFieldsData -v
goos: darwin
goarch: amd64
pkg: github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding/docs
BenchmarkWriteCompressedStoredFieldsData/snappy-4         	10000000	       185 ns/op	      11 B/op	       0 allocs/op
BenchmarkWriteCompressedStoredFieldsData/deflate-4        	 2000000	       814 ns/op	       2 B/op	       0 allocs/op
BenchmarkWriteCompressedStoredFieldsData/lz4-4            	 3000000	       369 ns/op	     554 B/op	       0 allocs/op
PASS
ok  	github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding/docs	6.124s
*/
func BenchmarkWriteCompressedStoredFieldsData(b *testing.B) {
	for _, codec := range []CompressionType{SnappyCompressionType, DeflateCompressionType, LZ4CompressionType} {
		b.Run(codec.String(), func(b *testing.B) {
			var buf bytes.Buffer
			opts := CompressionOptions{
				Type:     codec,
				PageSize: 1 << 14,
			}
			w, err := newCompressedDataWriter(opts, nil)
			if err != nil {
				panic(err.Error())
			}
			w.Reset(&buf)
			b.ResetTimer()
			b.ReportAllocs()
			for j := 0; j < b.N; j++ {
				i := b.N % len(testDocuments)
				d := testDocuments[i]
				_, err := w.Write(d)
				if err != nil {
					panic(err.Error())
				}
			}
			if err := w.Flush(); err != nil {
				panic(err.Error())
			}
		})
	}
}

/*
$ go test ./src/m3ninx/index/segment/fst/encoding/docs -run 'Benchmark' -bench=BenchmarkAccessSingleCompressedStoredFieldsData -v
goos: darwin
goarch: amd64
pkg: github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding/docs
BenchmarkAccessSingleCompressedStoredFieldsData/uncached_snappy-4         	  100000	     12433 ns/op	1317.78 MB/s	   16560 B/op	       4 allocs/op
BenchmarkAccessSingleCompressedStoredFieldsData/uncached_deflate-4        	   10000	    161008 ns/op	 101.76 MB/s	   57904 B/op	      19 allocs/op
BenchmarkAccessSingleCompressedStoredFieldsData/uncached_lz4-4            	   20000	     58620 ns/op	 279.49 MB/s	  188944 B/op	       9 allocs/op
BenchmarkAccessSingleCompressedStoredFieldsData/cached_snappy-4           	  200000	      7031 ns/op	2330.03 MB/s	     128 B/op	       2 allocs/op
BenchmarkAccessSingleCompressedStoredFieldsData/cached_deflate-4          	   10000	    137704 ns/op	 118.98 MB/s	     832 B/op	      11 allocs/op
BenchmarkAccessSingleCompressedStoredFieldsData/cached_lz4-4              	  100000	     10149 ns/op	1614.26 MB/s	     129 B/op	       2 allocs/op
PASS
ok  	github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding/docs	9.009s
*/
func BenchmarkAccessSingleCompressedStoredFieldsData(b *testing.B) {
	pageSize := 1 << 14
	for _, benchCase := range []struct {
		name   string
		cached bool
	}{
		{"uncached", false},
		{"cached", true},
	} {
		for _, codec := range []CompressionType{SnappyCompressionType, DeflateCompressionType, LZ4CompressionType} {
			b.Run(fmt.Sprintf("%v %v", benchCase.name, codec.String()), func(b *testing.B) {
				var buf bytes.Buffer
				opts := CompressionOptions{
					Type:     codec,
					PageSize: pageSize,
				}
				w, err := newCompressedDataWriter(opts, nil)
				if err != nil {
					panic(err.Error())
				}
				w.Reset(&buf)
				offsets := make([]offset, 0, len(testDocuments))
				for _, d := range testDocuments {
					off, err := w.Write(d)
					if err != nil {
						panic(err.Error())
					}
					offsets = append(offsets, off)
				}
				if err := w.Flush(); err != nil {
					panic(err.Error())
				}
				ro := ReaderOptions{
					CompressionOptions: opts,
				}
				if benchCase.cached {
					dec, err := opts.newDecompressor()
					if err != nil {
						panic(err.Error())
					}
					ro.Decompressor = dec
				}
				r, err := newCompressedDataReader(ro, buf.Bytes())
				if err != nil {
					panic(err.Error())
				}

				b.SetBytes(int64(pageSize))
				b.ResetTimer()
				b.ReportAllocs()
				w.Reset(ioutil.Discard)
				for j := 0; j < b.N; j++ {
					i := b.N % len(testDocuments)
					off := offsets[i]

					_, err := r.Read(off)
					if err != nil {
						panic(err.Error())
					}
				}
			})
		}
	}
}

/*
 $ go test ./src/m3ninx/index/segment/fst/encoding/docs -run 'TestCompressionRatio' -v | grep -ve '\-\-' | paste -d ' ' - - | grep -v PASS |tail -n+2|cut -d':' -f2- | tr '_' ' ' | column -s ' ' -t

snappy   page-size:2048     compression-ratio:  2.354
snappy   page-size:4096     compression-ratio:  2.523
snappy   page-size:8192     compression-ratio:  2.632
snappy   page-size:16384    compression-ratio:  2.724
snappy   page-size:32768    compression-ratio:  2.747
snappy   page-size:65536    compression-ratio:  2.760
snappy   page-size:131072   compression-ratio:  2.761
snappy   page-size:262144   compression-ratio:  2.762
snappy   page-size:524288   compression-ratio:  2.762
snappy   page-size:1048576  compression-ratio:  2.762
deflate  page-size:2048     compression-ratio:  3.078
deflate  page-size:4096     compression-ratio:  3.426
deflate  page-size:8192     compression-ratio:  3.640
deflate  page-size:16384    compression-ratio:  3.765
deflate  page-size:32768    compression-ratio:  3.820
deflate  page-size:65536    compression-ratio:  3.847
deflate  page-size:131072   compression-ratio:  3.859
deflate  page-size:262144   compression-ratio:  3.874
deflate  page-size:524288   compression-ratio:  3.874
deflate  page-size:1048576  compression-ratio:  3.874
lz4      page-size:2048     compression-ratio:  2.291
lz4      page-size:4096     compression-ratio:  2.453
lz4      page-size:8192     compression-ratio:  2.541
lz4      page-size:16384    compression-ratio:  2.587
lz4      page-size:32768    compression-ratio:  2.607
lz4      page-size:65536    compression-ratio:  2.626
lz4      page-size:131072   compression-ratio:  2.626
lz4      page-size:262144   compression-ratio:  2.627
lz4      page-size:524288   compression-ratio:  2.627
lz4      page-size:1048576  compression-ratio:  2.627

// NB: the benchmarks in this test rely on synthetic data, with less repeated information
// than we'd expect in production metrics data (e.g. no repitition between _id and tags),
// so the compression ratios we're observing are lower bounds.
*/
func TestCompressionRatio(t *testing.T) {
	rawSize := rawCorpusSize(t)
	fmt.Printf("Raw Size: %d\n", rawSize)
	for _, codec := range []CompressionType{SnappyCompressionType, DeflateCompressionType, LZ4CompressionType} {
		for i := 10; i < 20; i++ {
			sz := 2 << uint32(i)
			t.Run(fmt.Sprintf("codec:%v page-size:%d", codec.String(), sz), func(t *testing.T) {
				coSize := compressedCorpusSize(t, sz, codec)
				fmt.Printf("compression-ratio: %0.3f\n", float64(rawSize)/float64(coSize))
			})
		}
	}
}

func compressedCorpusSize(t *testing.T, size int, codec CompressionType) int {
	var buf bytes.Buffer
	w, err := newCompressedDataWriter(CompressionOptions{
		Type:     codec,
		PageSize: size,
	}, nil)
	require.NoError(t, err)
	w.Reset(&buf)
	for _, d := range testDocuments {
		_, err := w.Write(d)
		require.NoError(t, err)
	}
	require.NoError(t, w.Flush())
	return len(buf.Bytes())
}

func rawCorpusSize(t *testing.T) int {
	var buf bytes.Buffer
	w := newRawDataWriter(nil)
	w.Reset(&buf)
	for _, d := range testDocuments {
		_, err := w.Write(d)
		require.NoError(t, err)
	}
	require.NoError(t, w.Flush())
	return len(buf.Bytes())
}
