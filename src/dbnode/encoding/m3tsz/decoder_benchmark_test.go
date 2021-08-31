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

package m3tsz

import (
	"encoding/base64"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/x/xio"
)

// BenchmarkM3TSZDecode benchmarks M3TSZ decoding performance against a mix of randomly
// permuted real world time series.
func BenchmarkM3TSZDecode(b *testing.B) {
	var (
		encodingOpts = encoding.NewOptions()
		reader       = xio.NewBytesReader64(nil)
		seriesRun    = prepareSampleSeriesRun(b)
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader.Reset(seriesRun[i])
		iter := NewReaderIterator(reader, DefaultIntOptimizationEnabled, encodingOpts)
		for iter.Next() {
			_, _, _ = iter.Current()
		}
		require.NoError(b, iter.Err())
	}
}

// BenchmarkM3TSZDecodeConstant benchmarks M3TSZ decoding performance against a time series
// containing a constant value (as opposed to BenchmarkM3TSZDecodeVarying).
func BenchmarkM3TSZDecodeConstant(b *testing.B) {
	constSeriesBase64 := "FiYqRnIdAACAQEAAAAAarE7gA+AABOIJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJMAAA==" // nolint: lll
	benchmarkM3TSZDecodeSingleSeries(b, constSeriesBase64)
}

// BenchmarkM3TSZDecodeVarying benchmarks M3TSZ decoding performance against a time series
// containing a highly varying value (as opposed to BenchmarkM3TSZDecodeConstant).
func BenchmarkM3TSZDecodeVarying(b *testing.B) {
	varyingSeriesBase64 := "FiYqRnIdAACAQEAAAAArkizADVrDlnvgAATiGAAEbAABZgABkwAA6DFXAxnaGOwDF2ON7Yw85trFGksvYiyRjTFW3MeYs21wLHm9t/YkxtjbHW5vCYi6JwTF2LMcYsGI2DGdTRBjsCxRi7bHdsRZI2ZjDdGQsfbs15ijGHosPYqxNjjPGnMcYu29jbJmusVY03FibeGkMYY8xVizVHHsXY+3BjTR2NMYcE2ti7V2yMZb63hi7dmdMYdoxpizgGxMWa805ljgGMsVY4zRiLiHWslZo11lLOGLMdY61Zkjd2uMRZi1BljI2ostbo1hmDfHasVZUytjTeWOshZK3BjTdGtsWYwxdjwYjgMZpNwzLKM8+btsqGOwjHGMNubIxtnTVWVt1bUxRtLWmWtnY+x1nLU2YtjcuzJw7VWbMfYu0RjLVWbM6aY4lpjT2LtVaS0NqTGGJNeYq3torFWMNJaS1ZrTRWpuCYw1xjLFmItCaExJkDWGZMWZg6xjLMGLtiZmxps7EWLNlYw6NjzFmLtvZaxhi7GGNBiPAxmK8DRM0yj8uq2TKMk0DZOu+rPMsyjQumGOxTgGMNzaaxVrLEWLMUZk0xoDy2QN3Y8yNvLNGmM0boxRtrxGNMcY20dy7G2fM2bqyBjrXmHNyY4xlvzGWJsXcIxdt7H2LtIY2xRq7gGJsbZoxRiTVWVtvaey92LdGKMeYsxoMR+GM9WgZcMdsWKNrcIxNibl2KMaY0x5mTOWOvecYxRuDbGLsubWxJpjaWKsebExZv7JGKsucAxVu7HGOMfbkxdtjdGLMZY8xBkjH2Kt1d2xVtzIGLuCYyyBjTJ2KstbWxVtDbmMMzY6xF4bPWJtxdgxJvrJWMsdaGxhuzTWJs1egxRt7ZmItNYuxRpzFmOtvdyw9kTZ2LtzdaxZiTV2LsabYxJmTXWJtzZCx5pTH2Lt4cQxdtTiWNNea4xNn7imLtccaxVjTZmLMYYuxZnDSmNM0euxVmjU2KtwcWxRjrj2JsbdsxhjjHWNhiOAxW9rhjOwMdl2LN3aczRjbsmOOCbkxhkDa2LN3Zo1xtjGGMtxbexNmLJWJsZbQ19jDU2LNydwxZnLIGONwbI1xuTNGLNqYwxNnbVmQMdcg15uDF2NtKbaxdq7SWKtqa015jbbmNMib2x9mrHmMtxZA1htrWmLNzZGxNoLQmONzbA1drbGmJt0ZCxRjLIWJt0Y41lsDNWJtiaqxFjzF2OuEbk1ltjRGKNYZUxRtjI2MN/eI11vbe2Jsob4xljrJmKttaM19j7HGKuEaOxJkLdmJOIcW1hmLbWNMvY6xZmTHmMs9b82Fk7TmKM7cKxtijW2LMuYy2BpLQ2NNacOxpjbg2OODaSxp4LVmJtfbux1vcAA" // nolint:lll
	benchmarkM3TSZDecodeSingleSeries(b, varyingSeriesBase64)
}

func benchmarkM3TSZDecodeSingleSeries(b *testing.B, seriesBase64 string) {
	var (
		encodingOpts      = encoding.NewOptions()
		reader            = xio.NewBytesReader64(nil)
	)

	data, err := base64.StdEncoding.DecodeString(seriesBase64)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader.Reset(data)
		iter := NewReaderIterator(reader, DefaultIntOptimizationEnabled, encodingOpts)
		for iter.Next() {
			_, _, _ = iter.Current()
		}
		require.NoError(b, iter.Err())
	}
}

func prepareSampleSeriesRun(b *testing.B) [][]byte {
	b.Helper()
	var (
		rnd          = rand.New(rand.NewSource(42)) // nolint: gosec
		sampleSeries = make([][]byte, 0, len(sampleSeriesBase64))
		seriesRun    = make([][]byte, 0, b.N)
	)

	for _, b64 := range sampleSeriesBase64 {
		data, err := base64.StdEncoding.DecodeString(b64)
		require.NoError(b, err)
		sampleSeries = append(sampleSeries, data)
	}

	for i := 0; i < b.N; i++ {
		seriesRun = append(seriesRun, sampleSeries[rnd.Intn(len(sampleSeries))])
	}

	return seriesRun
}
