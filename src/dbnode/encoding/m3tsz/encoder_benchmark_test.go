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
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	xtime "github.com/m3db/m3/src/x/time"
)

var sampleSeriesBase64 = []string{
	"FiYqRnIdAACAQEAAAAArkizADVrDlnvgAATiGAAEbAABZgABkwAA6DFXAxnaGOwDF2ON7Yw85trFGksvYiyRjTFW3MeYs21wLHm9t/YkxtjbHW5vCYi6JwTF2LMcYsGI2DGdTRBjsCxRi7bHdsRZI2ZjDdGQsfbs15ijGHosPYqxNjjPGnMcYu29jbJmusVY03FibeGkMYY8xVizVHHsXY+3BjTR2NMYcE2ti7V2yMZb63hi7dmdMYdoxpizgGxMWa805ljgGMsVY4zRiLiHWslZo11lLOGLMdY61Zkjd2uMRZi1BljI2ostbo1hmDfHasVZUytjTeWOshZK3BjTdGtsWYwxdjwYjgMZpNwzLKM8+btsqGOwjHGMNubIxtnTVWVt1bUxRtLWmWtnY+x1nLU2YtjcuzJw7VWbMfYu0RjLVWbM6aY4lpjT2LtVaS0NqTGGJNeYq3torFWMNJaS1ZrTRWpuCYw1xjLFmItCaExJkDWGZMWZg6xjLMGLtiZmxps7EWLNlYw6NjzFmLtvZaxhi7GGNBiPAxmK8DRM0yj8uq2TKMk0DZOu+rPMsyjQumGOxTgGMNzaaxVrLEWLMUZk0xoDy2QN3Y8yNvLNGmM0boxRtrxGNMcY20dy7G2fM2bqyBjrXmHNyY4xlvzGWJsXcIxdt7H2LtIY2xRq7gGJsbZoxRiTVWVtvaey92LdGKMeYsxoMR+GM9WgZcMdsWKNrcIxNibl2KMaY0x5mTOWOvecYxRuDbGLsubWxJpjaWKsebExZv7JGKsucAxVu7HGOMfbkxdtjdGLMZY8xBkjH2Kt1d2xVtzIGLuCYyyBjTJ2KstbWxVtDbmMMzY6xF4bPWJtxdgxJvrJWMsdaGxhuzTWJs1egxRt7ZmItNYuxRpzFmOtvdyw9kTZ2LtzdaxZiTV2LsabYxJmTXWJtzZCx5pTH2Lt4cQxdtTiWNNea4xNn7imLtccaxVjTZmLMYYuxZnDSmNM0euxVmjU2KtwcWxRjrj2JsbdsxhjjHWNhiOAxW9rhjOwMdl2LN3aczRjbsmOOCbkxhkDa2LN3Zo1xtjGGMtxbexNmLJWJsZbQ19jDU2LNydwxZnLIGONwbI1xuTNGLNqYwxNnbVmQMdcg15uDF2NtKbaxdq7SWKtqa015jbbmNMib2x9mrHmMtxZA1htrWmLNzZGxNoLQmONzbA1drbGmJt0ZCxRjLIWJt0Y41lsDNWJtiaqxFjzF2OuEbk1ltjRGKNYZUxRtjI2MN/eI11vbe2Jsob4xljrJmKttaM19j7HGKuEaOxJkLdmJOIcW1hmLbWNMvY6xZmTHmMs9b82Fk7TmKM7cKxtijW2LMuYy2BpLQ2NNacOxpjbg2OODaSxp4LVmJtfbux1vcAA", // nolint:lll
	"FiYqRnIdAACAQEAAAAArkizADAfgAATiCSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSTAAA=", // nolint:lll
	"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", // nolint:lll
	"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",                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 // nolint:lll
	"FiYqRnIdAACAQEAAAAAnPgFYA+AABOIJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJPFcvVHPFcpJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJPgAAGhz4AABno////L/JJJJJJJJJgAA==", // nolint:lll
	"FiYqRnIdAACAQEAAAAAWlSx4DYa+fHfgAATiGAAgoYABJ1gABamAAJ9DLWSWyU46nCsw5GX05SG/Y2OWyCwqmgq2cGuK2q25IwqGLg1gmuS2uWlAypm0+2QIDrJX7YLgOxGyh2lo3k2/a1eW3Gwqmdq3BWvU4I2/6w1GXS3eXH01VGm0woGYu0jXEC02G1mxlGTY47Gyi18WpMwbGjW2zm0Kz8Wx2wy2fY1wmoO1wHGiwy2XO3fm3O1RmzEwKWCAzp2382R2/cwj2fw21nE81KWtmwVGGoxKG5Y0/moSxTmVg1lmri2P4tk9VTEhi2RA1OG7c1vmsKxnmWYzy238202rgwCmuS2C2wU0h252xbWQw4EWtO4dHA4wNWhM3hWc81fG+oyLGqS26G5s2dWpIxTmZK3/m0I2129CwX2kwzgGxY1EnBMxKmtG5Bmxy2D2rKxa2fS1GW1+11G64xNmmW1DG1e1Im86wImlA1rWpg2k20gwPGbO2SXI625nCQwqWgM1BXAw2nHMOxQmqA1x2ps2+2qUxbWZU2tGvi2s2tiwsmYo1Hmqo3bW6swhmSC2dWnY0rW7owRWgW2CWi62Qm5kwPmGKzyWuC1UHPAw5m9M3jm524Vmk8w7WSe2XHDC4TW7KwbGqE3BGsG1S3AQxpWXEz6Gto1MmyGwbmZU2H2+419mxMwHWcAzWHCO3AGsWxNWm82jW3W1KmxEwLmJQ2825wyzGsQw0Gae3v2oI0sm5ywa2Ue1ymve4FHEoxRWfQ38m2I2vWpmxI2ee1mWoe1R2yQw9Wos1AGvs0nW7gyAWZu2BGfg1+2sGxiGkW2GXlg3IHCqx4IDoyX7e5gOsg0/mruwQGhG1eW8c3Gm8Kws2ZS4ZnCy30WjKwX2UI2Bmui1oHCeyRmxk19mrI1W25Uw5WWo2HWvm2u2nuwrmr8zXoHqg3vgygOzlAcD9ikNTRxANflu2sYFjFtSJqRNJhrmsH9hTMnBtvNW1vKMK9i7t0huMQHWYv3DvAcXpqktctxBuGhruwHCxv2PPAdTRr0NHFtbsM1kytcZsaN7ZsNMN9oYNThsdtw9o9sLplmuHBrUuRttnMbln6tX+A6F9+0dIDwvMDJkWt4Rlwt2ZyDsklloN21r8tahndMIBq2OChq7thBu5sbRnzNhNvCtmpqrQLCTvmiLAtlxvXtqCC7bl2wQILovtoBpUNWFshsWtptNt9yCNqpsAwHBIv2n/AdmdpktyJtFMHBrANk9xRtWRsOMLpmQtt1qhNUBvqsO5lstDVqktmluDsfdq2NvNrVtw5sPsUFkrNWRyXumxscsRhiotmVoBtmVsgsEFmHtmtspNwBsSsGphKtDptKOctuOsV9mht69tFNpFqIMexlUtbNrONLJx6Mq5ldti5tQNAxqYMvhlAte5rCtczwAACtTYy///+pYxJvAAAK1Mc9wJtrlohuG5hLwHN6v2s9AdtpuNQXXtu2CTBdR1vgtkJtutEVhQsn5t+NV5jutQliKNGVwftxdsANuhgKtExtlNDlp7N1RhOMstuCNetr9uR6A4p1+ynIDs4NI1ubOB9gmMH9rttN9qGNwRgWM3pqOtdZo/OWlhvNI5stNhNr7txZjRsvtqgN1ZpcNUBjhseRwKt7lytNzBgStANl2NQBu0NLxh6NQJtMtmlqkuEhjstlBsDuHZqkNixhEs7lqnNppt5NuNiXMPxt4NyNvDNb9hyNU9rztxhsKNUNhAsi9rMtuNvINqdgMMM1lFuM5u5NlVgUs+BpAtydqyNstk4MlppstXZtTt8hhSs0NrBtcxqWNQtiOQHV3v2ihAdVRpONw1lLtZhrJNzRrLtntjRMlJrGNc5qCtF1izMtZtAtOdtoNhJhEMrRrxtFVv0QXW4u2MnBczBsfuYtrrtLmC4oN21nYLoqQAA==", // nolint:lll
	"FiYqRnIdAACAQEAAAAAPGEnQDAfgAATiCSSSSSSSSSSSSSSSSSSSSSSSfAAAnEH///2PBJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJMBb8wFJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJgAA==", // nolint:lll
	"FiYqRnIdAACAQEAAAAAarE7gA+AABOIJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJMAAA==",                 // nolint:lll
	"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",                                                                                                                                                                                                                                                                 // nolint:lll
	"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", // nolint:lll
}

func BenchmarkM3TSZEncode(b *testing.B) {
	var (
		encodingOpts = encoding.NewOptions()
		seriesRun    = prepareSampleSeriesEncRun(b)
		encoder      = NewEncoder(xtime.Now(), nil, DefaultIntOptimizationEnabled, encodingOpts)
	)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		run := seriesRun[i]
		encoder.Reset(run[0].TimestampNanos, len(run), nil)

		for i := range run {
			// Using index access to avoid copying a 40 byte datapoint.
			_ = encoder.Encode(run[i], xtime.Nanosecond, nil)
		}

		encoder.Discard()
	}
}

func prepareSampleSeriesEncRun(b *testing.B) [][]ts.Datapoint {
	var (
		rnd          = rand.New(rand.NewSource(42)) // nolint:gosec
		sampleSeries = make([][]byte, 0, len(sampleSeriesBase64))
		seriesRun    = make([][]ts.Datapoint, b.N)
		encodingOpts = encoding.NewOptions()
		reader       = xio.NewBytesReader64(nil)
	)

	for _, b64 := range sampleSeriesBase64 {
		data, err := base64.StdEncoding.DecodeString(b64)
		require.NoError(b, err)

		sampleSeries = append(sampleSeries, data)
	}

	for i := 0; i < len(seriesRun); i++ {
		reader.Reset(sampleSeries[rnd.Intn(len(sampleSeries))])

		iter := NewReaderIterator(reader, DefaultIntOptimizationEnabled, encodingOpts)
		for iter.Next() {
			dp, _, _ := iter.Current()
			seriesRun[i] = append(seriesRun[i], dp)
		}

		require.NoError(b, iter.Err())
		iter.Close()
	}

	return seriesRun
}
