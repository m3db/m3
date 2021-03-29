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

package ingestcarbon

import (
	"github.com/m3db/m3/src/cmd/services/m3query/config"
)

// nolint: gocyclo
func copyAndRewrite(
	dst, src []byte,
	cfg *config.CarbonIngesterRewriteConfiguration,
) []byte {
	if cfg == nil || !cfg.Cleanup {
		// No rewrite required.
		return append(dst[:0], src...)
	}

	// Copy into dst as we rewrite.
	dst = dst[:0]
	leadingDots := true
	numDots := 0
	for _, c := range src {
		if c == '.' {
			numDots++
		} else {
			numDots = 0
			leadingDots = false
		}

		if leadingDots {
			// Currently processing leading dots.
			continue
		}

		if numDots > 1 {
			// Do not keep multiple dots.
			continue
		}

		if !(c >= 'a' && c <= 'z') &&
			!(c >= 'A' && c <= 'Z') &&
			!(c >= '0' && c <= '9') &&
			c != '.' &&
			c != '-' &&
			c != '_' &&
			c != ':' &&
			c != '#' {
			// Invalid character, replace with underscore.
			if n := len(dst); n > 0 && dst[n-1] == '_' {
				// Preceding character already underscore.
				continue
			}
			dst = append(dst, '_')
			continue
		}

		// Valid character and not proceeding dot or multiple dots.
		dst = append(dst, c)
	}
	for i := len(dst) - 1; i >= 0; i-- {
		if dst[i] != '.' {
			// Found non dot.
			break
		}
		// Remove trailing dot.
		dst = dst[:i]
	}
	return dst
}
