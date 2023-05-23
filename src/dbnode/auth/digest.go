// Copyright (c) 2023 Uber Technologies, Inc.
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

package auth

import (
	"crypto/md5" // #nosec
	"encoding/hex"
	"fmt"
	"hash"
)

var (
	credentialCache = map[string]string{}
	hashFunc        = md5.New() // #nosec
)

func generateHashWithCacheLookup(data string, h hash.Hash) (string, error) {
	if val, ok := credentialCache[data]; ok {
		return val, nil
	}
	_, err := h.Write([]byte(data))
	if err != nil {
		return "", fmt.Errorf("error generating hash")
	}
	generatedHash := hex.EncodeToString(h.Sum(nil))
	credentialCache[data] = generatedHash
	return generatedHash, nil
}

// GenerateHash takes data as input param and returns md5 hash either from cache or creating md5 hash on runtime.
func GenerateHash(data string) (string, error) {
	defer hashFunc.Reset()
	return generateHashWithCacheLookup(data, hashFunc)
}

func invalidateCache() {
	credentialCache = map[string]string{}
}
