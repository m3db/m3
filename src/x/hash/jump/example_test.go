// Copyright (c) 2017 Uber Technologies, Inc.
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

package jump_test

import (
	"fmt"
	"hash/fnv"
	"log"

	"github.com/m3db/m3/src/x/hash/jump"
)

func ExampleHash() {
	var (
		numBuckets int64 = 10
		key              = []byte("foo")
		hasher           = fnv.New64()
	)

	// Create hash of the key using whatever hash function you wish.
	if _, err := hasher.Write(key); err != nil {
		log.Fatal(err)
	}
	keyHash := hasher.Sum64()

	// Get which bucket the key is assigned to.
	bucket := jump.Hash(keyHash, numBuckets)

	fmt.Printf("Key '%s' is assigned to bucket %d.\n", string(key), bucket)
	// Output: Key 'foo' is assigned to bucket 9.
}
