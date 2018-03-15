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

package aggregation

import (
	"fmt"

	"github.com/willf/bitset"
)

// IDCompressor can compress Types into an ID.
type IDCompressor interface {
	// Compress compresses a set of aggregation types into an aggregation id.
	Compress(aggTypes Types) (ID, error)

	// MustCompress compresses a set of aggregation types into an aggregation id,
	// and panics if an error is encountered.
	MustCompress(aggTypes Types) ID
}

// IDDecompressor can decompress ID.
type IDDecompressor interface {
	// Decompress decompresses aggregation types,
	// returns error if any invalid aggregation type is encountered.
	Decompress(compressed ID) (Types, error)
}

type idCompressor struct {
	bs *bitset.BitSet
}

// NewIDCompressor returns a new IDCompressor.
func NewIDCompressor() IDCompressor {
	// NB(cw): If we start to support more than 64 types, the library will
	// expand the underlying word list itself.
	return &idCompressor{
		bs: bitset.New(maxTypeID),
	}
}

func (c *idCompressor) Compress(aggTypes Types) (ID, error) {
	c.bs.ClearAll()
	for _, aggType := range aggTypes {
		if !aggType.IsValid() {
			return DefaultID, fmt.Errorf("could not compress invalid Type %v", aggType)
		}
		c.bs.Set(uint(aggType.ID()))
	}

	codes := c.bs.Bytes()
	var id ID
	// NB(cw) it's guaranteed that len(id) == len(codes) == IDLen, we need to copy
	// the words in bitset out because the bitset contains a slice internally.
	for i := 0; i < IDLen; i++ {
		id[i] = codes[i]
	}
	return id, nil
}

func (c *idCompressor) MustCompress(aggTypes Types) ID {
	id, err := c.Compress(aggTypes)
	if err != nil {
		panic(fmt.Errorf("unable to compress %v: %v", aggTypes, err))
	}
	return id
}

type idDecompressor struct {
	bs   *bitset.BitSet
	buf  []uint64
	pool TypesPool
}

// NewIDDecompressor returns a new IDDecompressor.
func NewIDDecompressor() IDDecompressor {
	return NewPooledIDDecompressor(nil)
}

// NewPooledIDDecompressor returns a new pooled TypeDecompressor.
func NewPooledIDDecompressor(pool TypesPool) IDDecompressor {
	bs := bitset.New(maxTypeID)
	return &idDecompressor{
		bs:   bs,
		buf:  bs.Bytes(),
		pool: pool,
	}
}

func (c *idDecompressor) Decompress(id ID) (Types, error) {
	if id.IsDefault() {
		return DefaultTypes, nil
	}
	// NB(cw) it's guaranteed that len(c.buf) == len(id) == IDLen, we need to copy
	// the words from id into a slice to be used in bitset.
	for i := range id {
		c.buf[i] = id[i]
	}

	var res Types
	if c.pool == nil {
		res = make(Types, 0, maxTypeID)
	} else {
		res = c.pool.Get()
	}

	for i, e := c.bs.NextSet(0); e; i, e = c.bs.NextSet(i + 1) {
		aggType := Type(i)
		if !aggType.IsValid() {
			return DefaultTypes, fmt.Errorf("invalid Type: %s", aggType.String())
		}

		res = append(res, aggType)
	}

	return res, nil
}
