// Copyright (c) 2016 Uber Technologies, Inc.
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

package cluster

// A Shard represents a piece of data owned by the service
type Shard interface {
	ID() uint32 // ID returns the id of the shard
}

// NewShard returns a new Shard
func NewShard(id uint32) Shard { return shard{id: id} }

// Shards is a collection of shards owned by one ServiceInstance
type Shards interface {
	Shards() []Shard // Shards returns the shards
	ShardsLen() int  // ShardsLen returns the length of the shards
}

// NewShards returns a new instance of Shards
func NewShards(ss []Shard) Shards { return shards{ss: ss} }

type shard struct {
	id uint32
}

func (s shard) ID() uint32 { return s.id }

type shards struct {
	ss []Shard
}

func (s shards) Shards() []Shard { return s.ss }
func (s shards) ShardsLen() int  { return len(s.ss) }
