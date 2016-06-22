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

package topology

import (
	"math"

	"github.com/m3db/m3db/interfaces/m3db"
)

func quorum(replicas int) int {
	return int(math.Ceil(0.5 * float64(replicas+1)))
}

type simpleHost string

func (s simpleHost) Address() string {
	return string(s)
}

// NewHost creates a new host
func NewHost(address string) m3db.Host {
	return simpleHost(address)
}

type hostShardSet struct {
	host     m3db.Host
	shardSet m3db.ShardSet
}

// NewHostShardSet creates a new host shard set
func NewHostShardSet(host m3db.Host, shardSet m3db.ShardSet) m3db.HostShardSet {
	return &hostShardSet{host, shardSet}
}

func (h *hostShardSet) Host() m3db.Host {
	return h.host
}

func (h *hostShardSet) ShardSet() m3db.ShardSet {
	return h.shardSet
}
