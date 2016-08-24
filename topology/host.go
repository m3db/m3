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
	"fmt"
	"math"

	"github.com/m3db/m3db/sharding"
)

func majority(replicas int) int {
	return int(math.Ceil(0.5 * float64(replicas+1)))
}

type host struct {
	id      string
	address string
}

func (h *host) ID() string {
	return h.id
}

func (h *host) Address() string {
	return h.address
}

func (h *host) String() string {
	return fmt.Sprintf("Host<ID=%s, Address=%s>", h.id, h.address)
}

// NewHost creates a new host
func NewHost(id, address string) Host {
	return &host{id: id, address: address}
}

type hostShardSet struct {
	host     Host
	shardSet sharding.ShardSet
}

// NewHostShardSet creates a new host shard set
func NewHostShardSet(host Host, shardSet sharding.ShardSet) HostShardSet {
	return &hostShardSet{host, shardSet}
}

func (h *hostShardSet) Host() Host {
	return h.host
}

func (h *hostShardSet) ShardSet() sharding.ShardSet {
	return h.shardSet
}
