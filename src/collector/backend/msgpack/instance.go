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

package msgpack

import "fmt"

type instance struct {
	id      string
	address string
}

func newInstance(id string, addr string) instance {
	return instance{id: id, address: addr}
}

func (i instance) ID() string      { return i.id }
func (i instance) Address() string { return i.address }
func (i instance) String() string  { return fmt.Sprintf("Instance<id=%s, address=%s>", i.id, i.address) }

// instancesByIDAsc sorts instances by their ids in ascending order.
type instancesByIDAsc []instance

func (inst instancesByIDAsc) Len() int           { return len(inst) }
func (inst instancesByIDAsc) Swap(i, j int)      { inst[i], inst[j] = inst[j], inst[i] }
func (inst instancesByIDAsc) Less(i, j int) bool { return inst[i].ID() < inst[j].ID() }
