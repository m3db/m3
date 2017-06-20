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

package namespace

import (
	"github.com/m3db/m3db/ts"
)

type registry struct {
	namespaces map[ts.Hash]Metadata
	ids        []ts.ID
	metadatas  []Metadata
}

// NewRegistry returns a new registry containing provided metadatas
func NewRegistry(metadatas []Metadata) Registry {
	var (
		ns          = make(map[ts.Hash]Metadata, len(metadatas))
		ids         = make([]ts.ID, 0, len(metadatas))
		nsMetadatas = make([]Metadata, 0, len(metadatas))
	)
	for _, m := range metadatas {
		ids = append(ids, m.ID())
		nsMetadatas = append(nsMetadatas, m)
		ns[m.ID().Hash()] = m
	}
	return &registry{
		namespaces: ns,
		ids:        ids,
		metadatas:  nsMetadatas,
	}
}

func (r *registry) Get(namespace ts.ID) (Metadata, bool) {
	idHash := namespace.Hash()
	metadata, ok := r.namespaces[idHash]
	return metadata, ok
}

func (r *registry) IDs() []ts.ID {
	return r.ids
}

func (r *registry) Metadatas() []Metadata {
	return r.metadatas
}
