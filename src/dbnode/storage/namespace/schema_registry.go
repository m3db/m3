// Copyright (c) 2019 Uber Technologies, Inc.
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
	"sync"

	"github.com/m3db/m3/src/x/ident"
	"fmt"
)

var (
	defaultSchemaRegistry = newSchemaRegistry()
)


type schemaRegistry struct {
	sync.RWMutex

	registry map[string]SchemaHistory
}

func NewSchemaRegistry() SchemaRegistry {
	return defaultSchemaRegistry
}

func newSchemaRegistry() SchemaRegistry {
	return &schemaRegistry{registry: make(map[string]SchemaHistory)}
}

func (sr *schemaRegistry) SetSchemaHistory(id ident.ID, history SchemaHistory) error {
	sr.Lock()
	defer sr.Unlock()

	current, ok := sr.registry[id.String()]
	if ok {
		if !history.Extends(current) {
			return fmt.Errorf("can not update schema registry to one that does not extends the existing one")
		}
	}

	sr.registry[id.String()] = history
	return nil
}

func (sr *schemaRegistry) GetLatestSchema(id ident.ID) (SchemaDescr, bool) {
	sr.RLock()
	defer sr.RUnlock()

	history, ok := sr.registry[id.String()]
	if !ok {
		return nil, false
	}
	return history.GetLatest()
}

func (sr *schemaRegistry) GetSchema(id ident.ID, schemaId string) (SchemaDescr, bool) {
	sr.RLock()
	defer sr.RUnlock()

	history, ok := sr.registry[id.String()]
	if !ok {
		return nil, false
	}
	return history.Get(schemaId)
}
