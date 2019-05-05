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
	"fmt"
	"sync"

	"github.com/m3db/m3/src/x/ident"
	xclose "github.com/m3db/m3/src/x/close"
	xwatch "github.com/m3db/m3/src/x/watch"
)

type schemaRegistry struct {
	sync.RWMutex

	registry map[string]xwatch.Watchable
}

func NewSchemaRegistry() SchemaRegistry {
	return newSchemaRegistry()
}

func newSchemaRegistry() SchemaRegistry {
	return &schemaRegistry{
		registry: make(map[string]xwatch.Watchable),
	}
}

func (sr *schemaRegistry) SetSchemaHistory(id ident.ID, history SchemaHistory) error {
	sr.Lock()
	defer sr.Unlock()

	current, ok := sr.registry[id.String()]
	if ok {
		if !history.Extends(current.Get().(SchemaHistory)) {
			return fmt.Errorf("can not update schema registry to one that does not extends the existing one")
		}
	} else {
		sr.registry[id.String()] = xwatch.NewWatchable()
	}

	sr.registry[id.String()].Update(history)
	return nil
}

func (sr *schemaRegistry) GetLatestSchema(id ident.ID) (SchemaDescr, bool) {
	history, ok := sr.getSchemaHistory(id)
	if !ok {
		return nil, false
	}
	return history.GetLatest()
}

func (sr *schemaRegistry) GetSchema(id ident.ID, schemaId string) (SchemaDescr, bool) {
	history, ok := sr.getSchemaHistory(id)
	if !ok {
		return nil, false
	}
	return history.Get(schemaId)
}

func (sr *schemaRegistry) getSchemaHistory(id ident.ID) (SchemaHistory, bool) {
	sr.RLock()
	defer sr.RUnlock()

	history, ok := sr.registry[id.String()]
	if !ok {
		return nil, false
	}
	return history.Get().(SchemaHistory), true
}


func (sr *schemaRegistry) RegisterListener(
	nsID ident.ID,
	listener SchemaListener,
) (xclose.SimpleCloser, bool) {
	sr.RLock()
	defer sr.RUnlock()

	watchable, ok := sr.registry[nsID.String()]
	if !ok {
		return nil, false
	}

	_, watch, _ := watchable.Watch()

	// We always initialize the watchable so always read
	// the first notification value
	<-watch.C()

	// Deliver the current schema
	if schema, ok := watchable.Get().(SchemaHistory).GetLatest(); ok {
		listener.SetSchema(schema)
	}

	// Spawn a new goroutine that will terminate when the
	// watchable terminates on the close of the runtime options manager
	go func() {
		for range watch.C() {
			if schema, ok := watchable.Get().(SchemaHistory).GetLatest(); ok {
				listener.SetSchema(schema)
			}
		}
	}()

	return watch, true
}

func (sr *schemaRegistry) Close() {
	sr.RLock()
	defer sr.RUnlock()
	for _, w := range sr.registry {
		w.Close()
	}
}
