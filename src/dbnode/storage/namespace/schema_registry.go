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
