package routing

import (
	"sync"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/x/watch"
)

type PolicyHandler interface {
	// Init initializes the policy watcher.
	Init() error
	// Close closes the policy watcher.
	Close()
	// GetTrafficTypes returns the traffic types.
	GetTrafficTypes() map[string]uint64
}

type routingPolicyHandler struct {
	sync.RWMutex

	store        kv.Store
	trafficTypes map[string]uint64
	key          string

	value     watch.Value
	isClosed  bool
	processFn watch.ProcessFn
}

func NewRoutingPolicyHandler(opts PolicyHandlerOptions) (PolicyHandler, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	store, err := opts.KVClient().Store(opts.KVOverrideOptions())
	if err != nil {
		return nil, err
	}
	p := &routingPolicyHandler{
		store:        store,
		trafficTypes: opts.StaticTrafficTypes(),
		isClosed:     false,
	}
	p.processFn = p.processUpdate
	return p, nil
}

func (p *routingPolicyHandler) Init() error {
	newUpdatableFn := func() (watch.Updatable, error) {
		return p.store.Watch(p.key)
	}
	getUpdateFn := func(value watch.Updatable) (interface{}, error) {
		v := value.(kv.ValueWatch).Get()
		if v == nil {
			// TODO log error
		}
		policy, err := NewPolicyFromValue(v)
		if err != nil {
			// TODO log error
		}
		return policy, nil
	}
	vOptions := watch.NewOptions().
		SetNewUpdatableFn(newUpdatableFn).
		SetGetUpdateFn(getUpdateFn).
		SetProcessFn(p.processFn).
		SetKey(p.key)
	p.value = watch.NewValue(vOptions)
	if err := p.value.Watch(); err != nil {
		return err
	}
	return nil
}

func (p *routingPolicyHandler) processUpdate(update interface{}) error {
	p.Lock()
	defer p.Unlock()
	if p.isClosed {
		return nil
	}
	v := update.(Policy)
	p.trafficTypes = v.TrafficTypes()
	return nil
}

func (p *routingPolicyHandler) Close() {
	p.Lock()
	if p.isClosed {
		p.Unlock()
		return
	}
	p.isClosed = true
	p.Unlock()

	if p.value != nil {
		p.value.Unwatch()
	}
}

// GetTrafficTypes returns the traffic types.
func (p *routingPolicyHandler) GetTrafficTypes() map[string]uint64 {
	p.RLock()
	defer p.RUnlock()
	return p.trafficTypes
}
