package routing

import (
	"errors"
	"sync"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/x/watch"
)

// PolicyUpdateListener is called when the policy configuration changes.
type PolicyUpdateListener func(PolicyConfig)

// PolicyHandler handles dynamic routing policy updates from a KV store.
type PolicyHandler interface {
	// Close closes the policy watcher.
	Close()
	// GetTrafficTypes returns the traffic types.
	GetTrafficTypes() map[string]uint64
	// Subscribe adds a listener that will be called when the policy updates.
	// Returns a subscription ID that can be used to unsubscribe.
	Subscribe(listener PolicyUpdateListener) int
	// Unsubscribe removes a listener by its subscription ID.
	Unsubscribe(id int)
}

type routingPolicyHandler struct {
	sync.RWMutex

	store           kv.Store
	policyConfig    PolicyConfig
	value           watch.Value
	isWatchingValue bool

	listeners      map[int]PolicyUpdateListener
	nextListenerID int
}

// NewRoutingPolicyHandler creates a new routing policy handler.
func NewRoutingPolicyHandler(opts PolicyHandlerOptions) (PolicyHandler, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	p := &routingPolicyHandler{
		store:        nil,
		policyConfig: NewPolicyConfig(nil),
		listeners:    make(map[int]PolicyUpdateListener),
	}

	if err := p.initWatch(opts); err != nil {
		return nil, err
	}
	return p, nil
}

// initWatch starts the watch for the policy config if configured
func (p *routingPolicyHandler) initWatch(opts PolicyHandlerOptions) error {
	var err error
	p.store, err = opts.KVClient().Store(opts.KVOverrideOptions())
	if err != nil {
		return err
	}
	p.isWatchingValue = true

	newUpdatableFn := func() (watch.Updatable, error) {
		return p.store.Watch(opts.KVKey())
	}
	getUpdateFn := func(value watch.Updatable) (interface{}, error) {
		v := value.(kv.ValueWatch).Get()
		if v == nil {
			return nil, errors.New("routing policy update value is nil")
		}
		policy, err := NewPolicyConfigFromValue(v)
		if err != nil {
			return nil, err
		}
		return policy, nil
	}
	vOptions := watch.NewOptions().
		SetNewUpdatableFn(newUpdatableFn).
		SetGetUpdateFn(getUpdateFn).
		SetProcessFn(p.processUpdate).
		SetKey(opts.KVKey())
	p.value = watch.NewValue(vOptions)
	if err := p.value.Watch(); err != nil {
		// If the error is an InitValueError (e.g., timeout), we can continue
		// since the watch is still running in the background and will update
		// when the key becomes available.
		var initValueError watch.InitValueError
		if !errors.As(err, &initValueError) {
			return err
		}
	}
	return nil
}

func (p *routingPolicyHandler) processUpdate(update interface{}) error {
	p.Lock()
	if !p.isWatchingValue {
		p.Unlock()
		return nil
	}
	v, ok := update.(PolicyConfig)
	if !ok {
		p.Unlock()
		return errors.New("incoming update is not a PolicyConfig")
	}
	p.policyConfig = v

	// Copy listeners to avoid holding lock during callbacks
	listeners := make([]PolicyUpdateListener, 0, len(p.listeners))
	for _, listener := range p.listeners {
		listeners = append(listeners, listener)
	}
	p.Unlock()

	for _, listener := range listeners {
		listener(v)
	}

	return nil
}

func (p *routingPolicyHandler) Close() {
	p.Lock()
	if !p.isWatchingValue {
		p.Unlock()
		return
	}
	p.isWatchingValue = false
	// Clear all listeners
	p.listeners = make(map[int]PolicyUpdateListener)
	p.Unlock()
	if p.value != nil {
		p.value.Unwatch()
	}
}

// GetTrafficTypes returns the traffic types.
func (p *routingPolicyHandler) GetTrafficTypes() map[string]uint64 {
	p.RLock()
	defer p.RUnlock()
	return p.policyConfig.TrafficTypes()
}

// Subscribe adds a listener that will be called when the policy updates.
// Returns a subscription ID that can be used to unsubscribe.
// The listener will be called immediately with the current policy config if available.
func (p *routingPolicyHandler) Subscribe(listener PolicyUpdateListener) int {
	p.Lock()
	defer p.Unlock()

	id := p.nextListenerID
	p.nextListenerID++
	p.listeners[id] = listener

	// Notify immediately with current config if available
	if listener != nil && p.policyConfig != nil {
		currentConfig := p.policyConfig
		listener(currentConfig)
	}

	return id
}

// Unsubscribe removes a listener by its subscription ID.
func (p *routingPolicyHandler) Unsubscribe(id int) {
	p.Lock()
	defer p.Unlock()
	delete(p.listeners, id)
}
