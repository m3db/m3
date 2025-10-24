package routing

import (
	"errors"
	"sync"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/x/watch"
)

// PolicyHandler handles dynamic routing policy updates from a KV store.
type PolicyHandler interface {
	// Close closes the policy watcher.
	Close()
	// GetTrafficTypes returns the traffic types.
	GetTrafficTypes() map[string]uint64
}

type routingPolicyHandler struct {
	sync.RWMutex

	store        kv.Store
	policyConfig PolicyConfig

	value            watch.Value
	isWatchingValue  bool
}

// NewRoutingPolicyHandler creates a new routing policy handler.
func NewRoutingPolicyHandler(opts PolicyHandlerOptions) (PolicyHandler, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	p := &routingPolicyHandler{
		store:        nil,
		policyConfig: opts.PolicyConfig(),
	}

	if err := p.initWatch(opts); err != nil {
		return nil, err
	}
	return p, nil
}

// initWatch starts the watch for the policy config if configured
func (p *routingPolicyHandler) initWatch(opts PolicyHandlerOptions) error {
	// if no kv client is set, we don't need to watch for dynamic kv updates
	if opts.KVClient() == nil {
		return nil
	}

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
	defer p.Unlock()
	if !p.isWatchingValue {
		return nil
	}
	v, ok := update.(PolicyConfig)
	if !ok {
		return errors.New("incoming update is not a PolicyConfig")
	}
	p.policyConfig = v
	return nil
}

func (p *routingPolicyHandler) Close() {
	p.Lock()
	if !p.isWatchingValue {
		p.Unlock()
		return
	}
	p.isWatchingValue = false
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
