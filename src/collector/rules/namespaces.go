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

package rules

import (
	"errors"
	"sync"
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/util/runtime"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/id"
	"github.com/m3db/m3x/log"
)

var (
	emptyNamespaces rules.Namespaces
	errNilValue     = errors.New("nil value received")
)

// namespaces contains the list of namespace users have defined rules for.
type namespaces struct {
	sync.RWMutex
	runtime.Value

	key             string
	store           kv.Store
	cache           Cache
	opts            Options
	log             xlog.Logger
	ruleSetKeyFn    RuleSetKeyFn
	nowFn           clock.NowFn
	maxNegativeSkew time.Duration

	proto *schema.Namespaces
	rules map[xid.Hash]*ruleSet
}

func newNamespaces(key string, cache Cache, opts Options) *namespaces {
	clockOpts := opts.ClockOptions()
	n := &namespaces{
		key:             key,
		store:           opts.KVStore(),
		cache:           cache,
		opts:            opts,
		log:             opts.InstrumentOptions().Logger(),
		ruleSetKeyFn:    opts.RuleSetKeyFn(),
		proto:           &schema.Namespaces{},
		rules:           make(map[xid.Hash]*ruleSet),
		nowFn:           clockOpts.NowFn(),
		maxNegativeSkew: clockOpts.MaxNegativeSkew(),
	}
	valueOpts := runtime.NewOptions().
		SetInstrumentOptions(opts.InstrumentOptions()).
		SetInitWatchTimeout(opts.InitWatchTimeout()).
		SetKVStore(n.store).
		SetUnmarshalFn(n.toNamespaces).
		SetProcessFn(n.process)
	n.Value = runtime.NewValue(key, valueOpts)
	return n
}

func (n *namespaces) Close() {
	// NB(xichen): we stop watching the value outside lock because otherwise we might
	// be holding the namespace lock while attempting to acquire the value lock, and
	// the updating goroutine might be holding the value lock and attempting to
	// acquire the namespace lock, causing a deadlock.
	n.Value.Unwatch()

	n.RLock()
	for _, rs := range n.rules {
		rs.Unwatch()
	}
	n.RUnlock()
}

func (n *namespaces) toNamespaces(value kv.Value) (interface{}, error) {
	n.Lock()
	defer n.Unlock()

	if value == nil {
		return emptyNamespaces, errNilValue
	}
	n.proto.Reset()
	if err := value.Unmarshal(n.proto); err != nil {
		return emptyNamespaces, err
	}
	return rules.NewNamespaces(value.Version(), n.proto)
}

func (n *namespaces) process(value interface{}) error {
	var (
		nss        = value.(rules.Namespaces)
		version    = nss.Version()
		namespaces = nss.Namespaces()
		incoming   = make(map[xid.Hash]rules.Namespace, len(namespaces))
	)
	for _, ns := range namespaces {
		incoming[xid.HashFn(ns.Name())] = ns
	}

	n.Lock()
	defer n.Unlock()

	for nsHash, ns := range incoming {
		nsName, snapshots := ns.Name(), ns.Snapshots()
		ruleSet, exists := n.rules[nsHash]
		if !exists {
			ruleSetKey := n.ruleSetKeyFn(ns.Name())
			ruleSet = newRuleSet(nsName, ruleSetKey, n.cache, n.opts)
			n.rules[nsHash] = ruleSet
		}

		shouldWatch := true
		// This should never happen but just to be on the defensive side.
		if len(snapshots) == 0 {
			n.log.WithFields(
				xlog.NewLogField("version", version),
			).Warn("namespace updates have no snapshots")
		} else {
			latestSnapshot := snapshots[len(snapshots)-1]
			// If the latest update shows the namespace is tombstoned, and we
			// have received the corresponding ruleset update, we can stop watching
			// the ruleset updates.
			if latestSnapshot.Tombstoned() && latestSnapshot.ForRuleSetVersion() == ruleSet.Version() {
				shouldWatch = false
			}
		}
		if !shouldWatch {
			ruleSet.Unwatch()
		} else if err := ruleSet.Watch(); err != nil {
			n.log.WithFields(
				xlog.NewLogField("ruleSetKey", ruleSet.Key()),
				xlog.NewLogErrField(err),
			).Error("failed to watch ruleset updates")
		}

		if !exists {
			n.cache.Register(nsName, ruleSet)
		}
	}

	for nsHash, ruleSet := range n.rules {
		_, exists := incoming[nsHash]
		if exists {
			continue
		}
		// Process the namespaces not in the incoming update.
		earliest := n.nowFn().Add(-n.maxNegativeSkew)
		if ruleSet.Tombstoned() && ruleSet.CutoverNanos() <= earliest.UnixNano() {
			n.cache.Unregister(ruleSet.Namespace())
			delete(n.rules, nsHash)
			ruleSet.Unwatch()
		}
	}

	return nil
}
