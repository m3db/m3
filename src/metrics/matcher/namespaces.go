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

package matcher

import (
	"errors"
	"sync"
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/util/runtime"
	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/generated/proto/rulepb"
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/watch"

	"github.com/uber-go/tally"
)

var (
	emptyNamespaces rules.Namespaces
	errNilValue     = errors.New("nil value received")
)

// Namespaces manages runtime updates to registered namespaces and provides
// API to match metic ids against rules in the corresponding namespaces.
type Namespaces interface {
	// Open opens the namespaces and starts watching runtime rule updates
	Open() error

	// Version returns the current version for a give namespace.
	Version(namespace []byte) int

	// ForwardMatch forward matches the matching policies for a given id in a given namespace
	// between [fromNanos, toNanos).
	ForwardMatch(namespace, id []byte, fromNanos, toNanos int64) rules.MatchResult

	// ReverseMatch reverse matches the matching policies for a given id in a given namespace
	// between [fromNanos, toNanos), taking into account the metric type and aggregation type for the given id.
	ReverseMatch(namespace, id []byte, fromNanos, toNanos int64, mt metric.Type, at aggregation.Type) rules.MatchResult

	// Close closes the namespaces.
	Close()
}

type rulesNamespace rules.Namespace

type namespacesMetrics struct {
	notExists         tally.Counter
	added             tally.Counter
	removed           tally.Counter
	watched           tally.Counter
	watchErrors       tally.Counter
	unwatched         tally.Counter
	createWatchErrors tally.Counter
	initWatchErrors   tally.Counter
}

func newNamespacesMetrics(scope tally.Scope) namespacesMetrics {
	return namespacesMetrics{
		notExists:         scope.Counter("not-exists"),
		added:             scope.Counter("added"),
		removed:           scope.Counter("removed"),
		watched:           scope.Counter("watched"),
		watchErrors:       scope.Counter("watch-errors"),
		unwatched:         scope.Counter("unwatched"),
		createWatchErrors: scope.Counter("create-watch-errors"),
		initWatchErrors:   scope.Counter("init-watch-errors"),
	}
}

// namespaces contains the list of namespace users have defined rules for.
type namespaces struct {
	sync.RWMutex
	runtime.Value

	key                  string
	store                kv.Store
	opts                 Options
	nowFn                clock.NowFn
	log                  log.Logger
	ruleSetKeyFn         RuleSetKeyFn
	matchRangePast       time.Duration
	onNamespaceAddedFn   OnNamespaceAddedFn
	onNamespaceRemovedFn OnNamespaceRemovedFn

	proto   *rulepb.Namespaces
	rules   *namespaceRuleSetsMap
	metrics namespacesMetrics
}

// NewNamespaces creates a new namespaces object.
func NewNamespaces(key string, opts Options) Namespaces {
	instrumentOpts := opts.InstrumentOptions()
	n := &namespaces{
		key:                  key,
		store:                opts.KVStore(),
		opts:                 opts,
		nowFn:                opts.ClockOptions().NowFn(),
		log:                  instrumentOpts.Logger(),
		ruleSetKeyFn:         opts.RuleSetKeyFn(),
		matchRangePast:       opts.MatchRangePast(),
		onNamespaceAddedFn:   opts.OnNamespaceAddedFn(),
		onNamespaceRemovedFn: opts.OnNamespaceRemovedFn(),
		proto:                &rulepb.Namespaces{},
		rules:                newNamespaceRuleSetsMap(namespaceRuleSetsMapOptions{}),
		metrics:              newNamespacesMetrics(instrumentOpts.MetricsScope()),
	}
	valueOpts := runtime.NewOptions().
		SetInstrumentOptions(instrumentOpts).
		SetInitWatchTimeout(opts.InitWatchTimeout()).
		SetKVStore(n.store).
		SetUnmarshalFn(n.toNamespaces).
		SetProcessFn(n.process)
	n.Value = runtime.NewValue(key, valueOpts)
	return n
}

func (n *namespaces) Open() error {
	err := n.Watch()
	if err == nil {
		return nil
	}

	errCreateWatch, ok := err.(watch.CreateWatchError)
	if ok {
		n.metrics.createWatchErrors.Inc(1)
		return errCreateWatch
	}
	// NB(xichen): we managed to watch the key but weren't able
	// to initialize the value. In this case, log the error instead
	// to be more resilient to error conditions preventing process
	// from starting up.
	n.metrics.initWatchErrors.Inc(1)
	n.opts.InstrumentOptions().Logger().WithFields(
		log.NewField("key", n.key),
		log.NewErrField(err),
	).Error("error initializing namespaces values, retrying in the background")
	return nil
}

func (n *namespaces) Version(namespace []byte) int {
	n.RLock()
	ruleSet, exists := n.rules.Get(namespace)
	n.RUnlock()
	if !exists {
		return kv.UninitializedVersion
	}
	return ruleSet.Version()
}

func (n *namespaces) ForwardMatch(namespace, id []byte, fromNanos, toNanos int64) rules.MatchResult {
	ruleSet, exists := n.ruleSet(namespace)
	if !exists {
		return rules.EmptyMatchResult
	}
	return ruleSet.ForwardMatch(id, fromNanos, toNanos)
}

func (n *namespaces) ReverseMatch(
	namespace, id []byte,
	fromNanos, toNanos int64,
	mt metric.Type,
	at aggregation.Type,
) rules.MatchResult {
	ruleSet, exists := n.ruleSet(namespace)
	if !exists {
		return rules.EmptyMatchResult
	}
	return ruleSet.ReverseMatch(id, fromNanos, toNanos, mt, at)
}

func (n *namespaces) ruleSet(namespace []byte) (RuleSet, bool) {
	n.RLock()
	ruleSet, exists := n.rules.Get(namespace)
	n.RUnlock()
	if !exists {
		n.metrics.notExists.Inc(1)
	}
	return ruleSet, exists
}

func (n *namespaces) Close() {
	// NB(xichen): we stop watching the value outside lock because otherwise we might
	// be holding the namespace lock while attempting to acquire the value lock, and
	// the updating goroutine might be holding the value lock and attempting to
	// acquire the namespace lock, causing a deadlock.
	n.Value.Unwatch()

	n.RLock()
	for _, entry := range n.rules.Iter() {
		rs := entry.Value()
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
		incoming   = newRuleNamespacesMap(ruleNamespacesMapOptions{
			InitialSize: len(namespaces),
		})
	)
	for _, ns := range namespaces {
		incoming.Set(ns.Name(), rulesNamespace(ns))
	}

	n.Lock()
	defer n.Unlock()

	for _, entry := range incoming.Iter() {
		namespace, elem := entry.Key(), rules.Namespace(entry.Value())
		nsName, snapshots := elem.Name(), elem.Snapshots()
		ruleSet, exists := n.rules.Get(namespace)
		if !exists {
			instrumentOpts := n.opts.InstrumentOptions()
			ruleSetScope := instrumentOpts.MetricsScope().SubScope("ruleset")
			ruleSetOpts := n.opts.SetInstrumentOptions(instrumentOpts.SetMetricsScope(ruleSetScope))
			ruleSetKey := n.ruleSetKeyFn(elem.Name())
			ruleSet = newRuleSet(nsName, ruleSetKey, ruleSetOpts)
			n.rules.Set(namespace, ruleSet)
			n.metrics.added.Inc(1)
		}

		shouldWatch := true
		// This should never happen but just to be on the defensive side.
		if len(snapshots) == 0 {
			n.log.WithFields(
				log.NewField("version", version),
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
			n.metrics.unwatched.Inc(1)
			ruleSet.Unwatch()
		} else {
			n.metrics.watched.Inc(1)
			if err := ruleSet.Watch(); err != nil {
				n.metrics.watchErrors.Inc(1)
				n.log.WithFields(
					log.NewField("ruleSetKey", ruleSet.Key()),
					log.NewErrField(err),
				).Error("failed to watch ruleset updates")
			}
		}

		if !exists && n.onNamespaceAddedFn != nil {
			n.onNamespaceAddedFn(nsName, ruleSet)
		}
	}

	for _, entry := range n.rules.Iter() {
		namespace, ruleSet := entry.Key(), entry.Value()
		_, exists := incoming.Get(namespace)
		if exists {
			continue
		}
		// Process the namespaces not in the incoming update.
		earliestNanos := n.nowFn().Add(-n.matchRangePast).UnixNano()
		if ruleSet.Tombstoned() && ruleSet.CutoverNanos() <= earliestNanos {
			if n.onNamespaceRemovedFn != nil {
				n.onNamespaceRemovedFn(ruleSet.Namespace())
			}
			n.rules.Delete(namespace)
			ruleSet.Unwatch()
			n.metrics.unwatched.Inc(1)
		}
	}

	return nil
}
