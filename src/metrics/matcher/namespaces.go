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

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/util/runtime"
	"github.com/m3db/m3/src/metrics/generated/proto/rulepb"
	"github.com/m3db/m3/src/metrics/matcher/namespace"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/rules"
	"github.com/m3db/m3/src/metrics/rules/view"
	"github.com/m3db/m3/src/x/clock"
	xerrors "github.com/m3db/m3/src/x/errors"
	xos "github.com/m3db/m3/src/x/os"
	"github.com/m3db/m3/src/x/watch"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

var (
	emptyNamespaces rules.Namespaces
	errNilValue     = errors.New("nil value received")
)

// Namespaces manages runtime updates to registered namespaces and provides
// API to match metic ids against rules in the corresponding namespaces.
type Namespaces interface {
	rules.ActiveSet
	// Open opens the namespaces and starts watching runtime rule updates
	Open() error

	// Version returns the current version for a given namespace.
	Version(namespace []byte) int

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
	log                  *zap.Logger
	ruleSetKeyFn         RuleSetKeyFn
	matchRangePast       time.Duration
	onNamespaceAddedFn   OnNamespaceAddedFn
	onNamespaceRemovedFn OnNamespaceRemovedFn

	proto                       *rulepb.Namespaces
	rules                       *namespaceRuleSetsMap
	metrics                     namespacesMetrics
	nsResolver                  namespace.Resolver
	requireNamespaceWatchOnInit bool
}

// NewNamespaces creates a new namespaces object.
func NewNamespaces(key string, opts Options) Namespaces {
	instrumentOpts := opts.InstrumentOptions()
	n := &namespaces{
		key:                         key,
		store:                       opts.KVStore(),
		opts:                        opts,
		nowFn:                       opts.ClockOptions().NowFn(),
		log:                         instrumentOpts.Logger(),
		ruleSetKeyFn:                opts.RuleSetKeyFn(),
		matchRangePast:              opts.MatchRangePast(),
		onNamespaceAddedFn:          opts.OnNamespaceAddedFn(),
		onNamespaceRemovedFn:        opts.OnNamespaceRemovedFn(),
		proto:                       &rulepb.Namespaces{},
		rules:                       newNamespaceRuleSetsMap(namespaceRuleSetsMapOptions{}),
		metrics:                     newNamespacesMetrics(instrumentOpts.MetricsScope()),
		requireNamespaceWatchOnInit: opts.RequireNamespaceWatchOnInit(),
		nsResolver:                  opts.NamespaceResolver(),
	}
	valueOpts := runtime.NewOptions().
		SetInstrumentOptions(instrumentOpts).
		SetInitWatchTimeout(opts.InitWatchTimeout()).
		SetKVStore(n.store).
		SetUnmarshalFn(n.toNamespaces).
		SetProcessFn(n.process).
		SetInterruptedCh(opts.InterruptedCh())
	n.Value = runtime.NewValue(key, valueOpts)
	return n
}

func (n *namespaces) Open() error {
	err := n.Watch()
	var interruptErr *xos.InterruptError
	if err == nil {
		return nil
	} else if errors.As(err, &interruptErr) {
		return err
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
	if n.requireNamespaceWatchOnInit {
		return err
	}

	n.opts.InstrumentOptions().Logger().With(
		zap.String("key", n.key),
		zap.Error(err),
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

func (n *namespaces) LatestRollupRules(namespace []byte, timeNanos int64) ([]view.RollupRule, error) {
	ruleSet, exists := n.ruleSet(namespace)
	if !exists {
		return nil, errors.New("ruleset not found for namespace")
	}

	return ruleSet.LatestRollupRules(namespace, timeNanos)
}

func (n *namespaces) ForwardMatch(id id.ID, fromNanos, toNanos int64,
	opts rules.MatchOptions) (rules.MatchResult, error) {
	namespace := n.nsResolver.Resolve(id)
	ruleSet, exists := n.ruleSet(namespace)
	if !exists {
		return rules.EmptyMatchResult, nil
	}
	return ruleSet.ForwardMatch(id, fromNanos, toNanos, opts)
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

	var (
		watchWg  sync.WaitGroup
		multiErr xerrors.MultiError
		errLock  sync.Mutex
	)

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
			n.log.Warn("namespace updates have no snapshots", zap.Int("version", version))
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

			watchWg.Add(1)
			go func() {
				// Start the watches in background goroutines so that if the store is unavailable they timeout
				// (approximately) in unison. This prevents the timeouts from stacking on top of each
				// other when the store is unavailable and causing a delay of timeout_duration * num_rules.
				defer watchWg.Done()

				if err := ruleSet.Watch(); err != nil {
					n.metrics.watchErrors.Inc(1)
					n.log.Error("failed to watch ruleset updates",
						zap.String("ruleSetKey", ruleSet.Key()),
						zap.Error(err))

					// Track errors if we explicitly want to ensure watches succeed.
					if n.requireNamespaceWatchOnInit {
						errLock.Lock()
						multiErr = multiErr.Add(err)
						errLock.Unlock()
					}
				}
			}()
		}

		if !exists && n.onNamespaceAddedFn != nil {
			n.onNamespaceAddedFn(nsName, ruleSet)
		}
	}

	watchWg.Wait()

	if !multiErr.Empty() {
		return multiErr.FinalError()
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
