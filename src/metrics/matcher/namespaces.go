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
	"github.com/m3db/m3/src/metrics/rules"
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
	// Open opens the namespaces and starts watching runtime rule updates
	Open() error

	// Version returns the current version for a give namespace.
	Version(namespace []byte) int

	// ForwardMatch forward matches the matching policies for a given id in a given namespace
	// between [fromNanos, toNanos).
	ForwardMatch(namespace, id []byte, fromNanos, toNanos int64) rules.MatchResult

	// Reset the Namespaces for a new request.
	Reset() error

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
	sync.Mutex
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

	rules                       *namespaceRuleSetsMap
	hasNext                     bool
	nextNamespaces              rules.Namespaces
	metrics                     namespacesMetrics
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
		rules:                       newNamespaceRuleSetsMap(namespaceRuleSetsMapOptions{}),
		metrics:                     newNamespacesMetrics(instrumentOpts.MetricsScope()),
		requireNamespaceWatchOnInit: opts.RequireNamespaceWatchOnInit(),
	}
	valueOpts := runtime.NewOptions().
		SetInstrumentOptions(instrumentOpts).
		SetInitWatchTimeout(opts.InitWatchTimeout()).
		SetKVStore(n.store).
		SetUnmarshalFn(toNamespaces).
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
	ruleSet, exists := n.rules.Get(namespace)
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

func (n *namespaces) ruleSet(namespace []byte) (RuleSet, bool) {
	ruleSet, exists := n.rules.Get(namespace)
	if !exists {
		n.metrics.notExists.Inc(1)
	}
	return ruleSet, exists
}

func (n *namespaces) Close() {
	n.Value.Unwatch()
	for _, entry := range n.rules.Iter() {
		rs := entry.Value()
		rs.Unwatch()
	}
}

func toNamespaces(value kv.Value) (interface{}, error) {
	if value == nil {
		return emptyNamespaces, errNilValue
	}
	var proto rulepb.Namespaces
	if err := value.Unmarshal(&proto); err != nil {
		return emptyNamespaces, err
	}
	return rules.NewNamespaces(value.Version(), &proto)
}

// process the latest dynamic set of namespaces from the watch.
// this acquires the exclusive lock to update nextNamespaces.
func (n *namespaces) process(value interface{}) error {
	n.Lock()
	defer n.Unlock()
	var ok bool
	n.nextNamespaces, ok = value.(rules.Namespaces)
	if !ok {
		return errors.New("expected rules.Namespaces")
	}
	n.hasNext = true
	return nil
}

// Reset the namespaces so it can be used by another write request.
// this acquires the exclusive lock to update nextNamespaces.
func (n *namespaces) Reset() error {
	n.Lock()
	hasNext := n.hasNext
	nextNamespaces := n.nextNamespaces
	n.Unlock()
	// no update since the last call, nothing to do.
	if !hasNext {
		for _, r := range n.rules.Iter() {
			r.Value().Reset()
		}
		return nil
	}
	var (
		watchWg  sync.WaitGroup
		multiErr xerrors.MultiError
		errLock  sync.Mutex
		nextMap  = make(map[string]rules.Namespace)
		version  = nextNamespaces.Version()
	)

	for _, elem := range nextNamespaces.Namespaces() {
		nextMap[string(elem.Name())] = elem
		nsName, snapshots := elem.Name(), elem.Snapshots()
		ruleSet, exists := n.rules.Get(elem.Name())
		if !exists {
			instrumentOpts := n.opts.InstrumentOptions()
			ruleSetScope := instrumentOpts.MetricsScope().SubScope("ruleset")
			ruleSetOpts := n.opts.SetInstrumentOptions(instrumentOpts.SetMetricsScope(ruleSetScope))
			ruleSetKey := n.ruleSetKeyFn(elem.Name())
			ruleSet = newRuleSet(nsName, ruleSetKey, ruleSetOpts)
			n.rules.Set(elem.Name(), ruleSet)
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
		_, exists := nextMap[string(entry.Key())]
		if exists {
			entry.Value().Reset()
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
	n.hasNext = false
	return nil
}
