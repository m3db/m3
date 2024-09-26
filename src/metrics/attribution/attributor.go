package attribution

import (
	"errors"
	"sync"

	"github.com/m3db/m3/src/metrics/attribution/rules"
	"github.com/m3db/m3/src/metrics/filters"
	"github.com/m3db/m3/src/x/watch"
)

// Attributor is responsible for attributing metrics to namespaces.
type Attributor interface {
	// Match return a list of namespaces that the metricID should
	// be attributed to.
	Match(metricID []byte) ([]rules.Result, error)

	// Update updates the attributor with new namespaces and their tag filters.
	Update(ruleSet rules.RuleSet) error

	// Init initializes the Attributor.
	Init() error

	// Close closes the Attributor.
	Close()
}

type attributor struct {
	matcherLock    sync.RWMutex
	w              watch.Value
	ruleService    rules.Service
	defaultMatcher RuleMatcher
	customMatcher  RuleMatcher
	opts           Options
	processFn      watch.ProcessFn
}

// NewAttributor creates a new Attributor.
func NewAttributor(opts Options) Attributor {
	a := &attributor{
		opts:           opts,
		ruleService:    opts.RulesService(),
		defaultMatcher: opts.DefaultRuleMatcher(),
	}
	a.processFn = a.process
	return a
}

func (a *attributor) Match(metricID []byte) ([]rules.Result, error) {
	var (
		ruleMap map[string]rules.Result
	)

	var customMatcher RuleMatcher
	{
		a.matcherLock.RLock()
		customMatcher = a.customMatcher
		a.matcherLock.RUnlock()
	}

	ruleMap = make(map[string]rules.Result)
	customMatchedRules, err := customMatcher.Match(metricID)
	if err != nil {
		return nil, err
	}

	for _, rule := range customMatchedRules {
		if rule.Rule == nil {
			return nil, errors.New("custom rule is nil")
		}
		ruleMap[rule.Rule.Name()] = rule
	}

	defaultMatchedRules, err := a.defaultMatcher.Match(metricID)
	if err != nil {
		return nil, err
	}

	for _, rule := range defaultMatchedRules {
		if rule.Rule == nil {
			return nil, errors.New("default rule is nil")
		}
		ruleMap[rule.Rule.Name()] = rule
	}

	rules := make([]rules.Result, 0, len(ruleMap))
	for _, rule := range ruleMap {
		rules = append(rules, rule)
	}

	return rules, nil
}

func (a *attributor) Update(ruleSet rules.RuleSet) error {
	return a.customMatcher.Update(ruleSet)
}

func (a *attributor) Init() error {
	newUpdatableFn := func() (watch.Updatable, error) {
		return a.ruleService.Watch(a.opts.CustomRulesKVKey())
	}
	getUpdateFn := func(value watch.Updatable) (interface{}, error) {
		rs, err := value.(rules.Watch).Get()
		if err != nil {
			return nil, err
		}
		return rs, nil
	}
	wOpts := watch.NewOptions().
		SetNewUpdatableFn(newUpdatableFn).
		SetGetUpdateFn(getUpdateFn).
		SetProcessFn(a.processFn).
		SetInitWatchTimeout(a.opts.KVWatchInitTimeout()).
		SetInstrumentOptions(a.opts.InstrumentOptions()).
		SetKey(a.opts.CustomRulesKVKey())

	w := watch.NewValue(wOpts)
	if err := w.Watch(); err != nil {
		return err
	}
	a.w = w
	return nil
}

func (a *attributor) Close() {
	a.w.Unwatch()
}

func (a *attributor) process(value interface{}) error {
	rs, ok := value.(rules.RuleSet)
	if !ok {
		return nil
	}

	ruleSet := rs
	customMatcher, err := NewCustomRuleMatcher(
		filters.TagMatchOptions{
			NameAndTagsFn:       a.opts.TagFilterOptions().NameAndTagsFn,
			SortedTagIteratorFn: a.opts.TagFilterOptions().SortedTagIteratorFn,
		},
		a.opts.InstrumentOptions(),
	)
	if err != nil {
		return err
	}

	{
		a.matcherLock.Lock()
		a.customMatcher = customMatcher
		a.matcherLock.Unlock()
	}

	if err := a.Update(ruleSet); err != nil {
		return err
	}

	return nil
}
