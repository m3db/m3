package attribution

import (
	"time"

	"github.com/m3db/m3/src/metrics/attribution/rules"
	"github.com/m3db/m3/src/metrics/filters"
	"github.com/m3db/m3/src/x/instrument"
)

// Options is a set of options for the attributor.
type Options interface {
	TagFilterOptions() filters.TagsFilterOptions
	SetTagFilterOptions(tf filters.TagsFilterOptions) Options

	InstrumentOptions() instrument.Options
	SetInstrumentOptions(iopts instrument.Options) Options

	DefaultRuleMatcher() RuleMatcher
	SetDefaultRuleMatcher(m RuleMatcher) Options

	// SetCustomRulesKVKey sets the key for the custom rules in KV store.
	SetCustomRulesKVKey(key string) Options
	CustomRulesKVKey() string

	// SetKVWatchInitTimeout sets the timeout for the initial KV watch.
	SetKVWatchInitTimeout(timeout time.Duration) Options
	KVWatchInitTimeout() time.Duration

	// RulesService returns the rules service.
	RulesService() rules.Service
	SetRulesService(service rules.Service) Options
}

type options struct {
	rulesService       rules.Service
	tagFilterOptions   filters.TagsFilterOptions
	instrumentOpts     instrument.Options
	defaultRuleMatcher RuleMatcher
	customRulesKVKey   string
	kvWatchInitTimeout time.Duration
}

// NewOptions creates a new set of options.
func NewOptions() Options {
	return &options{}
}

func (o *options) TagFilterOptions() filters.TagsFilterOptions {
	return o.tagFilterOptions
}

func (o *options) SetTagFilterOptions(tf filters.TagsFilterOptions) Options {
	opts := *o
	opts.tagFilterOptions = tf
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *options) SetInstrumentOptions(iopts instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = iopts
	return &opts
}

func (o *options) DefaultRuleMatcher() RuleMatcher {
	return o.defaultRuleMatcher
}

func (o *options) SetDefaultRuleMatcher(ruleMatcher RuleMatcher) Options {
	opts := *o
	opts.defaultRuleMatcher = ruleMatcher
	return &opts
}

func (o *options) SetCustomRulesKVKey(key string) Options {
	opts := *o
	opts.customRulesKVKey = key
	return &opts
}

func (o *options) CustomRulesKVKey() string {
	return o.customRulesKVKey
}

func (o *options) SetKVWatchInitTimeout(timeout time.Duration) Options {
	opts := *o
	opts.kvWatchInitTimeout = timeout
	return &opts
}

func (o *options) KVWatchInitTimeout() time.Duration {
	return o.kvWatchInitTimeout
}

func (o *options) RulesService() rules.Service {
	return o.rulesService
}

func (o *options) SetRulesService(service rules.Service) Options {
	opts := *o
	opts.rulesService = service
	return &opts
}
