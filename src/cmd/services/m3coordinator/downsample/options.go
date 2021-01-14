// Copyright (c) 2018 Uber Technologies, Inc.
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

package downsample

import (
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/aggregator/aggregator/handler"
	"github.com/m3db/m3/src/aggregator/client"
	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/cluster/placement"
	placementservice "github.com/m3db/m3/src/cluster/placement/service"
	placementstorage "github.com/m3db/m3/src/cluster/placement/storage"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/filters"
	"github.com/m3db/m3/src/metrics/generated/proto/aggregationpb"
	"github.com/m3db/m3/src/metrics/generated/proto/pipelinepb"
	"github.com/m3db/m3/src/metrics/generated/proto/rulepb"
	"github.com/m3db/m3/src/metrics/generated/proto/transformationpb"
	"github.com/m3db/m3/src/metrics/matcher"
	"github.com/m3db/m3/src/metrics/matcher/cache"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/pipeline/applied"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/rules"
	ruleskv "github.com/m3db/m3/src/metrics/rules/store/kv"
	"github.com/m3db/m3/src/metrics/rules/view"
	"github.com/m3db/m3/src/metrics/transformation"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xio "github.com/m3db/m3/src/x/io"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
	xsync "github.com/m3db/m3/src/x/sync"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/pborman/uuid"
)

const (
	instanceID                     = "downsampler_local"
	placementKVKey                 = "/placement"
	defaultConfigInMemoryNamespace = "default"
	replicationFactor              = 1
	defaultStorageFlushConcurrency = 20000
	defaultOpenTimeout             = 10 * time.Second
	defaultBufferFutureTimedMetric = time.Minute
	defaultVerboseErrors           = true
	// defaultMatcherCacheCapacity sets the default matcher cache
	// capacity to zero so that the cache is turned off.
	// This is due to discovering that there is a lot of contention
	// used by the cache and the fact that most coordinators are used
	// in a stateless manner with a central deployment which in turn
	// leads to an extremely low cache hit ratio anyway.
	defaultMatcherCacheCapacity = 0
)

var (
	numShards           = runtime.NumCPU()
	defaultNamespaceTag = metric.M3MetricsPrefixString + "_namespace__"

	errNoStorage                    = errors.New("downsampling enabled with storage not set")
	errNoClusterClient              = errors.New("downsampling enabled with cluster client not set")
	errNoRulesStore                 = errors.New("downsampling enabled with rules store not set")
	errNoClockOptions               = errors.New("downsampling enabled with clock options not set")
	errNoInstrumentOptions          = errors.New("downsampling enabled with instrument options not set")
	errNoTagEncoderOptions          = errors.New("downsampling enabled with tag encoder options not set")
	errNoTagDecoderOptions          = errors.New("downsampling enabled with tag decoder options not set")
	errNoTagEncoderPoolOptions      = errors.New("downsampling enabled with tag encoder pool options not set")
	errNoTagDecoderPoolOptions      = errors.New("downsampling enabled with tag decoder pool options not set")
	errNoMetricsAppenderPoolOptions = errors.New("downsampling enabled with metrics appender pool options not set")
	errRollupRuleNoTransforms       = errors.New("rollup rule has no transforms set")
)

// CustomRuleStoreFn is a function to swap the backend used for the rule stores.
type CustomRuleStoreFn func(clusterclient.Client, instrument.Options) (kv.TxnStore, error)

// DownsamplerOptions is a set of required downsampler options.
type DownsamplerOptions struct {
	Storage                    storage.Storage
	StorageFlushConcurrency    int
	ClusterClient              clusterclient.Client
	RulesKVStore               kv.Store
	ClusterNamespacesWatcher   m3.ClusterNamespacesWatcher
	NameTag                    string
	ClockOptions               clock.Options
	InstrumentOptions          instrument.Options
	TagEncoderOptions          serialize.TagEncoderOptions
	TagDecoderOptions          serialize.TagDecoderOptions
	TagEncoderPoolOptions      pool.ObjectPoolOptions
	TagDecoderPoolOptions      pool.ObjectPoolOptions
	OpenTimeout                time.Duration
	TagOptions                 models.TagOptions
	MetricsAppenderPoolOptions pool.ObjectPoolOptions
	RWOptions                  xio.Options
}

// AutoMappingRule is a mapping rule to apply to metrics.
type AutoMappingRule struct {
	Aggregations []aggregation.Type
	Policies     policy.StoragePolicies
}

// NewAutoMappingRules generates mapping rules from cluster namespaces.
func NewAutoMappingRules(namespaces []m3.ClusterNamespace) ([]AutoMappingRule, error) {
	autoMappingRules := make([]AutoMappingRule, 0, len(namespaces))
	for _, namespace := range namespaces {
		opts := namespace.Options()
		attrs := opts.Attributes()
		if attrs.MetricsType != storagemetadata.AggregatedMetricsType {
			continue
		}

		downsampleOpts, err := opts.DownsampleOptions()
		if err != nil {
			errFmt := "unable to resolve downsample options for namespace: %v"
			return nil, fmt.Errorf(errFmt, namespace.NamespaceID().String())
		}
		if downsampleOpts.All {
			storagePolicy := policy.NewStoragePolicy(attrs.Resolution,
				xtime.Second, attrs.Retention)
			autoMappingRules = append(autoMappingRules, AutoMappingRule{
				// NB(r): By default we will apply just keep all last values
				// since coordinator only uses downsampling with Prometheus
				// remote write endpoint.
				// More rich static configuration mapping rules can be added
				// in the future but they are currently not required.
				Aggregations: []aggregation.Type{aggregation.Last},
				Policies:     policy.StoragePolicies{storagePolicy},
			})
		}
	}
	return autoMappingRules, nil
}

// StagedMetadatas returns the corresponding staged metadatas for this mapping rule.
func (r AutoMappingRule) StagedMetadatas() (metadata.StagedMetadatas, error) {
	aggID, err := aggregation.CompressTypes(r.Aggregations...)
	if err != nil {
		return nil, err
	}

	return metadata.StagedMetadatas{
		metadata.StagedMetadata{
			Metadata: metadata.Metadata{
				Pipelines: metadata.PipelineMetadatas{
					metadata.PipelineMetadata{
						AggregationID:   aggID,
						StoragePolicies: r.Policies,
					},
				},
			},
		},
	}, nil
}

// Validate validates the dynamic downsampling options.
func (o DownsamplerOptions) validate() error {
	if o.Storage == nil {
		return errNoStorage
	}
	if o.ClusterClient == nil {
		return errNoClusterClient
	}
	if o.RulesKVStore == nil {
		return errNoRulesStore
	}
	if o.ClockOptions == nil {
		return errNoClockOptions
	}
	if o.InstrumentOptions == nil {
		return errNoInstrumentOptions
	}
	if o.TagEncoderOptions == nil {
		return errNoTagEncoderOptions
	}
	if o.TagDecoderOptions == nil {
		return errNoTagDecoderOptions
	}
	if o.TagEncoderPoolOptions == nil {
		return errNoTagEncoderPoolOptions
	}
	if o.TagDecoderPoolOptions == nil {
		return errNoTagDecoderPoolOptions
	}
	if o.MetricsAppenderPoolOptions == nil {
		return errNoMetricsAppenderPoolOptions
	}
	return nil
}

// agg will have one of aggregator or clientRemote set, the
// rest of the fields must not be nil.
type agg struct {
	aggregator   aggregator.Aggregator
	clientRemote client.Client

	clockOpts clock.Options
	matcher   matcher.Matcher
	pools     aggPools
}

// Configuration configurates a downsampler.
type Configuration struct {
	// Matcher is the configuration for the downsampler matcher.
	Matcher MatcherConfiguration `yaml:"matcher"`

	// Rules is a set of downsample rules. If set, this overrides any rules set
	// in the KV store (and the rules in KV store are not evaluated at all).
	Rules *RulesConfiguration `yaml:"rules"`

	// RemoteAggregator specifies that downsampling should be done remotely
	// by sending values to a remote m3aggregator cluster which then
	// can forward the aggregated values to stateless m3coordinator backends.
	RemoteAggregator *RemoteAggregatorConfiguration `yaml:"remoteAggregator"`

	// AggregationTypes configs the aggregation types.
	AggregationTypes *aggregation.TypesConfiguration `yaml:"aggregationTypes"`

	// Pool of counter elements.
	CounterElemPool pool.ObjectPoolConfiguration `yaml:"counterElemPool"`

	// Pool of timer elements.
	TimerElemPool pool.ObjectPoolConfiguration `yaml:"timerElemPool"`

	// Pool of gauge elements.
	GaugeElemPool pool.ObjectPoolConfiguration `yaml:"gaugeElemPool"`

	// BufferPastLimits specifies the buffer past limits.
	BufferPastLimits []BufferPastLimitConfiguration `yaml:"bufferPastLimits"`

	// EntryTTL determines how long an entry remains alive before it may be expired due to inactivity.
	EntryTTL time.Duration `yaml:"entryTTL"`
}

// MatcherConfiguration is the configuration for the rule matcher.
type MatcherConfiguration struct {
	// Cache if non-zero will set the capacity of the rules matching cache.
	Cache MatcherCacheConfiguration `yaml:"cache"`
	// NamespaceTag defines the namespace tag to use to select rules
	// namespace to evaluate against. Default is "__m3_namespace__".
	NamespaceTag string `yaml:"namespaceTag"`
}

// MatcherCacheConfiguration is the configuration for the rule matcher cache.
type MatcherCacheConfiguration struct {
	// Capacity if set the capacity of the rules matching cache.
	Capacity *int `yaml:"capacity"`
}

// RulesConfiguration is a set of rules configuration to use for downsampling.
type RulesConfiguration struct {
	// MappingRules are mapping rules that set retention and resolution
	// for metrics given a filter to match metrics against.
	MappingRules []MappingRuleConfiguration `yaml:"mappingRules"`

	// RollupRules are rollup rules that sets specific aggregations for sets
	// of metrics given a filter to match metrics against.
	RollupRules []RollupRuleConfiguration `yaml:"rollupRules"`
}

// MappingRuleConfiguration is a mapping rule configuration.
type MappingRuleConfiguration struct {
	// Filter is a string separated filter of label name to label value
	// glob patterns to filter the mapping rule to.
	// e.g. "app:*nginx* foo:bar baz:qux*qaz*"
	Filter string `yaml:"filter"`

	// Aggregations is the aggregations to apply to the set of metrics.
	// One of:
	// - "Last"
	// - "Min"
	// - "Max"
	// - "Mean"
	// - "Median"
	// - "Count"
	// - "Sum"
	// - "SumSq"
	// - "Stdev"
	// - "P10"
	// - "P20"
	// - "P30"
	// - "P40"
	// - "P50"
	// - "P60"
	// - "P70"
	// - "P80"
	// - "P90"
	// - "P95"
	// - "P99"
	// - "P999"
	// - "P9999"
	Aggregations []aggregation.Type `yaml:"aggregations"`

	// StoragePolicies are retention/resolution storage policies at which to
	// keep matched metrics.
	StoragePolicies []StoragePolicyConfiguration `yaml:"storagePolicies"`

	// Drop specifies to drop any metrics that match the filter rather than
	// keeping them with a storage policy.
	Drop bool `yaml:"drop"`

	// Tags are the tags to be added to the metric while applying the mapping
	// rule. Users are free to add name/value combinations to the metric. The
	// coordinator also supports certain first class tags which will augment
	// the metric with coordinator generated tag values.
	// __m3_graphite_aggregation__ as a tag will augment the metric with an
	// aggregation tag which is required for graphite. If a metric is of the
	// form {__g0__:stats __g1__:metric __g2__:timer} and we have configured
	// a P95 aggregation, this option will add __g3__:P95 to the metric.
	Tags []Tag `yaml:"tags"`

	// Optional fields follow.

	// Name is optional.
	Name string `yaml:"name"`
}

// Tag is structure describing tags as used by mapping rule configuration.
type Tag struct {
	// Name is the tag name.
	Name string `yaml:"name"`
	// Value is the tag value.
	Value string `yaml:"value"`
}

// Rule returns the mapping rule for the mapping rule configuration.
func (r MappingRuleConfiguration) Rule() (view.MappingRule, error) {
	id := uuid.New()
	name := r.Name
	if name == "" {
		name = id
	}
	filter := r.Filter

	aggID, err := aggregation.CompressTypes(r.Aggregations...)
	if err != nil {
		return view.MappingRule{}, err
	}

	storagePolicies, err := StoragePolicyConfigurations(r.StoragePolicies).StoragePolicies()
	if err != nil {
		return view.MappingRule{}, err
	}

	var drop policy.DropPolicy
	if r.Drop {
		drop = policy.DropIfOnlyMatch
	}

	tags := make([]models.Tag, 0, len(r.Tags))
	for _, tag := range r.Tags {
		tags = append(tags, models.Tag{
			Name:  []byte(tag.Name),
			Value: []byte(tag.Value),
		})
	}

	return view.MappingRule{
		ID:              id,
		Name:            name,
		Filter:          filter,
		AggregationID:   aggID,
		StoragePolicies: storagePolicies,
		DropPolicy:      drop,
		Tags:            tags,
	}, nil
}

// StoragePolicyConfiguration is the storage policy to apply to a set of metrics.
type StoragePolicyConfiguration struct {
	Resolution time.Duration `yaml:"resolution"`
	Retention  time.Duration `yaml:"retention"`
}

// StoragePolicy returns the corresponding storage policy.
func (p StoragePolicyConfiguration) StoragePolicy() (policy.StoragePolicy, error) {
	return policy.ParseStoragePolicy(p.String())
}

func (p StoragePolicyConfiguration) String() string {
	return fmt.Sprintf("%s:%s", p.Resolution.String(), p.Retention.String())
}

// StoragePolicyConfigurations are a set of storage policy configurations.
type StoragePolicyConfigurations []StoragePolicyConfiguration

// StoragePolicies returns storage policies.
func (p StoragePolicyConfigurations) StoragePolicies() (policy.StoragePolicies, error) {
	storagePolicies := make(policy.StoragePolicies, 0, len(p))
	for _, policy := range p {
		value, err := policy.StoragePolicy()
		if err != nil {
			return nil, err
		}
		storagePolicies = append(storagePolicies, value)
	}
	return storagePolicies, nil
}

// RollupRuleConfiguration is a rollup rule configuration.
type RollupRuleConfiguration struct {
	// Filter is a space separated filter of label name to label value glob
	// patterns to which to filter the mapping rule.
	// e.g. "app:*nginx* foo:bar baz:qux*qaz*"
	Filter string `yaml:"filter"`

	// Transforms are a set of of rollup rule transforms.
	Transforms []TransformConfiguration `yaml:"transforms"`

	// StoragePolicies are retention/resolution storage policies at which to keep
	// the matched metrics.
	StoragePolicies []StoragePolicyConfiguration `yaml:"storagePolicies"`

	// Optional fields follow.

	// Name is optional.
	Name string `yaml:"name"`
}

// Rule returns the rollup rule for the rollup rule configuration.
func (r RollupRuleConfiguration) Rule() (view.RollupRule, error) {
	id := uuid.New()
	name := r.Name
	if name == "" {
		name = id
	}
	filter := r.Filter

	storagePolicies, err := StoragePolicyConfigurations(r.StoragePolicies).
		StoragePolicies()
	if err != nil {
		return view.RollupRule{}, err
	}

	ops := make([]pipeline.OpUnion, 0, len(r.Transforms))
	for _, elem := range r.Transforms {
		// TODO: make sure only one of "Rollup" or "Aggregate" or "Transform" is not nil
		switch {
		case elem.Rollup != nil:
			cfg := elem.Rollup
			aggregationTypes, err := AggregationTypes(cfg.Aggregations).Proto()
			if err != nil {
				return view.RollupRule{}, err
			}
			op, err := pipeline.NewOpUnionFromProto(pipelinepb.PipelineOp{
				Type: pipelinepb.PipelineOp_ROLLUP,
				Rollup: &pipelinepb.RollupOp{
					NewName:          cfg.MetricName,
					Tags:             cfg.GroupBy,
					AggregationTypes: aggregationTypes,
				},
			})
			if err != nil {
				return view.RollupRule{}, err
			}
			ops = append(ops, op)
		case elem.Aggregate != nil:
			cfg := elem.Aggregate
			aggregationType, err := cfg.Type.Proto()
			if err != nil {
				return view.RollupRule{}, err
			}
			op, err := pipeline.NewOpUnionFromProto(pipelinepb.PipelineOp{
				Type: pipelinepb.PipelineOp_AGGREGATION,
				Aggregation: &pipelinepb.AggregationOp{
					Type: aggregationType,
				},
			})
			if err != nil {
				return view.RollupRule{}, err
			}
			ops = append(ops, op)
		case elem.Transform != nil:
			cfg := elem.Transform
			var transformType transformationpb.TransformationType
			err := cfg.Type.ToProto(&transformType)
			if err != nil {
				return view.RollupRule{}, err
			}
			op, err := pipeline.NewOpUnionFromProto(pipelinepb.PipelineOp{
				Type: pipelinepb.PipelineOp_TRANSFORMATION,
				Transformation: &pipelinepb.TransformationOp{
					Type: transformType,
				},
			})
			if err != nil {
				return view.RollupRule{}, err
			}
			ops = append(ops, op)
		}
	}

	if len(ops) == 0 {
		return view.RollupRule{}, errRollupRuleNoTransforms
	}

	targetPipeline := pipeline.NewPipeline(ops)

	targets := []view.RollupTarget{
		{
			Pipeline:        targetPipeline,
			StoragePolicies: storagePolicies,
		},
	}

	return view.RollupRule{
		ID:      id,
		Name:    name,
		Filter:  filter,
		Targets: targets,
	}, nil
}

// TransformConfiguration is a rollup rule transform operation, only one
// single operation is allowed to be specified on any one transform configuration.
type TransformConfiguration struct {
	Rollup    *RollupOperationConfiguration    `yaml:"rollup"`
	Aggregate *AggregateOperationConfiguration `yaml:"aggregate"`
	Transform *TransformOperationConfiguration `yaml:"transform"`
}

// RollupOperationConfiguration is a rollup operation.
type RollupOperationConfiguration struct {
	// MetricName is the name of the new metric that is emitted after
	// the rollup is applied with its aggregations and group by's.
	MetricName string `yaml:"metricName"`

	// GroupBy is a set of labels to group by, only these remain on the
	// new metric name produced by the rollup operation.
	GroupBy []string `yaml:"groupBy"`

	// Aggregations is a set of aggregate operations to perform.
	Aggregations []aggregation.Type `yaml:"aggregations"`
}

// AggregateOperationConfiguration is an aggregate operation.
type AggregateOperationConfiguration struct {
	// Type is an aggregation operation type.
	Type aggregation.Type `yaml:"type"`
}

// TransformOperationConfiguration is a transform operation.
type TransformOperationConfiguration struct {
	// Type is a transformation operation type.
	Type transformation.Type `yaml:"type"`
}

// AggregationTypes is a set of aggregation types.
type AggregationTypes []aggregation.Type

// Proto returns a set of aggregation types as their protobuf value.
func (t AggregationTypes) Proto() ([]aggregationpb.AggregationType, error) {
	result := make([]aggregationpb.AggregationType, 0, len(t))
	for _, elem := range t {
		value, err := elem.Proto()
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, nil
}

// RemoteAggregatorConfiguration specifies a remote aggregator
// to use for downsampling.
type RemoteAggregatorConfiguration struct {
	// Client is the remote aggregator client.
	Client client.Configuration `yaml:"client"`
	// clientOverride can be used in tests to test initializing a mock client.
	clientOverride client.Client
}

func (c RemoteAggregatorConfiguration) newClient(
	kvClient clusterclient.Client,
	clockOpts clock.Options,
	instrumentOpts instrument.Options,
	rwOpts xio.Options,
) (client.Client, error) {
	if c.clientOverride != nil {
		return c.clientOverride, nil
	}

	return c.Client.NewClient(kvClient, clockOpts, instrumentOpts, rwOpts)
}

// BufferPastLimitConfiguration specifies a custom buffer past limit
// for aggregation tiles.
type BufferPastLimitConfiguration struct {
	Resolution time.Duration `yaml:"resolution"`
	BufferPast time.Duration `yaml:"bufferPast"`
}

// NewDownsampler returns a new downsampler.
func (cfg Configuration) NewDownsampler(
	opts DownsamplerOptions,
) (Downsampler, error) {
	agg, err := cfg.newAggregator(opts)
	if err != nil {
		return nil, err
	}

	return newDownsampler(downsamplerOptions{
		opts: opts,
		agg:  agg,
	})
}

func (cfg Configuration) newAggregator(o DownsamplerOptions) (agg, error) {
	// Validate options first.
	if err := o.validate(); err != nil {
		return agg{}, err
	}

	var (
		storageFlushConcurrency = defaultStorageFlushConcurrency
		clockOpts               = o.ClockOptions
		instrumentOpts          = o.InstrumentOptions
		scope                   = instrumentOpts.MetricsScope()
		logger                  = instrumentOpts.Logger()
		openTimeout             = defaultOpenTimeout
		namespaceTag            = defaultNamespaceTag
	)
	if o.StorageFlushConcurrency > 0 {
		storageFlushConcurrency = o.StorageFlushConcurrency
	}
	if o.OpenTimeout > 0 {
		openTimeout = o.OpenTimeout
	}
	if cfg.Matcher.NamespaceTag != "" {
		namespaceTag = cfg.Matcher.NamespaceTag
	}

	pools := o.newAggregatorPools()
	ruleSetOpts := o.newAggregatorRulesOptions(pools)

	matcherOpts := matcher.NewOptions().
		SetClockOptions(clockOpts).
		SetInstrumentOptions(instrumentOpts).
		SetRuleSetOptions(ruleSetOpts).
		SetKVStore(o.RulesKVStore).
		SetNamespaceTag([]byte(namespaceTag))

	// NB(r): If rules are being explicitly set in config then we are
	// going to use an in memory KV store for rules and explicitly set them up.
	if cfg.Rules != nil {
		logger.Debug("registering downsample rules from config, not using KV")
		kvTxnMemStore := mem.NewStore()

		// Initialize the namespaces
		_, err := kvTxnMemStore.Set(matcherOpts.NamespacesKey(), &rulepb.Namespaces{})
		if err != nil {
			return agg{}, err
		}

		rulesetKeyFmt := matcherOpts.RuleSetKeyFn()([]byte("%s"))
		rulesStoreOpts := ruleskv.NewStoreOptions(matcherOpts.NamespacesKey(),
			rulesetKeyFmt, nil)
		rulesStore := ruleskv.NewStore(kvTxnMemStore, rulesStoreOpts)

		ruleNamespaces, err := rulesStore.ReadNamespaces()
		if err != nil {
			return agg{}, err
		}

		updateMetadata := rules.NewRuleSetUpdateHelper(0).
			NewUpdateMetadata(time.Now().UnixNano(), "config")

		// Create the default namespace, always not present since in-memory.
		_, err = ruleNamespaces.AddNamespace(defaultConfigInMemoryNamespace,
			updateMetadata)
		if err != nil {
			return agg{}, err
		}

		// Create the ruleset in the default namespace.
		rs := rules.NewEmptyRuleSet(defaultConfigInMemoryNamespace,
			updateMetadata)
		for _, mappingRule := range cfg.Rules.MappingRules {
			rule, err := mappingRule.Rule()
			if err != nil {
				return agg{}, err
			}

			_, err = rs.AddMappingRule(rule, updateMetadata)
			if err != nil {
				return agg{}, err
			}
		}

		for _, rollupRule := range cfg.Rules.RollupRules {
			rule, err := rollupRule.Rule()
			if err != nil {
				return agg{}, err
			}

			_, err = rs.AddRollupRule(rule, updateMetadata)
			if err != nil {
				return agg{}, err
			}
		}

		if err := rulesStore.WriteAll(ruleNamespaces, rs); err != nil {
			return agg{}, err
		}

		// Set the rules KV store to the in-memory one we created to
		// store the rules we created from config.
		// This makes sure that other components using rules KV store points to
		// the in-memory store that has the rules created from config.
		matcherOpts = matcherOpts.SetKVStore(kvTxnMemStore)
	}

	matcherCacheCapacity := defaultMatcherCacheCapacity
	if v := cfg.Matcher.Cache.Capacity; v != nil {
		matcherCacheCapacity = *v
	}

	matcher, err := o.newAggregatorMatcher(matcherOpts, matcherCacheCapacity)
	if err != nil {
		return agg{}, err
	}

	if remoteAgg := cfg.RemoteAggregator; remoteAgg != nil {
		// If downsampling setup to use a remote aggregator instead of local
		// aggregator, set that up instead.
		scope := instrumentOpts.MetricsScope().SubScope("remote-aggregator-client")
		iOpts := instrumentOpts.SetMetricsScope(scope)
		rwOpts := o.RWOptions
		if rwOpts == nil {
			logger.Info("no rw options set, using default")
			rwOpts = xio.NewOptions()
		}

		client, err := remoteAgg.newClient(o.ClusterClient, clockOpts, iOpts, rwOpts)
		if err != nil {
			err = fmt.Errorf("could not create remote aggregator client: %v", err)
			return agg{}, err
		}
		if err := client.Init(); err != nil {
			return agg{}, fmt.Errorf("could not initialize remote aggregator client: %v", err)
		}

		return agg{
			clientRemote: client,
			matcher:      matcher,
			pools:        pools,
		}, nil
	}

	serviceID := services.NewServiceID().
		SetEnvironment("production").
		SetName("downsampler").
		SetZone("embedded")

	localKVStore := mem.NewStore()

	placementManager, err := o.newAggregatorPlacementManager(serviceID, localKVStore)
	if err != nil {
		return agg{}, err
	}

	flushTimesManager := aggregator.NewFlushTimesManager(
		aggregator.NewFlushTimesManagerOptions().
			SetFlushTimesStore(localKVStore))

	electionManager, err := o.newAggregatorElectionManager(serviceID,
		placementManager, flushTimesManager)
	if err != nil {
		return agg{}, err
	}

	flushManager, flushHandler := o.newAggregatorFlushManagerAndHandler(serviceID,
		placementManager, flushTimesManager, electionManager, instrumentOpts,
		storageFlushConcurrency, pools)

	bufferPastLimits := defaultBufferPastLimits
	if numLimitsCfg := len(cfg.BufferPastLimits); numLimitsCfg > 0 {
		// Allow overrides from config.
		bufferPastLimits = make([]bufferPastLimit, 0, numLimitsCfg)
		for _, limit := range cfg.BufferPastLimits {
			bufferPastLimits = append(bufferPastLimits, bufferPastLimit{
				upperBound: limit.Resolution,
				bufferPast: limit.BufferPast,
			})
		}
	}

	bufferForPastTimedMetricFn := func(tile time.Duration) time.Duration {
		return bufferForPastTimedMetric(bufferPastLimits, tile)
	}

	maxAllowedForwardingDelayFn := func(tile time.Duration, numForwardedTimes int) time.Duration {
		return maxAllowedForwardingDelay(bufferPastLimits, tile, numForwardedTimes)
	}

	// Finally construct all options.
	aggregatorOpts := aggregator.NewOptions().
		SetClockOptions(clockOpts).
		SetInstrumentOptions(instrumentOpts).
		SetDefaultStoragePolicies(nil).
		SetMetricPrefix(nil).
		SetCounterPrefix(nil).
		SetGaugePrefix(nil).
		SetTimerPrefix(nil).
		SetPlacementManager(placementManager).
		SetFlushTimesManager(flushTimesManager).
		SetElectionManager(electionManager).
		SetFlushManager(flushManager).
		SetFlushHandler(flushHandler).
		SetBufferForPastTimedMetricFn(bufferForPastTimedMetricFn).
		SetBufferForFutureTimedMetric(defaultBufferFutureTimedMetric).
		SetMaxAllowedForwardingDelayFn(maxAllowedForwardingDelayFn).
		SetVerboseErrors(defaultVerboseErrors)

	if cfg.EntryTTL != 0 {
		aggregatorOpts = aggregatorOpts.SetEntryTTL(cfg.EntryTTL)
	}

	if cfg.AggregationTypes != nil {
		aggTypeOpts, err := cfg.AggregationTypes.NewOptions(instrumentOpts)
		if err != nil {
			return agg{}, err
		}
		aggregatorOpts = aggregatorOpts.SetAggregationTypesOptions(aggTypeOpts)
	}

	// Set counter elem pool.
	counterElemPoolOpts := cfg.CounterElemPool.NewObjectPoolOptions(
		instrumentOpts.SetMetricsScope(scope.SubScope("counter-elem-pool")),
	)
	counterElemPool := aggregator.NewCounterElemPool(counterElemPoolOpts)
	aggregatorOpts = aggregatorOpts.SetCounterElemPool(counterElemPool)
	counterElemPool.Init(func() *aggregator.CounterElem {
		return aggregator.MustNewCounterElem(
			nil,
			policy.EmptyStoragePolicy,
			aggregation.DefaultTypes,
			applied.DefaultPipeline,
			0,
			aggregator.WithPrefixWithSuffix,
			aggregatorOpts,
		)
	})

	// Set timer elem pool.
	timerElemPoolOpts := cfg.TimerElemPool.NewObjectPoolOptions(
		instrumentOpts.SetMetricsScope(scope.SubScope("timer-elem-pool")),
	)
	timerElemPool := aggregator.NewTimerElemPool(timerElemPoolOpts)
	aggregatorOpts = aggregatorOpts.SetTimerElemPool(timerElemPool)
	timerElemPool.Init(func() *aggregator.TimerElem {
		return aggregator.MustNewTimerElem(
			nil,
			policy.EmptyStoragePolicy,
			aggregation.DefaultTypes,
			applied.DefaultPipeline,
			0,
			aggregator.WithPrefixWithSuffix,
			aggregatorOpts,
		)
	})

	// Set gauge elem pool.
	gaugeElemPoolOpts := cfg.GaugeElemPool.NewObjectPoolOptions(
		instrumentOpts.SetMetricsScope(scope.SubScope("gauge-elem-pool")),
	)
	gaugeElemPool := aggregator.NewGaugeElemPool(gaugeElemPoolOpts)
	aggregatorOpts = aggregatorOpts.SetGaugeElemPool(gaugeElemPool)
	gaugeElemPool.Init(func() *aggregator.GaugeElem {
		return aggregator.MustNewGaugeElem(
			nil,
			policy.EmptyStoragePolicy,
			aggregation.DefaultTypes,
			applied.DefaultPipeline,
			0,
			aggregator.WithPrefixWithSuffix,
			aggregatorOpts,
		)
	})

	adminAggClient := newAggregatorLocalAdminClient()
	aggregatorOpts = aggregatorOpts.SetAdminClient(adminAggClient)

	aggregatorInstance := aggregator.NewAggregator(aggregatorOpts)
	if err := aggregatorInstance.Open(); err != nil {
		return agg{}, err
	}

	// Update the local aggregator client with the active aggregator instance.
	// NB: Can't do this at construction time since needs to be passed as an
	// option to the aggregator constructor.
	adminAggClient.setAggregator(aggregatorInstance)

	// Wait until the aggregator becomes leader so we don't miss datapoints
	deadline := time.Now().Add(openTimeout)
	for {
		if !time.Now().Before(deadline) {
			return agg{}, fmt.Errorf("aggregator not promoted to leader after: %s",
				openTimeout.String())
		}
		if electionManager.ElectionState() == aggregator.LeaderState {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	return agg{
		aggregator: aggregatorInstance,
		matcher:    matcher,
		pools:      pools,
	}, nil
}

type aggPools struct {
	tagEncoderPool         serialize.TagEncoderPool
	tagDecoderPool         serialize.TagDecoderPool
	metricTagsIteratorPool serialize.MetricTagsIteratorPool
	metricsAppenderPool    *metricsAppenderPool
}

func (o DownsamplerOptions) newAggregatorPools() aggPools {
	tagEncoderPool := serialize.NewTagEncoderPool(o.TagEncoderOptions,
		o.TagEncoderPoolOptions)
	tagEncoderPool.Init()

	tagDecoderPool := serialize.NewTagDecoderPool(o.TagDecoderOptions,
		o.TagDecoderPoolOptions)
	tagDecoderPool.Init()

	metricTagsIteratorPool := serialize.NewMetricTagsIteratorPool(tagDecoderPool,
		o.TagDecoderPoolOptions)
	metricTagsIteratorPool.Init()

	metricsAppenderPool := newMetricsAppenderPool(o.MetricsAppenderPoolOptions)

	return aggPools{
		tagEncoderPool:         tagEncoderPool,
		tagDecoderPool:         tagDecoderPool,
		metricTagsIteratorPool: metricTagsIteratorPool,
		metricsAppenderPool:    metricsAppenderPool,
	}
}

func (o DownsamplerOptions) newAggregatorRulesOptions(pools aggPools) rules.Options {
	nameTag := defaultMetricNameTagName
	if o.NameTag != "" {
		nameTag = []byte(o.NameTag)
	}

	sortedTagIteratorFn := func(tagPairs []byte) id.SortedTagIterator {
		it := pools.metricTagsIteratorPool.Get()
		it.Reset(tagPairs)
		return it
	}

	tagsFilterOpts := filters.TagsFilterOptions{
		NameTagKey: nameTag,
		NameAndTagsFn: func(id []byte) ([]byte, []byte, error) {
			name, err := resolveEncodedTagsNameTag(id, nameTag)
			if err != nil && err != errNoMetricNameTag {
				return nil, nil, err
			}
			// ID is always the encoded tags for IDs in the downsampler
			tags := id
			return name, tags, nil
		},
		SortedTagIteratorFn: sortedTagIteratorFn,
	}

	isRollupIDFn := func(name []byte, tags []byte) bool {
		return isRollupID(tags, pools.metricTagsIteratorPool)
	}

	newRollupIDProviderPool := newRollupIDProviderPool(pools.tagEncoderPool,
		o.TagEncoderPoolOptions, ident.BytesID(nameTag))
	newRollupIDProviderPool.Init()

	newRollupIDFn := func(newName []byte, tagPairs []id.TagPair) []byte {
		rollupIDProvider := newRollupIDProviderPool.Get()
		id, err := rollupIDProvider.provide(newName, tagPairs)
		if err != nil {
			panic(err) // Encoding should never fail
		}
		rollupIDProvider.finalize()
		return id
	}

	return rules.NewOptions().
		SetTagsFilterOptions(tagsFilterOpts).
		SetNewRollupIDFn(newRollupIDFn).
		SetIsRollupIDFn(isRollupIDFn)
}

func (o DownsamplerOptions) newAggregatorMatcher(
	opts matcher.Options,
	capacity int,
) (matcher.Matcher, error) {
	var matcherCache cache.Cache
	if capacity > 0 {
		scope := opts.InstrumentOptions().MetricsScope().SubScope("matcher-cache")
		instrumentOpts := opts.InstrumentOptions().
			SetMetricsScope(scope)
		cacheOpts := cache.NewOptions().
			SetCapacity(capacity).
			SetClockOptions(opts.ClockOptions()).
			SetInstrumentOptions(instrumentOpts)
		matcherCache = cache.NewCache(cacheOpts)
	}

	return matcher.NewMatcher(matcherCache, opts)
}

func (o DownsamplerOptions) newAggregatorPlacementManager(
	serviceID services.ServiceID,
	localKVStore kv.Store,
) (aggregator.PlacementManager, error) {
	instance := placement.NewInstance().
		SetID(instanceID).
		SetWeight(1).
		SetEndpoint(instanceID)

	placementOpts := placement.NewOptions().
		SetIsStaged(true).
		SetShardStateMode(placement.StableShardStateOnly)

	placementSvc := placementservice.NewPlacementService(
		placementstorage.NewPlacementStorage(localKVStore, placementKVKey, placementOpts),
		placementservice.WithPlacementOptions(placementOpts))

	_, err := placementSvc.BuildInitialPlacement([]placement.Instance{instance}, numShards,
		replicationFactor)
	if err != nil {
		return nil, err
	}

	placementWatcherOpts := placement.NewStagedPlacementWatcherOptions().
		SetStagedPlacementKey(placementKVKey).
		SetStagedPlacementStore(localKVStore)
	placementWatcher := placement.NewStagedPlacementWatcher(placementWatcherOpts)
	placementManagerOpts := aggregator.NewPlacementManagerOptions().
		SetInstanceID(instanceID).
		SetStagedPlacementWatcher(placementWatcher)

	return aggregator.NewPlacementManager(placementManagerOpts), nil
}

func (o DownsamplerOptions) newAggregatorElectionManager(
	serviceID services.ServiceID,
	placementManager aggregator.PlacementManager,
	flushTimesManager aggregator.FlushTimesManager,
) (aggregator.ElectionManager, error) {
	leaderValue := instanceID
	campaignOpts, err := services.NewCampaignOptions()
	if err != nil {
		return nil, err
	}

	campaignOpts = campaignOpts.SetLeaderValue(leaderValue)

	leaderService := newLocalLeaderService(serviceID)

	electionManagerOpts := aggregator.NewElectionManagerOptions().
		SetCampaignOptions(campaignOpts).
		SetLeaderService(leaderService).
		SetPlacementManager(placementManager).
		SetFlushTimesManager(flushTimesManager)

	return aggregator.NewElectionManager(electionManagerOpts), nil
}

func (o DownsamplerOptions) newAggregatorFlushManagerAndHandler(
	serviceID services.ServiceID,
	placementManager aggregator.PlacementManager,
	flushTimesManager aggregator.FlushTimesManager,
	electionManager aggregator.ElectionManager,
	instrumentOpts instrument.Options,
	storageFlushConcurrency int,
	pools aggPools,
) (aggregator.FlushManager, handler.Handler) {
	flushManagerOpts := aggregator.NewFlushManagerOptions().
		SetPlacementManager(placementManager).
		SetFlushTimesManager(flushTimesManager).
		SetElectionManager(electionManager).
		SetJitterEnabled(false)
	flushManager := aggregator.NewFlushManager(flushManagerOpts)

	flushWorkers := xsync.NewWorkerPool(storageFlushConcurrency)
	flushWorkers.Init()
	handler := newDownsamplerFlushHandler(o.Storage, pools.metricTagsIteratorPool,
		flushWorkers, o.TagOptions, instrumentOpts)

	return flushManager, handler
}

// Force the local aggregator client to implement client.Client.
var _ client.AdminClient = (*aggregatorLocalAdminClient)(nil)

type aggregatorLocalAdminClient struct {
	agg aggregator.Aggregator
}

func newAggregatorLocalAdminClient() *aggregatorLocalAdminClient {
	return &aggregatorLocalAdminClient{}
}

func (c *aggregatorLocalAdminClient) setAggregator(agg aggregator.Aggregator) {
	c.agg = agg
}

// Init initializes the client.
func (c *aggregatorLocalAdminClient) Init() error {
	return fmt.Errorf("always initialized")
}

// WriteUntimedCounter writes untimed counter metrics.
func (c *aggregatorLocalAdminClient) WriteUntimedCounter(
	counter unaggregated.Counter,
	metadatas metadata.StagedMetadatas,
) error {
	return c.agg.AddUntimed(counter.ToUnion(), metadatas)
}

// WriteUntimedBatchTimer writes untimed batch timer metrics.
func (c *aggregatorLocalAdminClient) WriteUntimedBatchTimer(
	batchTimer unaggregated.BatchTimer,
	metadatas metadata.StagedMetadatas,
) error {
	return c.agg.AddUntimed(batchTimer.ToUnion(), metadatas)
}

// WriteUntimedGauge writes untimed gauge metrics.
func (c *aggregatorLocalAdminClient) WriteUntimedGauge(
	gauge unaggregated.Gauge,
	metadatas metadata.StagedMetadatas,
) error {
	return c.agg.AddUntimed(gauge.ToUnion(), metadatas)
}

// WriteTimed writes timed metrics.
func (c *aggregatorLocalAdminClient) WriteTimed(
	metric aggregated.Metric,
	metadata metadata.TimedMetadata,
) error {
	return c.agg.AddTimed(metric, metadata)
}

// WriteTimedWithStagedMetadatas writes timed metrics with staged metadatas.
func (c *aggregatorLocalAdminClient) WriteTimedWithStagedMetadatas(
	metric aggregated.Metric,
	metadatas metadata.StagedMetadatas,
) error {
	return c.agg.AddTimedWithStagedMetadatas(metric, metadatas)
}

// WriteForwarded writes forwarded metrics.
func (c *aggregatorLocalAdminClient) WriteForwarded(
	metric aggregated.ForwardedMetric,
	metadata metadata.ForwardMetadata,
) error {
	return c.agg.AddForwarded(metric, metadata)
}

// WritePassthrough writes passthrough metrics.
func (c *aggregatorLocalAdminClient) WritePassthrough(
	metric aggregated.Metric,
	storagePolicy policy.StoragePolicy,
) error {
	return c.agg.AddPassthrough(metric, storagePolicy)
}

// Flush flushes any remaining data buffered by the client.
func (c *aggregatorLocalAdminClient) Flush() error {
	return nil
}

// Close closes the client.
func (c *aggregatorLocalAdminClient) Close() error {
	return nil
}

type bufferPastLimit struct {
	upperBound time.Duration
	bufferPast time.Duration
}

var (
	defaultBufferPastLimits = []bufferPastLimit{
		{upperBound: 0, bufferPast: 15 * time.Second},
		{upperBound: 30 * time.Second, bufferPast: 30 * time.Second},
		{upperBound: time.Minute, bufferPast: time.Minute},
		{upperBound: 2 * time.Minute, bufferPast: 2 * time.Minute},
	}
)

func bufferForPastTimedMetric(
	limits []bufferPastLimit,
	tile time.Duration,
) time.Duration {
	bufferPast := limits[0].bufferPast
	for _, limit := range limits {
		if tile < limit.upperBound {
			return bufferPast
		}
		bufferPast = limit.bufferPast
	}
	return bufferPast
}

func maxAllowedForwardingDelay(
	limits []bufferPastLimit,
	tile time.Duration,
	numForwardedTimes int,
) time.Duration {
	resolutionForwardDelay := tile * time.Duration(numForwardedTimes)
	bufferPast := limits[0].bufferPast
	for _, limit := range limits {
		if tile < limit.upperBound {
			return bufferPast + resolutionForwardDelay
		}
		bufferPast = limit.bufferPast
	}
	return bufferPast + resolutionForwardDelay
}
