package rules

import (
	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/policy"
)

// ValidationConfiguration is the configuration for rules validation.
type ValidationConfiguration struct {
	RequiredRollupTags     []string                           `yaml:"requiredRollupTags"`
	MetricTypes            metricTypesValidationConfiguration `yaml:"metricTypes"`
	Policies               policiesValidationConfiguration    `yaml:"policies"`
	TagNameInvalidChars    string                             `yaml:"tagNameInvalidChars"`
	MetricNameInvalidChars string                             `yaml:"metricNameInvalidChars"`
}

// NewValidator creates a new rules validator based on the given configuration.
func (c ValidationConfiguration) NewValidator() Validator {
	opts := NewValidatorOptions().
		SetRequiredRollupTags(c.RequiredRollupTags).
		SetMetricTypesFn(c.MetricTypes.NewMetricTypesFn()).
		SetDefaultAllowedStoragePolicies(c.Policies.DefaultAllowed.StoragePolicies).
		SetDefaultAllowedCustomAggregationTypes(c.Policies.DefaultAllowed.AggregationTypes).
		SetTagNameInvalidChars(toRunes(c.TagNameInvalidChars)).
		SetMetricNameInvalidChars(toRunes(c.MetricNameInvalidChars))
	for _, override := range c.Policies.Overrides {
		opts = opts.
			SetAllowedStoragePoliciesFor(override.Type, override.Allowed.StoragePolicies).
			SetAllowedCustomAggregationTypesFor(override.Type, override.Allowed.AggregationTypes)
	}
	return NewValidator(opts)
}

// metricTypesValidationConfiguration is th configuration for metric types validation.
type metricTypesValidationConfiguration struct {
	// Metric type tag.
	TypeTag string `yaml:"typeTag"`

	// Allowed metric types.
	Allowed []metric.Type `yaml:"allowed"`
}

// NewMetricTypesFn creates a new metric types fn from the given configuration.
func (c metricTypesValidationConfiguration) NewMetricTypesFn() MetricTypesFn {
	return func(tagFilters filters.TagFilterValueMap) ([]metric.Type, error) {
		allowed := make([]metric.Type, 0, len(c.Allowed))
		filterValue, exists := tagFilters[c.TypeTag]
		if !exists {
			// If there is not type filter provided, the filter may match any allowed type.
			allowed = append(allowed, c.Allowed...)
			return allowed, nil
		}
		f, err := filters.NewFilterFromFilterValue(filterValue)
		if err != nil {
			return nil, err
		}
		for _, t := range c.Allowed {
			if f.Matches([]byte(t.String())) {
				allowed = append(allowed, t)
			}
		}
		return allowed, nil
	}
}

// policiesValidationConfiguration is the configuration for policies validation.
type policiesValidationConfiguration struct {
	// DefaultAllowed defines the policies allowed by default.
	DefaultAllowed policiesConfiguration `yaml:"defaultAllowed"`

	// Overrides define the metric type specific policy overrides.
	Overrides []policiesOverrideConfiguration `yaml:"overrides"`
}

// policiesOverrideConfiguration is the configuration for metric type specific policy overrides.
type policiesOverrideConfiguration struct {
	Type    metric.Type           `yaml:"type"`
	Allowed policiesConfiguration `yaml:"allowed"`
}

// policiesConfiguration is the configuration for storage policies and aggregation types.
type policiesConfiguration struct {
	StoragePolicies  []policy.StoragePolicy   `yaml:"storagePolicies"`
	AggregationTypes []policy.AggregationType `yaml:"aggregationTypes"`
}

func toRunes(s string) []rune {
	r := make([]rune, 0, len(s))
	for _, c := range s {
		r = append(r, c)
	}
	return r
}
