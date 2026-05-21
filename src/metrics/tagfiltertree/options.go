package tagfiltertree

import "github.com/m3db/m3/src/metrics/filters"

// Options is a set of options for the attributor.
type Options interface {
	TagFilterOptions() filters.TagsFilterOptions
	SetTagFilterOptions(tf filters.TagsFilterOptions) Options
}

type options struct {
	tagFilterOptions filters.TagsFilterOptions
}

// NewOptions creates a new set of options.
func NewOptions() Options {
	return &options{}
}

// TagFilterOptions returns the tag filter options.
func (o *options) TagFilterOptions() filters.TagsFilterOptions {
	return o.tagFilterOptions
}

// SetTagFilterOptions sets the tag filter options.
func (o *options) SetTagFilterOptions(tf filters.TagsFilterOptions) Options {
	opts := *o
	opts.tagFilterOptions = tf
	return &opts
}
