package storage

import (
	"github.com/m3db/m3coordinator/models"

	"github.com/m3db/m3db/storage/index"
	"github.com/m3db/m3ninx/index/segment"
	"github.com/m3db/m3x/ident"
)

// FromM3IdentToMetric converts an M3 ident metric to a coordinator metric
func FromM3IdentToMetric(identNamespace, identID ident.ID, iterTags ident.TagIterator) *models.Metric {
	namespace := identNamespace.String()
	id := identID.String()
	tags := FromIdentTagsToTags(iterTags)

	return &models.Metric{
		ID:        id,
		Namespace: namespace,
		Tags:      tags,
	}
}

// FromIdentTagsToTags converts ident tags to coordinator tags
func FromIdentTagsToTags(iterTags ident.TagIterator) models.Tags {
	defer iterTags.Close()

	tags := make(models.Tags, iterTags.Remaining())
	for iterTags.Next() {
		identTag := iterTags.Current()
		tags[identTag.Name.String()] = identTag.Value.String()
	}
	return tags
}

// FetchOptionsToM3Options converts a set of coordinator options to M3 options
func FetchOptionsToM3Options(fetchOptions *FetchOptions, fetchQuery *FetchQuery) index.QueryOptions {
	return index.QueryOptions{
		Limit:          fetchOptions.Limit,
		StartInclusive: fetchQuery.Start,
		EndExclusive:   fetchQuery.End,
	}
}

// FetchQueryToM3Query converts an m3coordinator fetch query to an M3 query
func FetchQueryToM3Query(fetchQuery *FetchQuery) index.Query {
	var indexQuery index.Query
	segQuery := segment.Query{
		Filters:     MatchersToFilters(fetchQuery.TagMatchers),
		Conjunction: segment.AndConjunction, // NB (braskin): & is the only conjunction supported currently
	}
	indexQuery.Query = segQuery

	return indexQuery
}

// MatchersToFilters converts matchers to M3 filters
func MatchersToFilters(matchers models.Matchers) []segment.Filter {
	var (
		filters []segment.Filter
		negate  bool
		regexp  bool
	)

	for _, matcher := range matchers {
		if matcher.Type == models.MatchNotEqual || matcher.Type == models.MatchNotRegexp {
			negate = true
		}
		if matcher.Type == models.MatchNotRegexp || matcher.Type == models.MatchRegexp {
			regexp = true
		}

		filters = append(filters, segment.Filter{
			FieldName:        []byte(matcher.Name),
			FieldValueFilter: []byte(matcher.Value),
			Negate:           negate,
			Regexp:           regexp,
		})
	}
	return filters
}
