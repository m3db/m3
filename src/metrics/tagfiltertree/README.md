# Tag Filter Tree

## Motivation
There are many instances where we want to match an input metricID against
a set of tag filter patterns. Iterating through each filter individually and matching
them is extremely expensive since it has to be done on each incoming metricID.
One example is you want to drop incoming metricIDs that matches any
of the configured tag filter patterns. Instead of iterating over all configured tag
filter patterns, they could be structured as a tree and then quickly prune non-relevant
tag filter patterns from being matched and thereby also reducing the reducdancy in matching
the same tag filters over and over that are common between the tag filter patterns.

Example:
Consider a set of tag filter patterns:
1. service:foo env:prod* name:num_requests
2. service:foo env:staging* name:*num_errors

When an input metricID comes in:
service:foo env:staging-123 name:login.num_errors
it would be wasteful to match "service:foo" again. Also once we know that "env:staging*"
matched then we only need to look at the "name:*num_errors" to find a match for the
input metricID.

## Usage
First create a trie using New() and then add tagFilters using AddTagFilter().
The tags within a filter can be specified in any order but to condense the compiled
output of the trie, try and specify the most common set of tags in the beginning
and in the same order.
For instance, in case you have a tag "service" which you anticipate to be present
in all filters then make sure that is specified first and then specify the remaining tags
in the filter.
So basically re-order the tags filters in the tag filter pattern based on the popularity
of the tag filter.

There are two ways to use the Match() API.
1. Match All
When calling AddTagFilter() you can attach data to that tag filter pattern. This data
can be retreived during Match(). Consequently, a Match() call with an input metricID
can match multiple tag filter patterns thus multiple data. The 2nd parameter passed to
Match() is a data slice to hold the matched data.

2. Match Any
In case we are simply interested in knowing whether the input metricID matched any of the
tag filter patterns and don't really care about which in particular then we can call Match()
with the 2nd parameter as nil. This results an even faster code path than the one above.

```go
			tree := New[*Rule]()
			for _, rule := range tt.rules {
				for _, tagFilterPattern := range rule.TagFiltersPatterns {
					err := tree.AddTagFilter(tagFilterPattern, &rule)
					require.NoError(t, err)
				}
			}

      ...
      // check match all
			data := make([]*Rule, 0)
			matched, err := tree.Match(inputTags, &data)

      ...
      // check match any
			anyMatched, err := tree.Match(inputTags, nil)
      ...
```

## Caveats
The trie might return duplicates and it is up to the caller to de-dup the results.
