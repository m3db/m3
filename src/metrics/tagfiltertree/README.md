# Tag Filter Tree

## Motivation
There are many instances where we want to match an input metricID against
a set of tag filters. One such use-case is metric attribution to namespaces.
Iterating through each filter individually and matching them is extremely expensive
since it has to be done on each incoming metricID. Therefore, this data structure
pre-compiles a set of tag filters in order to optimize matches against an input metricID.

## Usage
First create a trie using New() and then add tagFilters using AddTagFilter().
The tags within a filter can be specified in any order but to condense the compiled
output of the trie, try and specify the most common set of tags in the beginning
and in the same order.
For instance, in case you have a tag "service" which you anticipate to be present
in all filters then make sure that is specified first and then specify the remaining tags
in the filter.
The trie also supports "*" for a tag value which can be used to ensure the existance of a tag
in the input metricID.

## Caveats
The trie might return duplicates and it is up to the caller to de-dup the results.
