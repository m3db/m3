SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
include $(SELF_DIR)/../../.ci/common.mk

gopath_prefix            := $(GOPATH)/src
query_package            := github.com/m3db/m3/src/query
query_package_path       := $(gopath_prefix)/$(query_package)
multiresult_package      := $(query_package)/storage/m3/multiresults
multiresult_package_path := $(gopath_prefix)/$(multiresult_package)
m3x_package              := github.com/m3db/m3x
m3x_package_path         := $(gopath_prefix)/$(m3x_package)
m3x_package_min_ver      := 76a586220279667a81eaaec4150de182f4d5077c

.PHONY: install-m3x-repo
install-m3x-repo: install-glide install-generics-bin
	# Check if repository exists, if not get it
	test -d $(m3x_package_path) || go get -u $(m3x_package)
	test -d $(m3x_package_path)/vendor || (cd $(m3x_package_path) && glide install)
	test "$(shell cd $(m3x_package_path) && git diff --shortstat 2>/dev/null)" = "" || ( \
		echo "WARNING: m3x repository is dirty, generated files might not be as expected" \
	)
	# If does exist but not at min version then update it
	(cd $(m3x_package_path) && git cat-file -t $(m3x_package_min_ver) > /dev/null) || ( \
		echo "WARNING: m3x repository is below commit $(m3x_package_min_ver), generated files might not be as expected" \
	)

# Generation rule for all generated types
.PHONY: genny-all
genny-all: genny-map-all

# Map generation rule for all generated maps
.PHONY: genny-map-all
genny-map-all: \
	genny-map-multi-complete-tags \
	genny-map-multi-complete-tag-values \
	genny-map-multi-result-series \
	genny-map-multi-search-result

# Map generation rule for query/multiCompleteTags
.PHONY: genny-map-multi-complete-tags
genny-map-multi-complete-tags: install-m3x-repo
	cd $(m3x_package_path) && make byteshashmap-gen \
		pkg=multiresults \
		value_type=completedTagBuilder \
		target_package=$(multiresult_package) \
		rename_type_prefix=multiCompleteTags \
		rename_constructor=newMultiCompleteTagsMap \
		rename_constructor_options=multiCompleteTagsMapOptions
	# Rename both generated map and constructor files
	mv -f $(multiresult_package_path)/map_gen.go \
		$(multiresult_package_path)/multi_complete_tags_map_gen.go
	mv -f $(multiresult_package_path)/new_map_gen.go \
		$(multiresult_package_path)/multi_complete_tags_new_map_gen.go

# Map generation rule for query/multiCompleteTagValues
.PHONY: genny-map-multi-complete-tag-values
genny-map-multi-complete-tag-values: install-m3x-repo
	cd $(m3x_package_path) && make byteshashmap-gen \
		pkg=multiresults \
		value_type=seen \
		target_package=$(multiresult_package) \
		rename_type_prefix=multiCompleteTagValues \
		rename_constructor=newMultiCompleteTagValuesMap \
		rename_constructor_options=multiCompleteTagValuesMapOptions
	# Rename both generated map and constructor files
	mv -f $(multiresult_package_path)/map_gen.go \
		$(multiresult_package_path)/multi_complete_tag_values_map_gen.go
	mv -f $(multiresult_package_path)/new_map_gen.go \
		$(multiresult_package_path)/multi_complete_tag_values_new_map_gen.go

# Map generation rule for query/multiResultSeries
.PHONY: genny-map-multi-result-series
genny-map-multi-result-series: install-m3x-repo
	cd $(m3x_package_path) && make byteshashmap-gen \
		pkg=multiresults \
		value_type=multiResultSeries \
		target_package=$(multiresult_package) \
		rename_type_prefix=multiResultSeries \
		rename_constructor=newMultiResultSeriesMap \
		rename_constructor_options=multiResultSeriesMapOptions
	# Rename both generated map and constructor files
	mv -f $(multiresult_package_path)/map_gen.go \
		$(multiresult_package_path)/multi_result_series_map_gen.go
	mv -f $(multiresult_package_path)/new_map_gen.go \
		$(multiresult_package_path)/multi_result_series_new_map_gen.go


# Map generation rule for query/multiSearchResult
.PHONY: genny-map-multi-search-result
genny-map-multi-search-result: install-m3x-repo
	cd $(m3x_package_path) && make byteshashmap-gen \
		pkg=multiresults \
		value_type=MultiTagResult \
		target_package=$(multiresult_package) \
		rename_type_prefix=multiSearchResult \
		rename_constructor=newMultiSearchResultMap \
		rename_constructor_options=multiSearchResultMapOptions
	# Rename both generated map and constructor files
	mv -f $(multiresult_package_path)/map_gen.go \
		$(multiresult_package_path)/multi_search_result_map_gen.go
	mv -f $(multiresult_package_path)/new_map_gen.go \
		$(multiresult_package_path)/multi_search_result_new_map_gen.go

