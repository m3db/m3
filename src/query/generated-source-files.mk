SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
include $(SELF_DIR)/../../.ci/common.mk

gopath_prefix              := $(GOPATH)/src
query_package              := github.com/m3db/m3/src/query
query_package_path         := $(gopath_prefix)/$(query_package)
storage_package            := $(query_package)/storage
storage_package_path       := $(gopath_prefix)/$(storage_package)
consolidators_package      := $(query_package)/storage/m3/consolidators
consolidators_package_path := $(gopath_prefix)/$(consolidators_package)
m3x_package                := github.com/m3db/m3/src/x
m3x_package_path           := $(gopath_prefix)/$(m3x_package)
m3db_package         := github.com/m3db/m3
m3db_package_path    := $(gopath_prefix)/$(m3db_package)


# Generation rule for all generated types
.PHONY: genny-all
genny-all: genny-map-all

# Map generation rule for all generated maps
.PHONY: genny-map-all
genny-map-all:                    \
	genny-map-multi-fetch-result  \
	genny-map-series-metadata-map \
	genny-map-series-group-map    \

# Map generation rule for query/storage/m3/consolidators/multiFetchResultMap
.PHONY: genny-map-multi-fetch-result
genny-map-multi-fetch-result:
	cd $(m3x_package_path) && make hashmap-gen     \
		pkg=consolidators                            \
		key_type=models.Tags                         \
		value_type=multiResultSeries                 \
		rename_nogen_key=true                        \
		target_package=$(consolidators_package)      \
		rename_type_prefix=fetchResult
	# Rename generated map file
	mv -f $(consolidators_package_path)/map_gen.go $(consolidators_package_path)/fetch_result_map_gen.go

# Map generation rule for query/graphite/storage/seriesMetadataMap
.PHONY: genny-map-series-metadata-map
genny-map-series-metadata-map:
	cd $(m3x_package_path) && make byteshashmap-gen          \
		pkg=storage                                            \
		value_type=seriesMetadata                              \
		target_package=$(query_package)/graphite/storage       \
		rename_nogen_key=true                                  \
		rename_type_prefix=seriesMetadata                      \
		rename_constructor=newSeriesMetadataMap                \
		rename_constructor_options=seriesMetadataMapOptions
	# Rename generated map file
	mv -f $(query_package_path)/graphite/storage/map_gen.go $(query_package_path)/graphite/storage/series_metadata_map_gen.go
	mv -f $(query_package_path)/graphite/storage/new_map_gen.go $(query_package_path)/graphite/storage/series_metadata_map_new.go

# Map generation rule for query/storage/m3/seriesGroupMap
.PHONY: genny-map-series-group-map
genny-map-series-group-map:
	cd $(m3x_package_path) && make hashmap-gen     \
		pkg=m3                                       \
		key_type=models.Tags                         \
		value_type=seriesGroup                       \
		rename_nogen_key=true                        \
		target_package=$(storage_package)            \
		rename_type_prefix=seriesGroup
	# Rename generated map file
	mv -f $(storage_package_path)/map_gen.go $(storage_package_path)/series_group_map_gen.go
