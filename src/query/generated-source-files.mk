SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
include $(SELF_DIR)/../../.ci/common.mk

gopath_prefix              := $(GOPATH)/src
query_package              := github.com/m3db/m3/src/query
query_package_path         := $(gopath_prefix)/$(query_package)
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
genny-map-all:                 \
	genny-map-multi-fetch-result

# Map generation rule for query/storage/m3/consolidators/mutliFetchResultMap
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
