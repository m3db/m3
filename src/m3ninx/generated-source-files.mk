SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
include $(SELF_DIR)/../../.ci/common.mk

gopath_prefix        := $(GOPATH)/src
m3ninx_package       := github.com/m3db/m3/src/m3ninx
m3ninx_package_path  := $(gopath_prefix)/$(m3ninx_package)
m3x_package          := github.com/m3db/m3/src/x
m3x_package_path     := $(gopath_prefix)/$(m3x_package)

# Generation rule for all generated types
.PHONY: genny-all
genny-all: genny-map-all genny-arraypool-all

# Map generation rule for all generated maps
.PHONY: genny-map-all
genny-map-all:                          \
	genny-map-segment-builder-postingsmap \
	genny-map-segment-builder-fieldsmap   \
	genny-map-segment-builder-idsmap      \
	genny-map-segment-mem-fieldsmap       \

# NB: We use (genny)[1] to combat the lack of generics in Go. It allows us
# to declare templat-ized versions of code, and specialize using code
# generation.  We have a generic HashMap<K,V> implementation in `m3x`,
# with two template variables (KeyType and ValueType), along with required
# functors (HashFn, EqualsFn, ...). Below, we generate a few specialized
# versions required in m3ninx today.
#
# If we need to generate other specialized map implementations, or other
# specialized datastructures, we should add targets similar to the ones below.
#
# [1]: https://github.com/cheekybits/genny

# Map generation rule for index/segment/builder.PostingsMap
.PHONY: genny-map-segment-builder-postingsmap
genny-map-segment-builder-postingsmap:
	cd $(m3x_package_path) && make byteshashmap-gen          \
		pkg=builder                                            \
		value_type=postings.MutableList                        \
		target_package=$(m3ninx_package)/index/segment/builder \
		rename_nogen_key=true                                  \
		rename_nogen_value=true                                \
		rename_type_prefix=Postings                            \
		rename_constructor=NewPostingsMap                      \
		rename_constructor_options=PostingsMapOptions
	# Rename generated map file
	mv -f $(m3ninx_package_path)/index/segment/builder/map_gen.go $(m3ninx_package_path)/index/segment/builder/postings_map_gen.go
	mv -f $(m3ninx_package_path)/index/segment/builder/new_map_gen.go $(m3ninx_package_path)/index/segment/builder/postings_map_new.go

	# Map generation rule for index/segment/builder.IDsMap
.PHONY: genny-map-segment-builder-idsmap
genny-map-segment-builder-idsmap:
	cd $(m3x_package_path) && make byteshashmap-gen          \
		pkg=builder                                            \
		value_type=struct{}                                    \
		target_package=$(m3ninx_package)/index/segment/builder \
	  rename_nogen_key=true                                  \
	  rename_nogen_value=true                                \
		rename_type_prefix=IDs                                 \
		rename_constructor=NewIDsMap                           \
		rename_constructor_options=IDsMapOptions
	# Rename generated map file
	mv -f $(m3ninx_package_path)/index/segment/builder/map_gen.go $(m3ninx_package_path)/index/segment/builder/ids_map_gen.go
	mv -f $(m3ninx_package_path)/index/segment/builder/new_map_gen.go $(m3ninx_package_path)/index/segment/builder/ids_map_new.go


# Map generation rule for index/segment/builder.fieldsMap
.PHONY: genny-map-segment-builder-fieldsmap
genny-map-segment-builder-fieldsmap:
	cd $(m3x_package_path) && make byteshashmap-gen          \
		pkg=builder                                            \
		value_type=*terms                                      \
		value_type_alias=terms                                 \
		target_package=$(m3ninx_package)/index/segment/builder \
	  rename_nogen_key=true                                  \
		rename_type_prefix=fields                              \
		rename_constructor=newFieldsMap                        \
		rename_constructor_options=fieldsMapOptions
	# Rename generated map file
	mv -f $(m3ninx_package_path)/index/segment/builder/map_gen.go $(m3ninx_package_path)/index/segment/builder/fields_map_gen.go
	mv -f $(m3ninx_package_path)/index/segment/builder/new_map_gen.go $(m3ninx_package_path)/index/segment/builder/fields_map_new.go

# Map generation rule for index/segment/mem.fieldsMap
.PHONY: genny-map-segment-mem-fieldsmap
genny-map-segment-mem-fieldsmap:
	cd $(m3x_package_path) && make byteshashmap-gen      \
		pkg=mem                                            \
		value_type=*concurrentPostingsMap                  \
		value_type_alias=concurrentPostingsMap             \
		target_package=$(m3ninx_package)/index/segment/mem \
	  rename_nogen_key=true                              \
		rename_type_prefix=fields                          \
		rename_constructor=newFieldsMap                    \
		rename_constructor_options=fieldsMapOptions
	# Rename generated map file
	mv -f $(m3ninx_package_path)/index/segment/mem/map_gen.go $(m3ninx_package_path)/index/segment/mem/fields_map_gen.go
	mv -f $(m3ninx_package_path)/index/segment/mem/new_map_gen.go $(m3ninx_package_path)/index/segment/mem/fields_map_new.go

# generation rule for all generated arraypools
.PHONY: genny-arraypool-all
genny-arraypool-all:                                  \
	genny-arraypool-bytes-slice-array-pool            \
	genny-arraypool-document-array-pool               \
	genny-arraypool-metadata-array-pool               \

# arraypool generation rule for ./x/bytes.SliceArrayPool
.PHONY: genny-arraypool-bytes-slice-array-pool
genny-arraypool-bytes-slice-array-pool:
	cd $(m3x_package_path) && make genny-arraypool \
	pkg=bytes                                      \
	elem_type=[]byte                               \
	target_package=$(m3ninx_package)/x/bytes       \
	out_file=slice_arraypool_gen.go                \
	rename_type_prefix=Slice                       \
	rename_type_middle=Slice                       \
	rename_constructor=NewSliceArrayPool           \

# arraypool generation rule for ./doc.DocumentArrayPool
.PHONY: genny-arraypool-document-array-pool
genny-arraypool-document-array-pool:
	cd $(m3x_package_path) && make genny-arraypool        \
	pkg=doc                                               \
	elem_type=Document                                    \
	target_package=$(m3ninx_package)/doc                  \
	out_file=doc_arraypool_gen.go                         \
	rename_type_prefix=Document                           \
	rename_type_middle=Document                           \
	rename_constructor=NewDocumentArrayPool               \
	rename_gen_types=true                                 \

# arraypool generation rule for ./doc.MetadataArrayPool
.PHONY: genny-arraypool-metadata-array-pool
genny-arraypool-metadata-array-pool:
	cd $(m3x_package_path) && make genny-arraypool \
	pkg=doc                                        \
	elem_type=Metadata                             \
	target_package=$(m3ninx_package)/doc           \
	out_file=metadata_arraypool_gen.go             \
	rename_type_prefix=Metadata                    \
	rename_type_middle=Metadata                    \
	rename_constructor=NewMetadataArrayPool        \
	rename_gen_types=true                          \
